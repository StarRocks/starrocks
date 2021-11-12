// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/schema_change.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/vectorized/schema_change.h"

#include <signal.h>
#include <util/defer_op.h>

#include <algorithm>
#include <memory>
#include <vector>

#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/mem_pool.h"
#include "storage/merger.h"
#include "storage/row.h"
#include "storage/row_block.h"
#include "storage/row_cursor.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_updates.h"
#include "storage/vectorized/chunk_aggregator.h"
#include "storage/vectorized/convert_helper.h"
#include "storage/wrapper_field.h"
#include "util/unaligned_access.h"

using std::deque;
using std::list;
using std::nothrow;
using std::pair;
using std::string;
using std::stringstream;
using std::vector;

namespace starrocks {
namespace vectorized {

using ChunkRow = std::pair<size_t, Chunk*>;

int compare_chunk_row(const ChunkRow& lhs, const ChunkRow& rhs) {
    for (uint16_t i = 0; i < lhs.second->schema()->num_key_fields(); ++i) {
        int res = lhs.second->get_column_by_index(i)->compare_at(lhs.first, rhs.first,
                                                                 *(rhs.second->get_column_by_index(i)), -1);
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

class ChunkSorter {
public:
    explicit ChunkSorter(ChunkAllocator* allocator);
    virtual ~ChunkSorter();

    bool sort(ChunkPtr& chunk, TabletSharedPtr new_tablet);
    size_t allocated_rows() { return _max_allocated_rows; }

    static bool _chunk_comparator(const ChunkRow& lhs, const ChunkRow& rhs) { return compare_chunk_row(lhs, rhs) < 0; }

private:
    ChunkAllocator* _chunk_allocator;
    ChunkPtr _swap_chunk;
    size_t _max_allocated_rows;
};

class ChunkMerger {
public:
    explicit ChunkMerger(TabletSharedPtr tablet);
    virtual ~ChunkMerger();

    bool merge(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* rowset_writer);
    void aggregate_chunk(ChunkAggregator& aggregator, ChunkPtr& chunk, RowsetWriter* rowset_writer);

private:
    struct MergeElement {
        bool operator<(const MergeElement& other) const {
            return compare_chunk_row(std::make_pair(row_index, chunk), std::make_pair(other.row_index, other.chunk)) >
                   0;
        }

        Chunk* chunk;
        size_t row_index;
    };

    bool _make_heap(std::vector<ChunkPtr>& chunk_arr);
    bool _pop_heap();

    TabletSharedPtr _tablet;
    std::priority_queue<MergeElement> _heap;
    std::unique_ptr<ChunkAggregator> _aggregator;
};

ChunkChanger::ChunkChanger(const TabletSchema& tablet_schema) {
    _schema_mapping.resize(tablet_schema.num_columns());
}

ChunkChanger::~ChunkChanger() {
    SchemaMapping::iterator it = _schema_mapping.begin();
    for (; it != _schema_mapping.end(); ++it) {
        SAFE_DELETE(it->default_value);
    }
    _schema_mapping.clear();
}

ColumnMapping* ChunkChanger::get_mutable_column_mapping(size_t column_index) {
    if (column_index >= _schema_mapping.size()) {
        return nullptr;
    }

    return &(_schema_mapping[column_index]);
}

#define TYPE_REINTERPRET_CAST(FromType, ToType)      \
    {                                                \
        size_t row_num = base_chunk->num_rows();     \
        for (size_t row = 0; row < row_num; ++row) { \
            Datum base_datum = base_col->get(row);   \
            Datum new_datum;                         \
            if (base_datum.is_null()) {              \
                new_datum.set_null();                \
                new_col->append_datum(new_datum);    \
                continue;                            \
            }                                        \
            FromType src;                            \
            src = base_datum.get<FromType>();        \
            ToType dst = static_cast<ToType>(src);   \
            new_datum.set(dst);                      \
            new_col->append_datum(new_datum);        \
        }                                            \
        break;                                       \
    }

#define CONVERT_FROM_TYPE(from_type)                                                \
    {                                                                               \
        switch (new_type) {                                                         \
        case OLAP_FIELD_TYPE_TINYINT:                                               \
            TYPE_REINTERPRET_CAST(from_type, int8_t);                               \
        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:                                      \
            TYPE_REINTERPRET_CAST(from_type, uint8_t);                              \
        case OLAP_FIELD_TYPE_SMALLINT:                                              \
            TYPE_REINTERPRET_CAST(from_type, int16_t);                              \
        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:                                     \
            TYPE_REINTERPRET_CAST(from_type, uint16_t);                             \
        case OLAP_FIELD_TYPE_INT:                                                   \
            TYPE_REINTERPRET_CAST(from_type, int32_t);                              \
        case OLAP_FIELD_TYPE_UNSIGNED_INT:                                          \
            TYPE_REINTERPRET_CAST(from_type, uint32_t);                             \
        case OLAP_FIELD_TYPE_BIGINT:                                                \
            TYPE_REINTERPRET_CAST(from_type, int64_t);                              \
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:                                       \
            TYPE_REINTERPRET_CAST(from_type, uint64_t);                             \
        case OLAP_FIELD_TYPE_LARGEINT:                                              \
            TYPE_REINTERPRET_CAST(from_type, int128_t);                             \
        case OLAP_FIELD_TYPE_DOUBLE:                                                \
            TYPE_REINTERPRET_CAST(from_type, double);                               \
        default:                                                                    \
            LOG(WARNING) << "the column type which was altered to was unsupported." \
                         << " origin_type=" << field_type_to_string(ref_type)       \
                         << ", alter_type=" << field_type_to_string(new_type);      \
            return false;                                                           \
        }                                                                           \
        break;                                                                      \
    }

struct ConvertTypeMapHash {
    size_t operator()(const std::pair<FieldType, FieldType>& pair) const { return (pair.first + 31) ^ pair.second; }
};

class ConvertTypeResolver {
    DECLARE_SINGLETON(ConvertTypeResolver);

public:
    bool get_convert_type_info(const FieldType from_type, const FieldType to_type) const {
        return _convert_type_set.find(std::make_pair(from_type, to_type)) != _convert_type_set.end();
    }

    template <FieldType from_type, FieldType to_type>
    void add_convert_type_mapping() {
        _convert_type_set.emplace(std::make_pair(from_type, to_type));
    }

private:
    typedef std::pair<FieldType, FieldType> convert_type_pair;
    std::unordered_set<convert_type_pair, ConvertTypeMapHash> _convert_type_set;

    DISALLOW_COPY_AND_ASSIGN(ConvertTypeResolver);
};

ConvertTypeResolver::ConvertTypeResolver() {
    // from varchar type
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_TINYINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_SMALLINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_INT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_BIGINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_LARGEINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_FLOAT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_DOUBLE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_DATE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_DATE_V2>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_DECIMAL32>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_DECIMAL64>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_DECIMAL128>();

    // to varchar type
    add_convert_type_mapping<OLAP_FIELD_TYPE_TINYINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_SMALLINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_BIGINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_LARGEINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_FLOAT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DOUBLE, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL_V2, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL32, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL64, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL128, OLAP_FIELD_TYPE_VARCHAR>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_DATE, OLAP_FIELD_TYPE_DATETIME>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DATE, OLAP_FIELD_TYPE_TIMESTAMP>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DATE_V2, OLAP_FIELD_TYPE_DATETIME>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DATE_V2, OLAP_FIELD_TYPE_TIMESTAMP>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_DATETIME, OLAP_FIELD_TYPE_DATE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DATETIME, OLAP_FIELD_TYPE_DATE_V2>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_TIMESTAMP, OLAP_FIELD_TYPE_DATE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_TIMESTAMP, OLAP_FIELD_TYPE_DATE_V2>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_FLOAT, OLAP_FIELD_TYPE_DOUBLE>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_DATE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_DATE_V2>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_DATE, OLAP_FIELD_TYPE_DATE_V2>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DATE_V2, OLAP_FIELD_TYPE_DATE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DATETIME, OLAP_FIELD_TYPE_TIMESTAMP>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_TIMESTAMP, OLAP_FIELD_TYPE_DATETIME>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL, OLAP_FIELD_TYPE_DECIMAL_V2>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL_V2, OLAP_FIELD_TYPE_DECIMAL>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL, OLAP_FIELD_TYPE_DECIMAL128>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL_V2, OLAP_FIELD_TYPE_DECIMAL128>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL32, OLAP_FIELD_TYPE_DECIMAL32>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL32, OLAP_FIELD_TYPE_DECIMAL64>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL32, OLAP_FIELD_TYPE_DECIMAL128>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL64, OLAP_FIELD_TYPE_DECIMAL32>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL64, OLAP_FIELD_TYPE_DECIMAL64>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL64, OLAP_FIELD_TYPE_DECIMAL128>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL128, OLAP_FIELD_TYPE_DECIMAL32>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL128, OLAP_FIELD_TYPE_DECIMAL64>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL128, OLAP_FIELD_TYPE_DECIMAL128>();
}

ConvertTypeResolver::~ConvertTypeResolver() {}

bool ChunkChanger::change_chunk(ChunkPtr& base_chunk, ChunkPtr& new_chunk, TabletSharedPtr& base_tablet,
                                TabletSharedPtr& new_tablet, MemPool* mem_pool) {
    if (new_chunk->num_columns() != _schema_mapping.size()) {
        LOG(WARNING) << "new chunk does not match with schema mapping rules. "
                     << "chunk_schema_size=" << new_chunk->num_columns()
                     << ", mapping_schema_size=" << _schema_mapping.size();
        return false;
    }

    for (size_t i = 0; i < new_chunk->num_columns(); ++i) {
        int ref_column = _schema_mapping[i].ref_column;

        if (_schema_mapping[i].ref_column >= 0) {
            FieldType ref_type = base_tablet->tablet_meta()->tablet_schema().column(ref_column).type();
            FieldType new_type = new_tablet->tablet_meta()->tablet_schema().column(i).type();
            if (!_schema_mapping[i].materialized_function.empty()) {
                const MaterializeTypeConverter* converter = nullptr;
                if (_schema_mapping[i].materialized_function == "to_bitmap") {
                    converter = vectorized::get_materialized_converter(ref_type, OLAP_MATERIALIZE_TYPE_BITMAP);
                } else if (_schema_mapping[i].materialized_function == "hll_hash") {
                    converter = vectorized::get_materialized_converter(ref_type, OLAP_MATERIALIZE_TYPE_HLL);
                } else if (_schema_mapping[i].materialized_function == "count_field") {
                    converter = vectorized::get_materialized_converter(ref_type, OLAP_MATERIALIZE_TYPE_COUNT);
                } else if (_schema_mapping[i].materialized_function == "percentile_hash") {
                    converter = vectorized::get_materialized_converter(ref_type, OLAP_MATERIALIZE_TYPE_PERCENTILE);
                } else {
                    LOG(WARNING) << "error materialized view function : " << _schema_mapping[i].materialized_function;
                    return false;
                }
                VLOG(3) << "_schema_mapping[" << i
                        << "].materialized_function : " << _schema_mapping[i].materialized_function;

                if (converter == nullptr) {
                    LOG(WARNING) << "error materialized view function : " << _schema_mapping[i].materialized_function;
                    return false;
                }
                ColumnPtr& base_col = base_chunk->get_column_by_index(ref_column);
                ColumnPtr& new_col = new_chunk->get_column_by_index(i);
                Field f = ChunkHelper::convert_field_to_format_v2(
                        ref_column, base_tablet->tablet_meta()->tablet_schema().column(ref_column));
                Status st =
                        converter->convert_materialized(base_col, new_col, f.type().get(),
                                                        base_tablet->tablet_meta()->tablet_schema().column(ref_column));
                if (!st.ok()) {
                    return false;
                }
                continue;
            }

            int reftype_precision = base_tablet->tablet_meta()->tablet_schema().column(ref_column).precision();
            int reftype_scale = base_tablet->tablet_meta()->tablet_schema().column(ref_column).scale();
            int newtype_precision = new_tablet->tablet_meta()->tablet_schema().column(i).precision();
            int newtype_scale = new_tablet->tablet_meta()->tablet_schema().column(i).scale();

            ColumnPtr& base_col = base_chunk->get_column_by_index(ref_column);
            ColumnPtr& new_col = new_chunk->get_column_by_index(i);

            if (new_type == ref_type && (!is_decimalv3_field_type(new_type) ||
                                         (reftype_precision == newtype_precision && reftype_scale == newtype_scale))) {
                if (new_type == OLAP_FIELD_TYPE_CHAR) {
                    for (size_t row_index = 0; row_index < base_chunk->num_rows(); ++row_index) {
                        Datum base_datum = base_col->get(row_index);
                        Datum new_datum;
                        if (base_datum.is_null()) {
                            new_datum.set_null();
                            new_col->append_datum(new_datum);
                            continue;
                        }

                        Slice base_slice = base_datum.get_slice();
                        Slice slice;
                        slice.size = new_tablet->tablet_meta()->tablet_schema().column(i).length();
                        slice.data = reinterpret_cast<char*>(mem_pool->allocate(slice.size));
                        memset(slice.data, 0, slice.size);
                        size_t copy_size = slice.size < base_slice.size ? slice.size : base_slice.size;
                        memcpy(slice.data, base_slice.data, copy_size);
                        new_datum.set(slice);
                        new_col->append_datum(new_datum);
                    }
                } else {
                    new_col = base_col;
                }
            } else if (ConvertTypeResolver::instance()->get_convert_type_info(ref_type, new_type)) {
                auto converter = vectorized::get_type_converter(ref_type, new_type);
                if (converter == nullptr) {
                    LOG(WARNING) << "failed to get type converter, from_type=" << ref_type << ", to_type" << new_type;
                    return false;
                }
                for (size_t row_index = 0; row_index < base_chunk->num_rows(); ++row_index) {
                    Datum base_datum = base_col->get(row_index);
                    Field f = ChunkHelper::convert_field_to_format_v2(
                            ref_column, base_tablet->tablet_meta()->tablet_schema().column(ref_column));
                    Field f2 = ChunkHelper::convert_field_to_format_v2(
                            i, new_tablet->tablet_meta()->tablet_schema().column(i));
                    Datum new_datum;
                    Status st =
                            converter->convert_datum(f.type().get(), base_datum, f2.type().get(), new_datum, mem_pool);
                    if (!st.ok()) {
                        LOG(WARNING) << "failed to convert " << field_type_to_string(ref_type) << " to "
                                     << field_type_to_string(new_type);
                        return false;
                    }
                    new_col->append_datum(new_datum);
                }
            } else {
                // copy and alter the field
                switch (ref_type) {
                case OLAP_FIELD_TYPE_TINYINT:
                    CONVERT_FROM_TYPE(int8_t);
                case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
                    CONVERT_FROM_TYPE(uint8_t);
                case OLAP_FIELD_TYPE_SMALLINT:
                    CONVERT_FROM_TYPE(int16_t);
                case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
                    CONVERT_FROM_TYPE(uint16_t);
                case OLAP_FIELD_TYPE_INT:
                    CONVERT_FROM_TYPE(int32_t);
                case OLAP_FIELD_TYPE_UNSIGNED_INT:
                    CONVERT_FROM_TYPE(uint32_t);
                case OLAP_FIELD_TYPE_BIGINT:
                    CONVERT_FROM_TYPE(int64_t);
                case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
                    CONVERT_FROM_TYPE(uint64_t);
                default:
                    LOG(WARNING) << "the column type which was altered from was unsupported."
                                 << " from_type=" << ref_type << ", to_type=" << new_type;
                    return false;
                }

                //new_col->append(*base_col);
                if (new_type < ref_type) {
                    LOG(INFO) << "type degraded while altering column. "
                              << "column=" << new_tablet->tablet_meta()->tablet_schema().column(i).name()
                              << ", origin_type=" << field_type_to_string(ref_type)
                              << ", alter_type=" << field_type_to_string(new_type);
                }
            }
        } else {
            ColumnPtr& new_col = new_chunk->get_column_by_index(i);
            for (size_t row_index = 0; row_index < base_chunk->num_rows(); ++row_index) {
                Datum dst_datum;
                if (_schema_mapping[i].default_value->is_null()) {
                    dst_datum.set_null();
                } else {
                    std::string tmp = _schema_mapping[i].default_value->to_string();
                    Field f = ChunkHelper::convert_field_to_format_v2(
                            i, new_tablet->tablet_meta()->tablet_schema().column(i));
                    Status st = datum_from_string(f.type().get(), &dst_datum, tmp, nullptr);
                    if (!st.ok()) {
                        LOG(WARNING) << "create datum from string failed";
                        return false;
                    }
                }
                new_col->append_datum(dst_datum);
            }
        }
    }
    return true;
}

#undef CONVERT_FROM_TYPE
#undef TYPE_REINTERPRET_CAST
#undef ASSIGN_DEFAULT_VALUE

ChunkSorter::ChunkSorter(ChunkAllocator* chunk_allocator)
        : _chunk_allocator(chunk_allocator), _swap_chunk(nullptr), _max_allocated_rows(0) {}

ChunkSorter::~ChunkSorter() {
    if (_swap_chunk != nullptr) {
        _chunk_allocator->release(_swap_chunk, _max_allocated_rows);
        _swap_chunk = nullptr;
    }
}

bool ChunkSorter::sort(ChunkPtr& chunk, TabletSharedPtr new_tablet) {
    vectorized::Schema new_schema = ChunkHelper::convert_schema(new_tablet->tablet_schema());
    if (_swap_chunk == nullptr || _max_allocated_rows < chunk->num_rows()) {
        _chunk_allocator->release(_swap_chunk, _max_allocated_rows);
        Status st = _chunk_allocator->allocate(_swap_chunk, chunk->num_rows(), new_schema);
        if (_swap_chunk == nullptr || !st.ok()) {
            LOG(WARNING) << "allocate swap chunk for sort failed";
            return false;
        }
        _max_allocated_rows = chunk->num_rows();
    }

    _swap_chunk->reset();
    std::vector<ChunkRow> chunk_rows;
    for (size_t i = 0; i < chunk->num_rows(); ++i) {
        chunk_rows.emplace_back(i, chunk.get());
    }

    std::stable_sort(chunk_rows.begin(), chunk_rows.end(), _chunk_comparator);
    // TODO
    // check chunk is need to be sort or not
    for (size_t i = 0; i < chunk->num_columns(); ++i) {
        ColumnPtr& base_col = chunk->get_column_by_index(i);
        ColumnPtr& new_col = _swap_chunk->get_column_by_index(i);
        for (auto row : chunk_rows) {
            size_t row_index = row.first;
            new_col->append_datum(base_col->get(row_index));
        }
    }

    chunk->swap_chunk(*_swap_chunk);
    return true;
}

ChunkAllocator::ChunkAllocator(const TabletSchema& tablet_schema, size_t memory_limitation)
        : _tablet_schema(tablet_schema), _memory_allocated(0), _memory_limitation(memory_limitation) {
    _row_len = 0;
    _row_len = tablet_schema.row_size();
}

ChunkAllocator::~ChunkAllocator() {
    if (_memory_allocated != 0) {
        LOG(WARNING) << "memory leak in ChunkAllocator. memory_size=" << _memory_allocated;
    }
}

bool ChunkAllocator::is_memory_enough_to_sort(size_t num_rows, size_t allocated_rows) {
    if (num_rows <= allocated_rows) {
        return true;
    }
    size_t chunk_size = _row_len * (num_rows - allocated_rows);
    return _memory_allocated + chunk_size < _memory_limitation;
}

Status ChunkAllocator::allocate(ChunkPtr& chunk, size_t num_rows, Schema& schema) {
    size_t mem_size = _row_len * num_rows;

    if (_memory_limitation > 0 && _memory_allocated + mem_size > _memory_limitation) {
        LOG(INFO) << "ChunkAllocator::allocate() memory exceed. "
                  << "m_memory_allocated=" << _memory_allocated;
        return Status::OK();
    }

    chunk = ChunkHelper::new_chunk(schema, num_rows);
    if (chunk == nullptr) {
        LOG(WARNING) << "ChunkAllocator allocate chunk failed.";
        return Status::InternalError("allocate chunk failed");
    }

    _memory_allocated += mem_size;
    return Status::OK();
}

void ChunkAllocator::release(ChunkPtr& chunk, size_t num_rows) {
    if (chunk == nullptr) {
        LOG(INFO) << "null chunk released.";
        return;
    }

    _memory_allocated -= std::max(chunk->num_rows(), num_rows) * _row_len;
    return;
}

ChunkMerger::ChunkMerger(TabletSharedPtr tablet) : _tablet(tablet), _aggregator(nullptr) {}

ChunkMerger::~ChunkMerger() {
    if (_aggregator != nullptr) {
        _aggregator->close();
    }
}

void ChunkMerger::aggregate_chunk(ChunkAggregator& aggregator, ChunkPtr& chunk, RowsetWriter* rowset_writer) {
    aggregator.aggregate();
    while (aggregator.is_finish()) {
        rowset_writer->add_chunk(*aggregator.aggregate_result());
        aggregator.aggregate_reset();
        aggregator.aggregate();
    }

    DCHECK(aggregator.source_exhausted());
    aggregator.update_source(chunk);
    aggregator.aggregate();

    while (aggregator.is_finish()) {
        rowset_writer->add_chunk(*aggregator.aggregate_result());
        aggregator.aggregate_reset();
        aggregator.aggregate();
    }

    return;
}

bool ChunkMerger::merge(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* rowset_writer) {
    auto process_err = [this] {
        LOG(WARNING) << "merge chunk failed";
        while (this->_heap.size() > 0) {
            this->_heap.pop();
        }
    };

    _make_heap(chunk_arr);
    size_t nread = 0;
    vectorized::Schema new_schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    ChunkPtr tmp_chunk = ChunkHelper::new_chunk(new_schema, config::vector_chunk_size);
    if (_tablet->keys_type() == KeysType::AGG_KEYS) {
        _aggregator = std::make_unique<ChunkAggregator>(&new_schema, config::vector_chunk_size, 0);
    }

    while (!_heap.empty()) {
        if (tmp_chunk->reach_capacity_limit() || nread >= config::vector_chunk_size) {
            if (_tablet->keys_type() == KeysType::AGG_KEYS) {
                aggregate_chunk(*_aggregator, tmp_chunk, rowset_writer);
            } else {
                rowset_writer->add_chunk(*tmp_chunk);
            }
            tmp_chunk->reset();
            nread = 0;
        }

        tmp_chunk->append(*(_heap.top().chunk), _heap.top().row_index, 1);
        nread += 1;
        if (!_pop_heap()) {
            process_err();
            return false;
        }
    }

    if (_tablet->keys_type() == KeysType::AGG_KEYS) {
        aggregate_chunk(*_aggregator, tmp_chunk, rowset_writer);
        if (_aggregator->has_aggregate_data()) {
            _aggregator->aggregate();
            rowset_writer->add_chunk(*_aggregator->aggregate_result());
        }
    } else {
        rowset_writer->add_chunk(*tmp_chunk);
    }

    if (rowset_writer->flush() != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to finalizing writer.";
        process_err();
        return false;
    }

    return true;
}

bool ChunkMerger::_make_heap(std::vector<ChunkPtr>& chunk_arr) {
    for (auto chunk : chunk_arr) {
        MergeElement element;
        element.chunk = chunk.get();
        element.row_index = 0;

        _heap.push(element);
    }

    return true;
}

bool ChunkMerger::_pop_heap() {
    MergeElement element = _heap.top();
    _heap.pop();

    if (++element.row_index >= element.chunk->num_rows()) {
        return true;
    }

    _heap.push(element);
    return true;
}

bool LinkedSchemaChange::process(vectorized::TabletReader* reader, RowsetWriter* new_rowset_writer,
                                 TabletSharedPtr new_tablet, TabletSharedPtr base_tablet, RowsetSharedPtr rowset) {
#ifndef BE_TEST
    Status st = tls_thread_status.mem_tracker()->check_mem_limit("LinkedSchemaChange");
    if (!st.ok()) {
        LOG(WARNING) << "fail to execute schema change: " << st.message() << std::endl;
        return false;
    }
#endif

    OLAPStatus status =
            new_rowset_writer->add_rowset_for_linked_schema_change(rowset, _chunk_changer.get_schema_mapping());
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to convert rowset."
                     << ", new_tablet=" << new_tablet->full_name() << ", base_tablet=" << base_tablet->full_name()
                     << ", version=" << new_rowset_writer->version().first << "-"
                     << new_rowset_writer->version().second;
        return false;
    }

    return true;
}

bool SchemaChangeDirectly::process(vectorized::TabletReader* reader, RowsetWriter* new_rowset_writer,
                                   TabletSharedPtr new_tablet, TabletSharedPtr base_tablet, RowsetSharedPtr rowset) {
    bool result = true;

    vectorized::Schema base_schema = ChunkHelper::convert_schema_to_format_v2(base_tablet->tablet_schema());
    ChunkPtr base_chunk = ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);

    vectorized::Schema new_schema = ChunkHelper::convert_schema_to_format_v2(new_tablet->tablet_schema());
    ChunkPtr new_chunk = ChunkHelper::new_chunk(new_schema, config::vector_chunk_size);

    std::unique_ptr<MemPool> mem_pool(new MemPool());
    do {
#ifndef BE_TEST
        Status st = tls_thread_status.mem_tracker()->check_mem_limit("DirectSchemaChange");
        if (!st.ok()) {
            LOG(WARNING) << "fail to execute schema change: " << st.message() << std::endl;
            return false;
        }
#endif
        Status status = reader->do_get_next(base_chunk.get());

        if (!status.ok()) {
            if (!status.is_end_of_file()) {
                LOG(WARNING) << "failed to get next chunk, status is:" << status.to_string();
                return false;
            } else {
                break;
            }
        }
        if (!_chunk_changer.change_chunk(base_chunk, new_chunk, base_tablet, new_tablet, mem_pool.get())) {
            LOG(WARNING) << "failed to change data in chunk";
            return false;
        }

        OLAPStatus olap_st = new_rowset_writer->add_chunk(*new_chunk);
        if (olap_st != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to write chunk";
            return false;
        }

        new_chunk->reset();
        base_chunk->reset();
        mem_pool->clear();
    } while (base_chunk->num_rows() == 0);

    if (base_chunk->num_rows() != 0) {
        if (!_chunk_changer.change_chunk(base_chunk, new_chunk, base_tablet, new_tablet, mem_pool.get())) {
            LOG(WARNING) << "failed to change data in chunk";
            return false;
        }

        OLAPStatus olap_st = new_rowset_writer->add_chunk(*new_chunk);
        if (olap_st != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to write chunk";
            return false;
        }
    }

    if (OLAP_SUCCESS != new_rowset_writer->flush()) {
        LOG(WARNING) << "failed to flush rowset writer";
        result = false;
    }

    return result;
}

SchemaChangeWithSorting::SchemaChangeWithSorting(ChunkChanger& chunk_changer, size_t memory_limitation)
        : SchemaChange(),
          _chunk_changer(chunk_changer),
          _memory_limitation(memory_limitation),
          _chunk_allocator(nullptr) {}

SchemaChangeWithSorting::~SchemaChangeWithSorting() {
    SAFE_DELETE(_chunk_allocator);
}

bool SchemaChangeWithSorting::process(vectorized::TabletReader* reader, RowsetWriter* new_rowset_writer,
                                      TabletSharedPtr new_tablet, TabletSharedPtr base_tablet, RowsetSharedPtr rowset) {
    bool result = true;

    if (_chunk_allocator == nullptr) {
        _chunk_allocator = new (nothrow) ChunkAllocator(new_tablet->tablet_schema(), _memory_limitation);
        if (_chunk_allocator == nullptr) {
            LOG(FATAL) << "failed to malloc chunk allocator. size=" << sizeof(ChunkAllocator);
            return false;
        }
    }
    std::vector<ChunkPtr> chunk_arr;
    vectorized::Schema base_schema = ChunkHelper::convert_schema(base_tablet->tablet_schema());
    vectorized::Schema new_schema = ChunkHelper::convert_schema(new_tablet->tablet_schema());

    ChunkSorter chunk_sorter(_chunk_allocator);
    std::unique_ptr<MemPool> mem_pool(new MemPool());

    DeferOp release_chunkarr([this, &chunk_arr] {
        for (auto& it : chunk_arr) {
            this->_chunk_allocator->release(it, it->num_rows());
        }
    });
    while (true) {
#ifndef BE_TEST
        Status st = tls_thread_status.mem_tracker()->check_mem_limit("SortSchemaChange");
        if (!st.ok()) {
            LOG(WARNING) << "fail to execute schema change: " << st.message() << std::endl;
            return false;
        }
#endif
        ChunkPtr base_chunk = ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);
        ChunkPtr new_chunk = nullptr;
        Status status = reader->do_get_next(base_chunk.get());
        if (!status.ok()) {
            if (!status.is_end_of_file()) {
                LOG(WARNING) << "failed to get next chunk, status is:" << status.to_string();
                return false;
            } else if (base_chunk->num_rows() <= 0) {
                break;
            }
        }

        if (!_chunk_allocator->is_memory_enough_to_sort(2 * base_chunk->num_rows(), chunk_sorter.allocated_rows())) {
            LOG(INFO) << "do internal sorting because of memory limit";
            if (chunk_arr.size() < 1) {
                LOG(WARNING) << "Memory limitation is too small for Schema Change."
                             << "memory_limitation=" << _memory_limitation;
                return false;
            }

            if (!_internal_sorting(chunk_arr, new_rowset_writer, new_tablet)) {
                LOG(WARNING) << "failed to sorting internally.";
                return false;
            }

            for (vector<ChunkPtr>::iterator it = chunk_arr.begin(); it != chunk_arr.end(); ++it) {
                _chunk_allocator->release(*it, (*it)->num_rows());
            }

            chunk_arr.clear();
        }

        if (!_chunk_allocator->allocate(new_chunk, base_chunk->num_rows(), new_schema).ok()) {
            LOG(WARNING) << "failed to allocate chunk";
            return false;
        }

        if (!_chunk_changer.change_chunk(base_chunk, new_chunk, base_tablet, new_tablet, mem_pool.get())) {
            _chunk_allocator->release(new_chunk, base_chunk->num_rows());
            LOG(WARNING) << "failed to change data in chunk";
            return false;
        }

        if (new_chunk->num_rows() > 0) {
            if (!chunk_sorter.sort(new_chunk, new_tablet)) {
                _chunk_allocator->release(new_chunk, base_chunk->num_rows());
                LOG(WARNING) << "failed to sort chunk.";
                return false;
            }
        }

        chunk_arr.push_back(new_chunk);
        mem_pool->clear();
    }

    if (!chunk_arr.empty()) {
        if (!_internal_sorting(chunk_arr, new_rowset_writer, new_tablet)) {
            LOG(WARNING) << "failed to sorting internally.";
            return false;
        }
    }

    if (OLAP_SUCCESS != new_rowset_writer->flush()) {
        LOG(WARNING) << "failed to flush rowset writer";
        result = false;
    }

    return result;
}

bool SchemaChangeWithSorting::_internal_sorting(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* new_rowset_writer,
                                                TabletSharedPtr tablet) {
    if (chunk_arr.size() == 1) {
        new_rowset_writer->add_chunk(*(chunk_arr[0]));
        if (new_rowset_writer->flush() != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to finalizing writer.";
            return false;
        } else {
            return true;
        }
    }

    ChunkMerger merger(tablet);
    if (!merger.merge(chunk_arr, new_rowset_writer)) {
        LOG(WARNING) << "failed to merger";
        return false;
    }

    return true;
}

Status SchemaChangeHandler::process_alter_tablet_v2(const TAlterTabletReqV2& request) {
    LOG(INFO) << "begin to do request alter tablet: base_tablet_id=" << request.base_tablet_id
              << ", base_schema_hash=" << request.base_schema_hash << ", new_tablet_id=" << request.new_tablet_id
              << ", new_schema_hash=" << request.new_schema_hash << ", alter_version=" << request.alter_version
              << ", alter_version_hash=" << request.alter_version_hash;

    MonotonicStopWatch timer;
    timer.start();

    // Lock schema_change_lock util schema change info is stored in tablet header
    if (!StorageEngine::instance()->tablet_manager()->try_schema_change_lock(request.base_tablet_id)) {
        LOG(WARNING) << "failed to obtain schema change lock. "
                     << "base_tablet=" << request.base_tablet_id;
        return Status::InternalError("failed to obtain schema change lock");
    }

    DeferOp release_lock(
            [&] { StorageEngine::instance()->tablet_manager()->release_schema_change_lock(request.base_tablet_id); });

    Status status = _do_process_alter_tablet_v2(request);
    LOG(INFO) << "finished alter tablet process, status=" << status.to_string()
              << " duration: " << timer.elapsed_time() / 1000000 << "ms";
    return status;
}

Status SchemaChangeHandler::_do_process_alter_tablet_v2(const TAlterTabletReqV2& request) {
    TabletSharedPtr base_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request.base_tablet_id);
    if (base_tablet == nullptr) {
        LOG(WARNING) << "fail to find base tablet. base_tablet=" << request.base_tablet_id
                     << ", base_schema_hash=" << request.base_schema_hash;
        return Status::InternalError("failed to find base tablet");
    }

    // new tablet has to exist
    TabletSharedPtr new_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request.new_tablet_id);
    if (new_tablet == nullptr) {
        LOG(WARNING) << "fail to find new tablet."
                     << " new_tablet=" << request.new_tablet_id << ", new_schema_hash=" << request.new_schema_hash;
        return Status::InternalError("failed to find new tablet");
    }

    // check if tablet's state is not_ready, if it is ready, it means the tablet already finished
    // check whether the tablet's max continuous version == request.version
    if (new_tablet->tablet_state() != TABLET_NOTREADY) {
        Status st = _validate_alter_result(new_tablet, request);
        LOG(INFO) << "tablet's state=" << new_tablet->tablet_state()
                  << " the convert job alreay finished, check its version"
                  << " res=" << st.to_string();
        return Status::InternalError("new tablet's meta is invalid");
    }

    LOG(INFO) << "finish to validate alter tablet request. begin to convert data from base tablet "
                 "to new tablet"
              << " base_tablet=" << base_tablet->full_name() << " new_tablet=" << new_tablet->full_name();

    std::shared_lock base_migration_rlock(base_tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::InternalError("base tablet get migration r_lock failed");
    }
    std::shared_lock new_migration_rlock(new_tablet->get_migration_lock(), std::try_to_lock);
    if (!new_migration_rlock.owns_lock()) {
        return Status::InternalError("new tablet get migration r_lock failed");
    }

    if (base_tablet->keys_type() == KeysType::PRIMARY_KEYS) {
        Status st = new_tablet->updates()->load_from_base_tablet(request.alter_version, base_tablet.get());
        if (!st.ok()) {
            LOG(WARNING) << "schema change new tablet load snapshot error " << st.to_string();
            return st;
        }
        return Status::OK();
    } else {
        return _do_process_alter_tablet_v2_normal(request, base_tablet, new_tablet);
    }
}

Status SchemaChangeHandler::_do_process_alter_tablet_v2_normal(const TAlterTabletReqV2& request,
                                                               const TabletSharedPtr& base_tablet,
                                                               const TabletSharedPtr& new_tablet) {
    OLAPStatus res = OLAP_SUCCESS;
    // begin to find deltas to convert from base tablet to new tablet so that
    // obtain base tablet and new tablet's push lock and header write lock to prevent loading data

    std::vector<ColumnId> return_columns;
    RowsetSharedPtr max_rowset;
    std::vector<RowsetSharedPtr> rowsets_to_change;
    int32_t end_version = -1;
    Status status;
    {
        std::lock_guard l1(base_tablet->get_push_lock());
        std::lock_guard l2(new_tablet->get_push_lock());
        std::lock_guard l3(base_tablet->get_header_lock());
        std::lock_guard l4(new_tablet->get_header_lock());

        // check if the tablet has alter task
        // if it has alter task, it means it is under old alter process

        std::vector<Version> versions_to_be_changed;
        status = _get_versions_to_be_changed(base_tablet, &versions_to_be_changed);
        if (!status.ok()) {
            LOG(WARNING) << "fail to get version to be changed. res=" << res;
            return status;
        }
        LOG(INFO) << "versions to be changed size:" << versions_to_be_changed.size();
        for (auto& version : versions_to_be_changed) {
            RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
            rowsets_to_change.push_back(rowset);
        }
        LOG(INFO) << "rowsets_to_change size is:" << rowsets_to_change.size();

        size_t num_cols = base_tablet->tablet_schema().num_columns();
        return_columns.resize(num_cols);
        for (int i = 0; i < num_cols; ++i) {
            return_columns[i] = i;
        }

        Version max_version = base_tablet->max_version();
        max_rowset = base_tablet->rowset_with_max_version();
        if (max_rowset == nullptr || max_version.second < request.alter_version) {
            LOG(WARNING) << "base tablet's max version=" << (max_rowset == nullptr ? 0 : max_rowset->end_version())
                         << " is less than request version=" << request.alter_version;

            return Status::InternalError("base tablet's max version is less than request version");
        }

        LOG(INFO) << "begin to remove all data from new tablet to prevent rewrite."
                  << " new_tablet=" << new_tablet->full_name();
        std::vector<RowsetSharedPtr> rowsets_to_delete;
        std::vector<Version> new_tablet_versions;
        new_tablet->list_versions(&new_tablet_versions);
        for (auto& version : new_tablet_versions) {
            if (version.second <= max_rowset->end_version()) {
                RowsetSharedPtr rowset = new_tablet->get_rowset_by_version(version);
                rowsets_to_delete.push_back(rowset);
            }
        }
        LOG(INFO) << "rowsets_to_delete size is:" << rowsets_to_delete.size()
                  << " version is:" << max_rowset->end_version();
        new_tablet->modify_rowsets(std::vector<RowsetSharedPtr>(), rowsets_to_delete);
        new_tablet->set_cumulative_layer_point(-1);
        new_tablet->save_meta();
        for (auto& rowset : rowsets_to_delete) {
            // do not call rowset.remove directly, using gc thread to delete it
            StorageEngine::instance()->add_unused_rowset(rowset);
        }

        // init one delete handler
        for (auto& version : versions_to_be_changed) {
            if (version.second > end_version) {
                end_version = version.second;
            }
        }
    }

    Schema base_schema = ChunkHelper::convert_schema_to_format_v2(base_tablet->tablet_schema(), return_columns);
    Version delete_predicates_version(0, max_rowset->version().second);
    TabletReaderParams read_params;
    read_params.reader_type = ReaderType::READER_ALTER_TABLE;
    read_params.skip_aggregation = false;
    read_params.chunk_size = config::vector_chunk_size;

    std::vector<std::unique_ptr<vectorized::TabletReader>> readers;
    for (auto rowset : rowsets_to_change) {
        auto tablet_rowset_reader =
                std::make_unique<vectorized::TabletReader>(base_tablet, rowset->version(), base_schema);
        tablet_rowset_reader->set_delete_predicates_version(delete_predicates_version);
        RETURN_IF_ERROR(tablet_rowset_reader->prepare());
        RETURN_IF_ERROR(tablet_rowset_reader->open(read_params));
        readers.emplace_back(std::move(tablet_rowset_reader));
    }

    SchemaChangeParams sc_params;
    sc_params.base_tablet = base_tablet;
    sc_params.new_tablet = new_tablet;
    sc_params.rowset_readers = std::move(readers);
    sc_params.version = Version(0, end_version);
    sc_params.rowsets_to_change = rowsets_to_change;
    if (request.__isset.materialized_view_params) {
        for (auto item : request.materialized_view_params) {
            AlterMaterializedViewParam mv_param;
            mv_param.column_name = item.column_name;
            /*
             * origin_column_name is always be set now,
             * but origin_column_name may be not set in some materialized view function. eg:count(1)
            */
            if (item.__isset.origin_column_name) {
                mv_param.origin_column_name = item.origin_column_name;
            }

            /*
            * TODO(lhy)
            * Building the materialized view function for schema_change here based on defineExpr.
            * This is a trick because the current storage layer does not support expression evaluation.
            * We can refactor this part of the code until the uniform expression evaluates the logic.
            * count distinct materialized view will set mv_expr with to_bitmap or hll_hash.
            * count materialized view will set mv_expr with count.
            */
            if (item.__isset.mv_expr) {
                if (item.mv_expr.nodes[0].node_type == TExprNodeType::FUNCTION_CALL) {
                    mv_param.mv_expr = item.mv_expr.nodes[0].fn.name.function_name;
                } else if (item.mv_expr.nodes[0].node_type == TExprNodeType::CASE_EXPR) {
                    mv_param.mv_expr = "count_field";
                }
            }
            sc_params.materialized_params_map.insert(std::make_pair(item.column_name, mv_param));
        }
    }
    status = _convert_historical_rowsets(sc_params);
    if (!status.ok()) {
        return status;
    }

    {
        // set state to ready
        std::unique_lock new_wlock(new_tablet->get_header_lock());
        res = new_tablet->set_tablet_state(TabletState::TABLET_RUNNING);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to alter tablet. base_tablet=" << base_tablet->full_name()
                         << ", drop new_tablet=" << new_tablet->full_name();
            // do not drop the new tablet and its data. GC thread will
            return Status::InternalError("failed to set tablet state");
        }
        new_tablet->save_meta();
    }

    if (res == OLAP_SUCCESS) {
        // _validate_alter_result should be outside the above while loop.
        // to avoid requiring the header lock twice.
        status = _validate_alter_result(new_tablet, request);
    }

    if (!status.ok()) {
        LOG(WARNING) << "failed to alter tablet. base_tablet=" << base_tablet->full_name()
                     << ", drop new_tablet=" << new_tablet->full_name();
        // do not drop the new tablet and its data. GC thread will
        return status;
    }
    LOG(INFO) << "success to alter tablet. base_tablet=" << base_tablet->full_name();
    return Status::OK();
}

Status SchemaChangeHandler::_get_versions_to_be_changed(TabletSharedPtr base_tablet,
                                                        std::vector<Version>* versions_to_be_changed) {
    RowsetSharedPtr rowset = base_tablet->rowset_with_max_version();
    if (rowset == nullptr) {
        LOG(WARNING) << "Tablet has no version. base_tablet=" << base_tablet->full_name();
        return Status::InternalError("tablet alter version does not exists");
    }

    std::vector<Version> span_versions;
    if (!base_tablet->capture_consistent_versions(Version(0, rowset->version().second), &span_versions).ok()) {
        return Status::InternalError("capture consistent versions failed");
    }

    for (uint32_t i = 0; i < span_versions.size(); i++) {
        versions_to_be_changed->push_back(span_versions[i]);
    }

    return Status::OK();
}

Status SchemaChangeHandler::_convert_historical_rowsets(SchemaChangeParams& sc_params) {
    LOG(INFO) << "begin to convert historical rowsets for new_tablet from base_tablet."
              << " base_tablet=" << sc_params.base_tablet->full_name()
              << ", new_tablet=" << sc_params.new_tablet->full_name();

    // The filter information is added to the change, the column information of the filter is set
    // in _parse_request and some data is filtered at each change of the row block.
    ChunkChanger chunk_changer(sc_params.new_tablet->tablet_schema());

    bool sc_sorting = false;
    bool sc_directly = false;
    std::unique_ptr<SchemaChange> sc_procedure;

    // a. parse Alter request
    Status status = _parse_request(sc_params.base_tablet, sc_params.new_tablet, &chunk_changer, &sc_sorting,
                                   &sc_directly, sc_params.materialized_params_map);

    DeferOp save_meta([&sc_params] {
        std::unique_lock new_wlock(sc_params.new_tablet->get_header_lock());
        sc_params.new_tablet->save_meta();
    });
    if (!status.ok()) {
        LOG(WARNING) << "failed to parse the request. res=" << status.get_error_msg();
        return status;
    }

    if (sc_sorting) {
        LOG(INFO) << "doing schema change with sorting for base_tablet " << sc_params.base_tablet->full_name();
        size_t memory_limitation =
                static_cast<size_t>(config::memory_limitation_per_thread_for_schema_change) * 1024 * 1024 * 1024;
        sc_procedure = std::make_unique<SchemaChangeWithSorting>(chunk_changer, memory_limitation);
    } else if (sc_directly) {
        sc_procedure = std::make_unique<SchemaChangeDirectly>(chunk_changer);
    } else {
        LOG(INFO) << "doing linked schema change for base_tablet " << sc_params.base_tablet->full_name();
        sc_procedure = std::make_unique<LinkedSchemaChange>(chunk_changer);
    }

    if (sc_procedure.get() == nullptr) {
        LOG(WARNING) << "failed to malloc SchemaChange. "
                     << "malloc_size=" << sizeof(SchemaChangeWithSorting);
        status = Status::InternalError("failed to malloc SchemaChange");
        return status;
    }

    for (int i = 0; i < sc_params.rowset_readers.size(); ++i) {
        LOG(INFO) << "begin to convert a history rowset. version=" << sc_params.rowsets_to_change[i]->version().first
                  << "-" << sc_params.rowsets_to_change[i]->version().second;
        // set status for monitor
        // If only one new_table is running, ref table will be set to running
        // NOTE if the first sub_table is fail, it will continue as normal
        TabletSharedPtr new_tablet = sc_params.new_tablet;
        TabletSharedPtr base_tablet = sc_params.base_tablet;
        RowsetWriterContext writer_context(kDataFormatUnknown, config::storage_format_version);
        writer_context.rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.tablet_uid = new_tablet->tablet_uid();
        writer_context.tablet_id = new_tablet->tablet_id();
        writer_context.partition_id = new_tablet->partition_id();
        writer_context.tablet_schema_hash = new_tablet->schema_hash();
        writer_context.rowset_type = sc_params.new_tablet->tablet_meta()->preferred_rowset_type();
        writer_context.rowset_path_prefix = new_tablet->tablet_path();
        writer_context.tablet_schema = &(new_tablet->tablet_schema());
        writer_context.rowset_state = VISIBLE;
        writer_context.version = sc_params.rowsets_to_change[i]->version();
        writer_context.version_hash = sc_params.rowsets_to_change[i]->version_hash();
        writer_context.segments_overlap = sc_params.rowsets_to_change[i]->rowset_meta()->segments_overlap();

        if (sc_sorting) {
            writer_context.write_tmp = true;
        }

        std::unique_ptr<RowsetWriter> rowset_writer;
        status = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        if (!status.ok()) {
            status = Status::InternalError("build rowset writer failed");
            LOG(INFO) << "build rowset writer failed";
            return status;
        }

        if (!sc_procedure->process(sc_params.rowset_readers[i].get(), rowset_writer.get(), new_tablet, base_tablet,
                                   sc_params.rowsets_to_change[i])) {
            LOG(WARNING) << "failed to process the version."
                         << " version=" << sc_params.version.first << "-" << sc_params.version.second;
            status = Status::InternalError("process failed");
            return status;
        }
        // Add the new version of the data to the header,
        // To prevent deadlocks, be sure to lock the old table first and then the new one
        sc_params.new_tablet->obtain_push_lock();
        DeferOp new_tablet_release_lock([&] { sc_params.new_tablet->release_push_lock(); });
        RowsetSharedPtr new_rowset = rowset_writer->build();
        if (new_rowset == nullptr) {
            LOG(WARNING) << "failed to build rowset, exit alter process";
            break;
        }
        LOG(INFO) << "new rowset has " << new_rowset->num_segments() << " segments";
        status = sc_params.new_tablet->add_rowset(new_rowset, false);
        if (status.is_already_exist()) {
            LOG(WARNING) << "version already exist, version revert occured. "
                         << "tablet=" << sc_params.new_tablet->full_name() << ", version='" << sc_params.version.first
                         << "-" << sc_params.version.second;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            status = Status::OK();
        } else if (!status.ok()) {
            LOG(WARNING) << "failed to register new version. "
                         << " tablet=" << sc_params.new_tablet->full_name() << ", version=" << sc_params.version.first
                         << "-" << sc_params.version.second;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            break;
        } else {
            VLOG(3) << "register new version. tablet=" << sc_params.new_tablet->full_name()
                    << ", version=" << sc_params.version.first << "-" << sc_params.version.second;
        }

        VLOG(10) << "succeed to convert a history version."
                 << " version=" << sc_params.version.first << "-" << sc_params.version.second;
    }

    if (status.ok()) {
        status = sc_params.new_tablet->check_version_integrity(sc_params.version);
    }

    LOG(INFO) << "finish converting rowsets for new_tablet from base_tablet. "
              << "base_tablet=" << sc_params.base_tablet->full_name()
              << ", new_tablet=" << sc_params.new_tablet->full_name() << ", status is " << status.to_string();

    return status;
}

// @static
Status SchemaChangeHandler::_parse_request(
        TabletSharedPtr base_tablet, TabletSharedPtr new_tablet, ChunkChanger* chunk_changer, bool* sc_sorting,
        bool* sc_directly,
        const std::unordered_map<std::string, AlterMaterializedViewParam>& materialized_function_map) {
    for (int i = 0, new_schema_size = new_tablet->tablet_schema().num_columns(); i < new_schema_size; ++i) {
        const TabletColumn& new_column = new_tablet->tablet_schema().column(i);
        string column_name = std::string(new_column.name());
        ColumnMapping* column_mapping = chunk_changer->get_mutable_column_mapping(i);

        if (materialized_function_map.find(column_name) != materialized_function_map.end()) {
            AlterMaterializedViewParam mvParam = materialized_function_map.find(column_name)->second;
            column_mapping->materialized_function = mvParam.mv_expr;
            std::string origin_column_name = mvParam.origin_column_name;
            int32_t column_index = base_tablet->field_index(origin_column_name);
            if (column_index >= 0) {
                column_mapping->ref_column = column_index;
                continue;
            } else {
                LOG(WARNING) << "referenced column was missing. "
                             << "[column=" << column_name << " referenced_column=" << column_index << "]";
                return Status::InternalError("referenced column was missing");
            }
        }

        int32_t column_index = base_tablet->field_index(column_name);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
            continue;
        }

        // to handle new added column
        //if (new_column_schema.is_allow_null || new_column_schema.has_default_value) {
        {
            column_mapping->ref_column = -1;

            if (i < base_tablet->num_short_key_columns()) {
                *sc_directly = true;
            }

            if (!_init_column_mapping(column_mapping, new_column, new_column.default_value()).ok()) {
                return Status::InternalError("init column mapping failed");
            }

            LOG(INFO) << "A column with default value will be added after schema changing. "
                      << "column=" << column_name << ", default_value=" << new_column.default_value();
            continue;
        }

        column_mapping->ref_column = -1;
        if (!_init_column_mapping(column_mapping, new_column, "").ok()) {
            return Status::InternalError("init column mapping failed");
        }
        LOG(INFO) << "A new schema delta is converted while dropping column. "
                  << "Dropped column will be assigned as '0' for the older schema. "
                  << "column=" << column_name;
    }

    // Check if re-aggregation is needed.
    *sc_sorting = false;
    // If the reference sequence of the Key column is out of order, it needs to be reordered
    int num_default_value = 0;

    for (int i = 0, new_schema_size = new_tablet->num_key_columns(); i < new_schema_size; ++i) {
        ColumnMapping* column_mapping = chunk_changer->get_mutable_column_mapping(i);

        if (column_mapping->ref_column < 0) {
            num_default_value++;
            continue;
        }

        if (column_mapping->ref_column != i - num_default_value) {
            *sc_sorting = true;
            return Status::OK();
        }
    }

    const TabletSchema& ref_tablet_schema = base_tablet->tablet_schema();
    const TabletSchema& new_tablet_schema = new_tablet->tablet_schema();
    if (ref_tablet_schema.keys_type() != new_tablet_schema.keys_type()) {
        // only when base table is dup and mv is agg
        // the rollup job must be reagg.
        *sc_sorting = true;
        return Status::OK();
    }

    // If the sort of key has not been changed but the new keys num is less then base's,
    // the new table should be re agg.
    // So we also need to set  sc_sorting = true.
    // A, B, C are keys(sort keys), D is value
    // followings need resort:
    //      old keys:    A   B   C   D
    //      new keys:    A   B
    if (new_tablet_schema.keys_type() != KeysType::DUP_KEYS &&
        new_tablet->num_key_columns() < base_tablet->num_key_columns()) {
        // this is a table with aggregate key type, and num of key columns in new schema
        // is less, which means the data in new tablet should be more aggregated.
        // so we use sorting schema change to sort and merge the data.
        *sc_sorting = true;
        return Status::OK();
    }

    if (base_tablet->num_short_key_columns() != new_tablet->num_short_key_columns()) {
        // the number of short_keys changed, can't do linked schema change
        *sc_directly = true;
        return Status::OK();
    }

    for (size_t i = 0; i < new_tablet->num_columns(); ++i) {
        ColumnMapping* column_mapping = chunk_changer->get_mutable_column_mapping(i);
        if (column_mapping->ref_column < 0) {
            continue;
        } else {
            auto& new_column = new_tablet_schema.column(i);
            auto& ref_column = ref_tablet_schema.column(column_mapping->ref_column);
            if (new_column.type() != ref_column.type()) {
                *sc_directly = true;
                return Status::OK();
            } else if (is_decimalv3_field_type(new_column.type()) &&
                       (new_column.precision() != ref_column.precision() || new_column.scale() != ref_column.scale())) {
                *sc_directly = true;
                return Status::OK();
            } else if (new_column.length() != ref_column.length()) {
                *sc_directly = true;
                return Status::OK();
            } else if (new_column.is_bf_column() != ref_column.is_bf_column()) {
                *sc_directly = true;
                return Status::OK();
            } else if (new_column.has_bitmap_index() != ref_column.has_bitmap_index()) {
                *sc_directly = true;
                return Status::OK();
            }
        }
    }

    if (base_tablet->delete_predicates().size() != 0) {
        //there exists delete condition in header, can't do linked schema change
        *sc_directly = true;
    }

    if (base_tablet->tablet_meta()->preferred_rowset_type() != new_tablet->tablet_meta()->preferred_rowset_type()) {
        // If the base_tablet and new_tablet rowset types are different, just use directly type
        *sc_directly = true;
    }

    return Status::OK();
}

Status SchemaChangeHandler::_init_column_mapping(ColumnMapping* column_mapping, const TabletColumn& column_schema,
                                                 const std::string& value) {
    column_mapping->default_value = WrapperField::create(column_schema);

    if (column_mapping->default_value == nullptr) {
        return Status::InternalError("malloc error");
    }

    if (column_schema.is_nullable() && value.length() == 0) {
        column_mapping->default_value->set_null();
    } else {
        column_mapping->default_value->from_string(value);
    }

    return Status::OK();
}

Status SchemaChangeHandler::_validate_alter_result(TabletSharedPtr new_tablet, const TAlterTabletReqV2& request) {
    Version max_continuous_version = new_tablet->max_continuous_version_from_beginning();
    LOG(INFO) << "find max continuous version of tablet=" << new_tablet->full_name()
              << ", start_version=" << max_continuous_version.first
              << ", end_version=" << max_continuous_version.second;
    if (max_continuous_version.second >= request.alter_version) {
        return Status::OK();
    } else {
        return Status::InternalError("version missed");
    }
}

} // namespace vectorized
} // namespace starrocks
