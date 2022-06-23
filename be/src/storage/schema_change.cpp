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

#include "storage/schema_change.h"

#include <signal.h>

#include <memory>
#include <utility>
#include <vector>

#include "exec/vectorized/sorting/sorting.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "storage/chunk_aggregator.h"
#include "storage/convert_helper.h"
#include "storage/memtable.h"
#include "storage/memtable_rowset_writer_sink.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"
#include "storage/wrapper_field.h"
#include "util/defer_op.h"
#include "util/percentile_value.h"
#include "util/unaligned_access.h"

namespace starrocks::vectorized {

using ChunkRow = std::pair<size_t, Chunk*>;

int compare_chunk_row(const ChunkRow& lhs, const ChunkRow& rhs) {
    for (uint16_t i = 0; i < lhs.second->schema()->num_key_fields(); ++i) {
        int res = lhs.second->get_column_by_index(i)->compare_at(lhs.first, rhs.first,
                                                                 *rhs.second->get_column_by_index(i), -1);
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

    bool sort(ChunkPtr& chunk, const TabletSharedPtr& new_tablet);

private:
    ChunkAllocator* _chunk_allocator = nullptr;
    ChunkPtr _swap_chunk;
    size_t _max_allocated_rows;
};

// TODO: optimize it with vertical sort
class ChunkMerger {
public:
    explicit ChunkMerger(TabletSharedPtr tablet);
    virtual ~ChunkMerger();

    bool merge(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* rowset_writer);
    static void aggregate_chunk(ChunkAggregator& aggregator, ChunkPtr& chunk, RowsetWriter* rowset_writer);

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
    for (auto it = _schema_mapping.begin(); it != _schema_mapping.end(); ++it) {
        SAFE_DELETE(it->default_value);
    }
    _schema_mapping.clear();
}

ColumnMapping* ChunkChanger::get_mutable_column_mapping(size_t column_index) {
    if (column_index >= _schema_mapping.size()) {
        return nullptr;
    }
    return &_schema_mapping[column_index];
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

#define COLUMN_APPEND_DATUM()                                                     \
    for (size_t row_index = 0; row_index < base_chunk->num_rows(); ++row_index) { \
        new_col->append_datum(dst_datum);                                         \
    }

struct ConvertTypeMapHash {
    size_t operator()(const std::pair<FieldType, FieldType>& pair) const { return (pair.first + 31) ^ pair.second; }
};

class ConvertTypeResolver {
    DECLARE_SINGLETON(ConvertTypeResolver);

public:
    bool convert_type_exist(const FieldType from_type, const FieldType to_type) const {
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
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_JSON>();

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
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_JSON, OLAP_FIELD_TYPE_VARCHAR>();

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

ConvertTypeResolver::~ConvertTypeResolver() = default;

const MaterializeTypeConverter* ChunkChanger::_get_materialize_type_converter(std::string materialized_function,
                                                                              FieldType type) {
    if (materialized_function == "to_bitmap") {
        return get_materialized_converter(type, OLAP_MATERIALIZE_TYPE_BITMAP);
    } else if (materialized_function == "hll_hash") {
        return get_materialized_converter(type, OLAP_MATERIALIZE_TYPE_HLL);
    } else if (materialized_function == "count_field") {
        return get_materialized_converter(type, OLAP_MATERIALIZE_TYPE_COUNT);
    } else if (materialized_function == "percentile_hash") {
        return get_materialized_converter(type, OLAP_MATERIALIZE_TYPE_PERCENTILE);
    } else {
        return nullptr;
    }
}

bool ChunkChanger::change_chunk(ChunkPtr& base_chunk, ChunkPtr& new_chunk, const TabletMetaSharedPtr& base_tablet_meta,
                                const TabletMetaSharedPtr& new_tablet_meta, MemPool* mem_pool) {
    if (new_chunk->num_columns() != _schema_mapping.size()) {
        LOG(WARNING) << "new chunk does not match with schema mapping rules. "
                     << "chunk_schema_size=" << new_chunk->num_columns()
                     << ", mapping_schema_size=" << _schema_mapping.size();
        return false;
    }

    for (size_t i = 0; i < new_chunk->num_columns(); ++i) {
        int ref_column = _schema_mapping[i].ref_column;
        if (ref_column >= 0) {
            FieldType ref_type =
                    TypeUtils::to_storage_format_v2(base_tablet_meta->tablet_schema().column(ref_column).type());
            FieldType new_type = TypeUtils::to_storage_format_v2(new_tablet_meta->tablet_schema().column(i).type());
            if (!_schema_mapping[i].materialized_function.empty()) {
                const auto& materialized_function = _schema_mapping[i].materialized_function;
                const MaterializeTypeConverter* converter =
                        _get_materialize_type_converter(materialized_function, ref_type);
                VLOG(3) << "_schema_mapping[" << i << "].materialized_function: " << materialized_function;
                if (converter == nullptr) {
                    LOG(WARNING) << "error materialized view function : " << materialized_function;
                    return false;
                }
                ColumnPtr& base_col = base_chunk->get_column_by_index(ref_column);
                ColumnPtr& new_col = new_chunk->get_column_by_index(i);
                Field ref_field = ChunkHelper::convert_field_to_format_v2(
                        ref_column, base_tablet_meta->tablet_schema().column(ref_column));
                Status st = converter->convert_materialized(base_col, new_col, ref_field.type().get());
                if (!st.ok()) {
                    return false;
                }
                continue;
            }

            int reftype_precision = base_tablet_meta->tablet_schema().column(ref_column).precision();
            int reftype_scale = base_tablet_meta->tablet_schema().column(ref_column).scale();
            int newtype_precision = new_tablet_meta->tablet_schema().column(i).precision();
            int newtype_scale = new_tablet_meta->tablet_schema().column(i).scale();

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
                        slice.size = new_tablet_meta->tablet_schema().column(i).length();
                        slice.data = reinterpret_cast<char*>(mem_pool->allocate(slice.size));
                        if (slice.data == nullptr) {
                            LOG(WARNING) << "failed to allocate memory in mem_pool";
                            return false;
                        }
                        memset(slice.data, 0, slice.size);
                        size_t copy_size = slice.size < base_slice.size ? slice.size : base_slice.size;
                        memcpy(slice.data, base_slice.data, copy_size);
                        new_datum.set(slice);
                        new_col->append_datum(new_datum);
                    }
                } else if (new_col->is_nullable() != base_col->is_nullable()) {
                    new_col->append(*base_col.get());
                } else {
                    new_col = base_col;
                }
            } else if (ConvertTypeResolver::instance()->convert_type_exist(ref_type, new_type)) {
                auto converter = get_type_converter(ref_type, new_type);
                if (converter == nullptr) {
                    LOG(WARNING) << "failed to get type converter, from_type=" << ref_type << ", to_type" << new_type;
                    return false;
                }

                Field ref_field = ChunkHelper::convert_field_to_format_v2(
                        ref_column, base_tablet_meta->tablet_schema().column(ref_column));
                Field new_field =
                        ChunkHelper::convert_field_to_format_v2(i, new_tablet_meta->tablet_schema().column(i));

                Status st = converter->convert_column(ref_field.type().get(), *base_col, new_field.type().get(),
                                                      new_col.get(), mem_pool);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to convert " << field_type_to_string(ref_type) << " to "
                                 << field_type_to_string(new_type);
                    return false;
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
                if (new_type < ref_type) {
                    LOG(INFO) << "type degraded while altering column. "
                              << "column=" << new_tablet_meta->tablet_schema().column(i).name()
                              << ", origin_type=" << field_type_to_string(ref_type)
                              << ", alter_type=" << field_type_to_string(new_type);
                }
            }
        } else {
            ColumnPtr& new_col = new_chunk->get_column_by_index(i);
            Datum dst_datum;
            if (_schema_mapping[i].default_value->is_null()) {
                dst_datum.set_null();
                COLUMN_APPEND_DATUM();
            } else {
                Field new_field =
                        ChunkHelper::convert_field_to_format_v2(i, new_tablet_meta->tablet_schema().column(i));
                const FieldType field_type = new_field.type()->type();
                std::string tmp = _schema_mapping[i].default_value->to_string();
                if (field_type == OLAP_FIELD_TYPE_HLL || field_type == OLAP_FIELD_TYPE_OBJECT ||
                    field_type == OLAP_FIELD_TYPE_PERCENTILE) {
                    switch (field_type) {
                    case OLAP_FIELD_TYPE_HLL: {
                        HyperLogLog hll(tmp);
                        dst_datum.set_hyperloglog(&hll);
                        COLUMN_APPEND_DATUM();
                        break;
                    }
                    case OLAP_FIELD_TYPE_OBJECT: {
                        BitmapValue bitmap(tmp);
                        dst_datum.set_bitmap(&bitmap);
                        COLUMN_APPEND_DATUM();
                        break;
                    }
                    case OLAP_FIELD_TYPE_PERCENTILE: {
                        PercentileValue percentile(tmp);
                        dst_datum.set_percentile(&percentile);
                        COLUMN_APPEND_DATUM();
                        break;
                    }
                    default:
                        LOG(WARNING) << "the column type is wrong. column_type: " << field_type_to_string(field_type);
                        return false;
                    }
                } else {
                    Status st = datum_from_string(new_field.type().get(), &dst_datum, tmp, nullptr);
                    if (!st.ok()) {
                        LOG(WARNING) << "create datum from string failed: status=" << st;
                        return false;
                    }
                    COLUMN_APPEND_DATUM();
                }
            }
        }
    }
    return true;
}

bool ChunkChanger::change_chunkV2(ChunkPtr& base_chunk, ChunkPtr& new_chunk, const Schema& base_schema,
                                  const Schema& new_schema, MemPool* mem_pool) {
    if (new_chunk->num_columns() != _schema_mapping.size()) {
        LOG(WARNING) << "new chunk does not match with schema mapping rules. "
                     << "chunk_schema_size=" << new_chunk->num_columns()
                     << ", mapping_schema_size=" << _schema_mapping.size();
        return false;
    }

    for (size_t i = 0; i < new_chunk->num_columns(); ++i) {
        int ref_column = _schema_mapping[i].ref_column;
        int base_index = _schema_mapping[i].ref_base_reader_column_index;
        if (ref_column >= 0) {
            const TypeInfoPtr& ref_type_info = base_schema.field(base_index)->type();
            const TypeInfoPtr& new_type_info = new_schema.field(i)->type();
            ColumnPtr& base_col = base_chunk->get_column_by_index(base_index);
            ColumnPtr& new_col = new_chunk->get_column_by_index(i);

            if (!_schema_mapping[i].materialized_function.empty()) {
                const auto& materialized_function = _schema_mapping[i].materialized_function;
                const MaterializeTypeConverter* converter =
                        _get_materialize_type_converter(materialized_function, ref_type_info->type());
                VLOG(3) << "_schema_mapping[" << i << "].materialized_function: " << materialized_function;
                if (converter == nullptr) {
                    LOG(WARNING) << "error materialized view function : " << materialized_function;
                    return false;
                }
                Status st = converter->convert_materialized(base_col, new_col, ref_type_info.get());
                if (!st.ok()) {
                    return false;
                }
                continue;
            }

            int reftype_precision = ref_type_info->precision();
            int reftype_scale = ref_type_info->scale();
            int newtype_precision = new_type_info->precision();
            int newtype_scale = new_type_info->scale();
            auto ref_type = ref_type_info->type();
            auto new_type = new_type_info->type();

            if (new_type == ref_type && (!is_decimalv3_field_type(new_type) ||
                                         (reftype_precision == newtype_precision && reftype_scale == newtype_scale))) {
                if (new_col->is_nullable() != base_col->is_nullable()) {
                    new_col->append(*base_col.get());
                } else {
                    new_col = base_col;
                }
            } else if (ConvertTypeResolver::instance()->convert_type_exist(ref_type, new_type)) {
                auto converter = get_type_converter(ref_type, new_type);
                if (converter == nullptr) {
                    LOG(WARNING) << "failed to get type converter, from_type=" << ref_type << ", to_type" << new_type;
                    return false;
                }
                Status st = converter->convert_column(ref_type_info.get(), *base_col, new_type_info.get(),
                                                      new_col.get(), mem_pool);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to convert " << field_type_to_string(ref_type) << " to "
                                 << field_type_to_string(new_type);
                    return false;
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
                if (new_type < ref_type) {
                    LOG(INFO) << "type degraded while altering column. "
                              << "column=" << new_schema.field(i)->name()
                              << ", origin_type=" << field_type_to_string(ref_type)
                              << ", alter_type=" << field_type_to_string(new_type);
                }
            }
        } else {
            ColumnPtr& new_col = new_chunk->get_column_by_index(i);
            Datum dst_datum;
            if (_schema_mapping[i].default_value->is_null()) {
                dst_datum.set_null();
                COLUMN_APPEND_DATUM();
            } else {
                TypeInfoPtr new_type_info = new_schema.field(i)->type();
                const FieldType field_type = new_type_info->type();
                std::string tmp = _schema_mapping[i].default_value->to_string();
                if (field_type == OLAP_FIELD_TYPE_HLL || field_type == OLAP_FIELD_TYPE_OBJECT ||
                    field_type == OLAP_FIELD_TYPE_PERCENTILE) {
                    switch (field_type) {
                    case OLAP_FIELD_TYPE_HLL: {
                        HyperLogLog hll(tmp);
                        dst_datum.set_hyperloglog(&hll);
                        COLUMN_APPEND_DATUM();
                        break;
                    }
                    case OLAP_FIELD_TYPE_OBJECT: {
                        BitmapValue bitmap(tmp);
                        dst_datum.set_bitmap(&bitmap);
                        COLUMN_APPEND_DATUM();
                        break;
                    }
                    case OLAP_FIELD_TYPE_PERCENTILE: {
                        PercentileValue percentile(tmp);
                        dst_datum.set_percentile(&percentile);
                        COLUMN_APPEND_DATUM();
                        break;
                    }
                    default:
                        LOG(WARNING) << "the column type is wrong. column_type: " << field_type_to_string(field_type);
                        return false;
                    }
                } else {
                    Status st = datum_from_string(new_type_info.get(), &dst_datum, tmp, nullptr);
                    if (!st.ok()) {
                        LOG(WARNING) << "create datum from string failed: status=" << st;
                        return false;
                    }
                    COLUMN_APPEND_DATUM();
                }
            }
        }
    }
    return true;
}

#undef CONVERT_FROM_TYPE
#undef TYPE_REINTERPRET_CAST
#undef ASSIGN_DEFAULT_VALUE
#undef COLUMN_APPEND_DATUM

ChunkSorter::ChunkSorter(ChunkAllocator* chunk_allocator)
        : _chunk_allocator(chunk_allocator), _swap_chunk(nullptr), _max_allocated_rows(0) {}

ChunkSorter::~ChunkSorter() = default;

bool ChunkSorter::sort(ChunkPtr& chunk, const TabletSharedPtr& new_tablet) {
    Schema new_schema = ChunkHelper::convert_schema_to_format_v2(new_tablet->tablet_schema());
    if (_swap_chunk == nullptr || _max_allocated_rows < chunk->num_rows()) {
        Status st = ChunkAllocator::allocate(_swap_chunk, chunk->num_rows(), new_schema);
        if (_swap_chunk == nullptr || !st.ok()) {
            LOG(WARNING) << "allocate swap chunk for sort failed: " << st.to_string();
            return false;
        }
        _max_allocated_rows = chunk->num_rows();
    }

    _swap_chunk->reset();

    Columns key_columns;
    std::vector<int> sort_orders;
    std::vector<int> null_firsts;
    for (int i = 0; i < chunk->schema()->num_key_fields(); i++) {
        key_columns.push_back(chunk->get_column_by_index(i));
        sort_orders.push_back(1);
        null_firsts.push_back(-1);
    }

    SmallPermutation perm = create_small_permutation(chunk->num_rows());
    Status st = stable_sort_and_tie_columns(false, key_columns, sort_orders, null_firsts, &perm);
    CHECK(st.ok());
    std::vector<uint32_t> selective;
    permutate_to_selective(perm, &selective);
    _swap_chunk = chunk->clone_empty_with_schema();
    _swap_chunk->append_selective(*chunk, selective.data(), 0, chunk->num_rows());

    chunk->swap_chunk(*_swap_chunk);
    return true;
}

ChunkAllocator::ChunkAllocator(const TabletSchema& tablet_schema, size_t memory_limitation)
        : _tablet_schema(tablet_schema), _memory_limitation(memory_limitation) {
    // Before the first chunk is readed, for the Varchar type, we can't get the actual size,
    // so we can only conservatively estimate a value for variable length type.
    // Then later, row_len will be adjusted according to the Chunk that has been read.
    _row_len = tablet_schema.estimate_row_size(8);
}

bool ChunkAllocator::is_memory_enough_to_sort(size_t num_rows) const {
    size_t chunk_size = _row_len * num_rows;
    return static_cast<double>(_memory_allocated + chunk_size) < static_cast<double>(_memory_limitation) * 0.8;
}

Status ChunkAllocator::allocate(ChunkPtr& chunk, size_t num_rows, Schema& schema) {
    chunk = ChunkHelper::new_chunk(schema, num_rows);
    if (chunk == nullptr) {
        LOG(WARNING) << "ChunkAllocator allocate chunk failed.";
        return Status::InternalError("allocate chunk failed");
    }

    return Status::OK();
}

ChunkMerger::ChunkMerger(TabletSharedPtr tablet) : _tablet(std::move(tablet)), _aggregator(nullptr) {}

ChunkMerger::~ChunkMerger() {
    if (_aggregator != nullptr) {
        _aggregator->close();
    }
}

void ChunkMerger::aggregate_chunk(ChunkAggregator& aggregator, ChunkPtr& chunk, RowsetWriter* rowset_writer) {
    aggregator.aggregate();
    while (aggregator.is_finish()) {
        (void)rowset_writer->add_chunk(*aggregator.aggregate_result());
        aggregator.aggregate_reset();
        aggregator.aggregate();
    }

    DCHECK(aggregator.source_exhausted());
    aggregator.update_source(chunk);
    aggregator.aggregate();

    while (aggregator.is_finish()) {
        (void)rowset_writer->add_chunk(*aggregator.aggregate_result());
        aggregator.aggregate_reset();
        aggregator.aggregate();
    }
}

bool ChunkMerger::merge(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* rowset_writer) {
    auto process_err = [this] {
        VLOG(3) << "merge chunk failed";
        while (!_heap.empty()) {
            _heap.pop();
        }
    };

    _make_heap(chunk_arr);
    size_t nread = 0;
    Schema new_schema = ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema());
    ChunkPtr tmp_chunk = ChunkHelper::new_chunk(new_schema, config::vector_chunk_size);
    if (_tablet->keys_type() == KeysType::AGG_KEYS) {
        _aggregator = std::make_unique<ChunkAggregator>(&new_schema, config::vector_chunk_size, 0);
    }

    StorageEngine* storage_engine = ExecEnv::GetInstance()->storage_engine();
    bool bg_worker_stopped = storage_engine->bg_worker_stopped();
    while (!_heap.empty() && !bg_worker_stopped) {
        if (tmp_chunk->capacity_limit_reached() || nread >= config::vector_chunk_size) {
            if (_tablet->keys_type() == KeysType::AGG_KEYS) {
                aggregate_chunk(*_aggregator, tmp_chunk, rowset_writer);
            } else {
                (void)rowset_writer->add_chunk(*tmp_chunk);
            }
            tmp_chunk->reset();
            nread = 0;
        }

        tmp_chunk->append(*_heap.top().chunk, _heap.top().row_index, 1);
        nread += 1;
        if (!_pop_heap()) {
            LOG(WARNING) << "get next chunk from heap failed";
            process_err();
            return false;
        }
        bg_worker_stopped = ExecEnv::GetInstance()->storage_engine()->bg_worker_stopped();
    }

    if (bg_worker_stopped) {
        return false;
    }

    if (_tablet->keys_type() == KeysType::AGG_KEYS) {
        aggregate_chunk(*_aggregator, tmp_chunk, rowset_writer);
        if (_aggregator->has_aggregate_data()) {
            _aggregator->aggregate();
            (void)rowset_writer->add_chunk(*_aggregator->aggregate_result());
        }
    } else {
        (void)rowset_writer->add_chunk(*tmp_chunk);
    }

    if (auto st = rowset_writer->flush(); !st.ok()) {
        LOG(WARNING) << "failed to finalizing writer: " << st;
        process_err();
        return false;
    }

    return true;
}

bool ChunkMerger::_make_heap(std::vector<ChunkPtr>& chunk_arr) {
    for (const auto& chunk : chunk_arr) {
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

bool LinkedSchemaChange::process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                                 TabletSharedPtr base_tablet, RowsetSharedPtr rowset) {
#ifndef BE_TEST
    Status st = CurrentThread::mem_tracker()->check_mem_limit("LinkedSchemaChange");
    if (!st.ok()) {
        LOG(WARNING) << "fail to execute schema change: " << st.message() << std::endl;
        return false;
    }
#endif

    Status status =
            new_rowset_writer->add_rowset_for_linked_schema_change(rowset, _chunk_changer->get_schema_mapping());
    if (!status.ok()) {
        LOG(WARNING) << "fail to convert rowset."
                     << ", new_tablet=" << new_tablet->full_name() << ", base_tablet=" << base_tablet->full_name()
                     << ", version=" << new_rowset_writer->version();
        return false;
    }
    return true;
}

Status LinkedSchemaChange::processV2(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                                     TabletSharedPtr base_tablet, RowsetSharedPtr rowset) {
    if (process(reader, new_rowset_writer, new_tablet, base_tablet, rowset)) {
        return Status::OK();
    } else {
        return Status::InternalError("failed to proccessV2 LinkedSchemaChange");
    }
}

bool SchemaChangeDirectly::process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                                   TabletSharedPtr base_tablet, RowsetSharedPtr rowset) {
    Schema base_schema = ChunkHelper::convert_schema_to_format_v2(base_tablet->tablet_schema());
    ChunkPtr base_chunk = ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);

    Schema new_schema = ChunkHelper::convert_schema_to_format_v2(new_tablet->tablet_schema());
    ChunkPtr new_chunk = ChunkHelper::new_chunk(new_schema, config::vector_chunk_size);

    std::unique_ptr<MemPool> mem_pool(new MemPool());
    do {
        bool bg_worker_stopped = ExecEnv::GetInstance()->storage_engine()->bg_worker_stopped();
        if (bg_worker_stopped) {
            return false;
        }
#ifndef BE_TEST
        Status st = CurrentThread::mem_tracker()->check_mem_limit("DirectSchemaChange");
        if (!st.ok()) {
            LOG(WARNING) << "fail to execute schema change: " << st.message() << std::endl;
            return false;
        }
#endif
        Status status = reader->do_get_next(base_chunk.get());

        if (!status.ok()) {
            if (status.is_end_of_file()) {
                break;
            } else {
                LOG(WARNING) << "tablet reader failed to get next chunk, status: " << status.get_error_msg();
                return false;
            }
        }
        if (!_chunk_changer->change_chunk(base_chunk, new_chunk, base_tablet->tablet_meta(), new_tablet->tablet_meta(),
                                          mem_pool.get())) {
            std::string err_msg = Substitute("failed to convert chunk data. base tablet:$0, new tablet:$1",
                                             base_tablet->tablet_id(), new_tablet->tablet_id());
            LOG(WARNING) << err_msg;
            return false;
        }
        if (auto tmp_st = new_rowset_writer->add_chunk(*new_chunk); !tmp_st.ok()) {
            std::string err_msg = Substitute(
                    "failed to execute schema change. base tablet:$0, new_tablet:$1. err msg: failed to add chunk to "
                    "rowset writer",
                    base_tablet->tablet_id(), new_tablet->tablet_id());
            LOG(WARNING) << err_msg;
            return false;
        }
        base_chunk->reset();
        new_chunk->reset();
        mem_pool->clear();
    } while (base_chunk->num_rows() == 0);

    if (base_chunk->num_rows() != 0) {
        if (!_chunk_changer->change_chunk(base_chunk, new_chunk, base_tablet->tablet_meta(), new_tablet->tablet_meta(),
                                          mem_pool.get())) {
            std::string err_msg = Substitute("failed to convert chunk data. base tablet:$0, new tablet:$1",
                                             base_tablet->tablet_id(), new_tablet->tablet_id());
            LOG(WARNING) << err_msg;
            return false;
        }
        if (auto st = new_rowset_writer->add_chunk(*new_chunk); !st.ok()) {
            LOG(WARNING) << "rowset writer add chunk failed: " << st;
            return false;
        }
    }

    if (auto st = new_rowset_writer->flush(); !st.ok()) {
        LOG(WARNING) << "failed to flush rowset writer: " << st;
        return false;
    }

    return true;
}

Status SchemaChangeDirectly::processV2(TabletReader* reader, RowsetWriter* new_rowset_writer,
                                       TabletSharedPtr new_tablet, TabletSharedPtr base_tablet,
                                       RowsetSharedPtr rowset) {
    Schema base_schema = std::move(ChunkHelper::convert_schema_to_format_v2(
            base_tablet->tablet_schema(), *_chunk_changer->get_mutable_selected_column_indexs()));
    ChunkPtr base_chunk = ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);
    Schema new_schema = std::move(ChunkHelper::convert_schema_to_format_v2(new_tablet->tablet_schema()));
    auto char_field_indexes = std::move(ChunkHelper::get_char_field_indexes(new_schema));

    ChunkPtr new_chunk = ChunkHelper::new_chunk(new_schema, config::vector_chunk_size);

    std::unique_ptr<MemPool> mem_pool(new MemPool());
    do {
        bool bg_worker_stopped = ExecEnv::GetInstance()->storage_engine()->bg_worker_stopped();
        if (bg_worker_stopped) {
            return Status::InternalError("bg_worker_stopped");
        }
#ifndef BE_TEST
        Status st = CurrentThread::mem_tracker()->check_mem_limit("DirectSchemaChange");
        if (!st.ok()) {
            LOG(WARNING) << "fail to execute schema change: " << st.message() << std::endl;
            return st;
        }
#endif
        Status status = reader->do_get_next(base_chunk.get());

        if (!status.ok()) {
            if (status.is_end_of_file()) {
                break;
            } else {
                LOG(WARNING) << "tablet reader failed to get next chunk, status: " << status.get_error_msg();
                return status;
            }
        }
        if (!_chunk_changer->change_chunkV2(base_chunk, new_chunk, base_schema, new_schema, mem_pool.get())) {
            std::string err_msg = Substitute("failed to convert chunk data. base tablet:$0, new tablet:$1",
                                             base_tablet->tablet_id(), new_tablet->tablet_id());
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }

        ChunkHelper::padding_char_columns(char_field_indexes, new_schema, new_tablet->tablet_schema(), new_chunk.get());

        if (auto tmp_st = new_rowset_writer->add_chunk(*new_chunk); !tmp_st.ok()) {
            std::string err_msg = Substitute(
                    "failed to execute schema change. base tablet:$0, new_tablet:$1. err msg: failed to add chunk to "
                    "rowset writer",
                    base_tablet->tablet_id(), new_tablet->tablet_id());
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }
        base_chunk->reset();
        new_chunk->reset();
        mem_pool->clear();
    } while (base_chunk->num_rows() == 0);

    if (base_chunk->num_rows() != 0) {
        if (!_chunk_changer->change_chunkV2(base_chunk, new_chunk, base_schema, new_schema, mem_pool.get())) {
            std::string err_msg = Substitute("failed to convert chunk data. base tablet:$0, new tablet:$1",
                                             base_tablet->tablet_id(), new_tablet->tablet_id());
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }
        if (auto st = new_rowset_writer->add_chunk(*new_chunk); !st.ok()) {
            LOG(WARNING) << "rowset writer add chunk failed: " << st;
            return st;
        }
    }

    if (auto st = new_rowset_writer->flush(); !st.ok()) {
        LOG(WARNING) << "failed to flush rowset writer: " << st;
        return st;
    }

    return Status::OK();
}

SchemaChangeWithSorting::SchemaChangeWithSorting(ChunkChanger* chunk_changer, size_t memory_limitation)
        : SchemaChange(), _chunk_changer(chunk_changer), _memory_limitation(memory_limitation) {}

SchemaChangeWithSorting::~SchemaChangeWithSorting() {
    SAFE_DELETE(_chunk_allocator);
}

bool SchemaChangeWithSorting::process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                                      TabletSharedPtr base_tablet, RowsetSharedPtr rowset) {
    if (_chunk_allocator == nullptr) {
        _chunk_allocator = new (std::nothrow) ChunkAllocator(new_tablet->tablet_schema(), _memory_limitation);
        if (_chunk_allocator == nullptr) {
            LOG(FATAL) << "failed to malloc chunk allocator. size=" << sizeof(ChunkAllocator);
            return false;
        }
    }
    std::vector<ChunkPtr> chunk_arr;
    Schema base_schema = ChunkHelper::convert_schema_to_format_v2(base_tablet->tablet_schema());
    Schema new_schema = ChunkHelper::convert_schema_to_format_v2(new_tablet->tablet_schema());

    ChunkSorter chunk_sorter(_chunk_allocator);
    std::unique_ptr<MemPool> mem_pool(new MemPool());

    StorageEngine* storage_engine = ExecEnv::GetInstance()->storage_engine();
    bool bg_worker_stopped = storage_engine->bg_worker_stopped();

    double total_bytes = 0;
    double row_count = 0;

    while (!bg_worker_stopped) {
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

        // Check if internal sorting needs to be performed
        // There are two places that may need to allocate memory
        //   1. We need to allocate a new chunk to save the data after convert
        //   2. We maybe need to allocate a new swap_chunk to save the sort result
        // So we should check that both of the above conditions are met
        _chunk_allocator->set_cur_mem_usage(CurrentThread::mem_tracker()->consumption());
        if (!_chunk_allocator->is_memory_enough_to_sort(base_chunk->num_rows() * 2)) {
            VLOG(3) << "do internal sorting because of memory limit";
            if (chunk_arr.empty()) {
                LOG(WARNING) << "Memory limitation is too small for Schema Change."
                             << "memory_limitation=" << _memory_limitation;
                return false;
            }

            if (!_internal_sorting(chunk_arr, new_rowset_writer, new_tablet)) {
                LOG(WARNING) << "failed to sorting internally.";
                return false;
            }

            chunk_arr.clear();
        }

        if (!_chunk_allocator->allocate(new_chunk, base_chunk->num_rows(), new_schema).ok()) {
            LOG(WARNING) << "failed to allocate chunk";
            return false;
        }

        if (!_chunk_changer->change_chunk(base_chunk, new_chunk, base_tablet->tablet_meta(), new_tablet->tablet_meta(),
                                          mem_pool.get())) {
            std::string err_msg = Substitute("failed to convert chunk data. base tablet:$0, new tablet:$1",
                                             base_tablet->tablet_id(), new_tablet->tablet_id());
            LOG(WARNING) << err_msg;
            return false;
        }

        total_bytes += static_cast<double>(new_chunk->memory_usage());
        row_count += static_cast<double>(new_chunk->num_rows());
        _chunk_allocator->set_row_len(std::max(static_cast<size_t>(total_bytes / row_count), static_cast<size_t>(1)));

        if (new_chunk->num_rows() > 0) {
            if (!chunk_sorter.sort(new_chunk, new_tablet)) {
                LOG(WARNING) << "chunk data sort failed";
                return false;
            }
        }

        chunk_arr.push_back(new_chunk);
        mem_pool->clear();
        bg_worker_stopped = storage_engine->bg_worker_stopped();
    }

    if (bg_worker_stopped) {
        return false;
    }

    if (!chunk_arr.empty()) {
        if (!_internal_sorting(chunk_arr, new_rowset_writer, new_tablet)) {
            LOG(WARNING) << "failed to sorting internally.";
            return false;
        }
    }

    if (auto st = new_rowset_writer->flush(); !st.ok()) {
        LOG(WARNING) << "failed to flush rowset writer: " << st;
        return false;
    }

    return true;
}

Status SchemaChangeWithSorting::processV2(TabletReader* reader, RowsetWriter* new_rowset_writer,
                                          TabletSharedPtr new_tablet, TabletSharedPtr base_tablet,
                                          RowsetSharedPtr rowset) {
    MemTableRowsetWriterSink mem_table_sink(new_rowset_writer);
    Schema base_schema = std::move(ChunkHelper::convert_schema_to_format_v2(
            base_tablet->tablet_schema(), *_chunk_changer->get_mutable_selected_column_indexs()));
    Schema new_schema = std::move(ChunkHelper::convert_schema_to_format_v2(new_tablet->tablet_schema()));
    auto char_field_indexes = std::move(ChunkHelper::get_char_field_indexes(new_schema));

    // memtable max buffer size set default 80% of memory limit so that it will do _merge() if reach limit
    // set max memtable size to 4G since some column has limit size, it will make invalid data
    size_t max_buffer_size = std::min<size_t>(
            4294967296, static_cast<size_t>(_memory_limitation * config::memory_ratio_for_sorting_schema_change));
    auto mem_table = std::make_unique<MemTable>(new_tablet->tablet_id(), &new_schema, &mem_table_sink, max_buffer_size,
                                                CurrentThread::mem_tracker());

    auto selective = std::make_unique<std::vector<uint32_t>>();
    selective->resize(config::vector_chunk_size);
    for (uint32_t i = 0; i < config::vector_chunk_size; i++) {
        (*selective)[i] = i;
    }

    std::unique_ptr<MemPool> mem_pool(new MemPool());

    StorageEngine* storage_engine = ExecEnv::GetInstance()->storage_engine();
    bool bg_worker_stopped = storage_engine->bg_worker_stopped();
    while (!bg_worker_stopped) {
#ifndef BE_TEST
        auto cur_usage = CurrentThread::mem_tracker()->consumption();
        // we check memory usage exceeds 90% since tablet reader use some memory
        // it will return fail if memory is exhausted
        if (cur_usage > CurrentThread::mem_tracker()->limit() * 0.9) {
            RETURN_IF_ERROR_WITH_WARN(mem_table->finalize(), "failed to finalize mem table");
            RETURN_IF_ERROR_WITH_WARN(mem_table->flush(), "failed to flush mem table");
            mem_table = std::make_unique<MemTable>(new_tablet->tablet_id(), &new_schema, &mem_table_sink,
                                                   max_buffer_size, CurrentThread::mem_tracker());
            VLOG(1) << "SortSchemaChange memory usage: " << cur_usage << " after mem table flush "
                    << CurrentThread::mem_tracker()->consumption();
        }
#endif
        ChunkPtr base_chunk = ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);
        Status status = reader->do_get_next(base_chunk.get());
        if (!status.ok()) {
            if (!status.is_end_of_file()) {
                LOG(WARNING) << "failed to get next chunk, status is:" << status.to_string();
                return status;
            } else if (base_chunk->num_rows() <= 0) {
                break;
            }
        }

        ChunkPtr new_chunk = ChunkHelper::new_chunk(new_schema, base_chunk->num_rows());

        if (!_chunk_changer->change_chunkV2(base_chunk, new_chunk, base_schema, new_schema, mem_pool.get())) {
            std::string err_msg = Substitute("failed to convert chunk data. base tablet:$0, new tablet:$1",
                                             base_tablet->tablet_id(), new_tablet->tablet_id());
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }

        ChunkHelper::padding_char_columns(char_field_indexes, new_schema, new_tablet->tablet_schema(), new_chunk.get());

        bool full = mem_table->insert(*new_chunk, selective->data(), 0, new_chunk->num_rows());
        if (full) {
            RETURN_IF_ERROR_WITH_WARN(mem_table->finalize(), "failed to finalize mem table");
            RETURN_IF_ERROR_WITH_WARN(mem_table->flush(), "failed to flush mem table");
            mem_table = std::make_unique<MemTable>(new_tablet->tablet_id(), &new_schema, &mem_table_sink,
                                                   max_buffer_size, CurrentThread::mem_tracker());
        }

        mem_pool->clear();
        bg_worker_stopped = storage_engine->bg_worker_stopped();
    }

    RETURN_IF_ERROR_WITH_WARN(mem_table->finalize(), "failed to finalize mem table");
    RETURN_IF_ERROR_WITH_WARN(mem_table->flush(), "failed to flush mem table");

    if (bg_worker_stopped) {
        return Status::InternalError("bg_worker_stopped");
    }

    if (auto st = new_rowset_writer->flush(); !st.ok()) {
        LOG(WARNING) << "failed to flush rowset writer: " << st;
        return st;
    }

    return Status::OK();
}

bool SchemaChangeWithSorting::_internal_sorting(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* new_rowset_writer,
                                                TabletSharedPtr tablet) {
    if (chunk_arr.size() == 1) {
        if (auto st = new_rowset_writer->add_chunk(*chunk_arr[0]); !st.ok()) {
            LOG(WARNING) << "failed to add chunk: " << st;
            return false;
        }
        if (auto st = new_rowset_writer->flush(); !st.ok()) {
            LOG(WARNING) << "failed to finalizing writer: " << st;
            return false;
        } else {
            return true;
        }
    }

    ChunkMerger merger(std::move(tablet));
    if (!merger.merge(chunk_arr, new_rowset_writer)) {
        LOG(WARNING) << "merge chunk arr failed";
        return false;
    }

    return true;
}

Status SchemaChangeHandler::process_alter_tablet_v2(const TAlterTabletReqV2& request) {
    LOG(INFO) << "begin to do request alter tablet: base_tablet_id=" << request.base_tablet_id
              << ", base_schema_hash=" << request.base_schema_hash << ", new_tablet_id=" << request.new_tablet_id
              << ", new_schema_hash=" << request.new_schema_hash << ", alter_version=" << request.alter_version;

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
              << " duration: " << timer.elapsed_time() / 1000000
              << "ms, peak_mem_usage: " << CurrentThread::mem_tracker()->peak_consumption() << " bytes";
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
                  << " the convert job already finished, check its version"
                  << " res=" << st.to_string();
        return st;
    }

    LOG(INFO) << "finish to validate alter tablet request. begin to convert data from base tablet to new tablet"
              << " base_tablet=" << base_tablet->full_name() << " new_tablet=" << new_tablet->full_name();

    std::shared_lock base_migration_rlock(base_tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::InternalError("base tablet get migration r_lock failed");
    }
    if (Tablet::check_migrate(base_tablet)) {
        return Status::InternalError(Substitute("tablet $0 is doing disk balance", base_tablet->table_id()));
    }
    std::shared_lock new_migration_rlock(new_tablet->get_migration_lock(), std::try_to_lock);
    if (!new_migration_rlock.owns_lock()) {
        return Status::InternalError("new tablet get migration r_lock failed");
    }
    if (Tablet::check_migrate(new_tablet)) {
        return Status::InternalError(Substitute("tablet $0 is doing disk balance", new_tablet->table_id()));
    }

    SchemaChangeParams sc_params;
    sc_params.base_tablet = base_tablet;
    sc_params.new_tablet = new_tablet;
    sc_params.chunk_changer = std::make_unique<ChunkChanger>(sc_params.new_tablet->tablet_schema());

    // primary key do not support materialized view, initialize materialized_params_map here,
    // just for later column_mapping of _parse_request.
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

    Status status = _parse_request(sc_params.base_tablet, sc_params.new_tablet, sc_params.chunk_changer.get(),
                                   &sc_params.sc_sorting, &sc_params.sc_directly, sc_params.materialized_params_map);
    if (!status.ok()) {
        LOG(WARNING) << "failed to parse the request. res=" << status.get_error_msg();
        return status;
    }

    if (base_tablet->keys_type() == KeysType::PRIMARY_KEYS) {
        if (sc_params.sc_directly) {
            status = new_tablet->updates()->convert_from(base_tablet, request.alter_version,
                                                         sc_params.chunk_changer.get());
        } else if (sc_params.sc_sorting) {
            LOG(WARNING) << "schema change of primary key model do not support sorting.";
            status = Status::NotSupported("schema change of primary key model do not support sorting.");
        } else {
            status = new_tablet->updates()->link_from(base_tablet.get(), request.alter_version);
        }
        if (!status.ok()) {
            LOG(WARNING) << "schema change new tablet load snapshot error: " << status.to_string();
            return status;
        }
        return Status::OK();
    } else {
        return _do_process_alter_tablet_v2_normal(request, sc_params, base_tablet, new_tablet);
    }
}

Status SchemaChangeHandler::_do_process_alter_tablet_v2_normal(const TAlterTabletReqV2& request,
                                                               SchemaChangeParams& sc_params,
                                                               const TabletSharedPtr& base_tablet,
                                                               const TabletSharedPtr& new_tablet) {
    // begin to find deltas to convert from base tablet to new tablet so that
    // obtain base tablet and new tablet's push lock and header write lock to prevent loading data
    RowsetSharedPtr max_rowset;
    std::vector<RowsetSharedPtr> rowsets_to_change;
    int32_t end_version = -1;
    Status status;
    std::vector<std::unique_ptr<TabletReader>> readers;
    {
        std::lock_guard l1(base_tablet->get_push_lock());
        std::lock_guard l2(new_tablet->get_push_lock());
        std::shared_lock l3(base_tablet->get_header_lock());
        std::lock_guard l4(new_tablet->get_header_lock());

        std::vector<Version> versions_to_be_changed;
        status = _get_versions_to_be_changed(base_tablet, &versions_to_be_changed);
        if (!status.ok()) {
            LOG(WARNING) << "fail to get version to be changed. status: " << status;
            return status;
        }
        VLOG(3) << "versions to be changed size:" << versions_to_be_changed.size();

        Schema base_schema;
        if (config::enable_schema_change_v2) {
            base_schema = std::move(ChunkHelper::convert_schema_to_format_v2(
                    base_tablet->tablet_schema(), *sc_params.chunk_changer->get_mutable_selected_column_indexs()));
        } else {
            base_schema = std::move(ChunkHelper::convert_schema_to_format_v2(base_tablet->tablet_schema()));
        }

        for (auto& version : versions_to_be_changed) {
            rowsets_to_change.push_back(base_tablet->get_rowset_by_version(version));
            // prepare tablet reader to prevent rowsets being compacted
            std::unique_ptr<TabletReader> tablet_reader =
                    std::make_unique<TabletReader>(base_tablet, version, base_schema);
            RETURN_IF_ERROR(tablet_reader->prepare());
            readers.emplace_back(std::move(tablet_reader));
        }
        VLOG(3) << "rowsets_to_change size is:" << rowsets_to_change.size();

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
                rowsets_to_delete.push_back(new_tablet->get_rowset_by_version(version));
            }
        }
        VLOG(3) << "rowsets_to_delete size is:" << rowsets_to_delete.size()
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

    Version delete_predicates_version(0, max_rowset->version().second);
    TabletReaderParams read_params;
    read_params.reader_type = ReaderType::READER_ALTER_TABLE;
    read_params.skip_aggregation = false;
    read_params.chunk_size = config::vector_chunk_size;

    // open tablet readers out of lock for open is heavy because of io
    for (auto& tablet_reader : readers) {
        tablet_reader->set_delete_predicates_version(delete_predicates_version);
        RETURN_IF_ERROR(tablet_reader->open(read_params));
    }

    sc_params.rowset_readers = std::move(readers);
    sc_params.version = Version(0, end_version);
    sc_params.rowsets_to_change = rowsets_to_change;

    status = _convert_historical_rowsets(sc_params);
    if (!status.ok()) {
        return status;
    }

    Status res = Status::OK();
    {
        // set state to ready
        std::unique_lock new_wlock(new_tablet->get_header_lock());
        res = new_tablet->set_tablet_state(TabletState::TABLET_RUNNING);
        if (!res.ok()) {
            LOG(WARNING) << "failed to alter tablet. base_tablet=" << base_tablet->full_name()
                         << ", drop new_tablet=" << new_tablet->full_name();
            // do not drop the new tablet and its data. GC thread will
            return res;
        }
        new_tablet->save_meta();
    }

    // _validate_alter_result should be outside the above while loop.
    // to avoid requiring the header lock twice.
    status = _validate_alter_result(new_tablet, request);
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
    versions_to_be_changed->insert(std::end(*versions_to_be_changed), std::begin(span_versions),
                                   std::end(span_versions));
    return Status::OK();
}

Status SchemaChangeHandler::_convert_historical_rowsets(SchemaChangeParams& sc_params) {
    LOG(INFO) << "begin to convert historical rowsets for new_tablet from base_tablet."
              << " base_tablet=" << sc_params.base_tablet->full_name()
              << ", new_tablet=" << sc_params.new_tablet->full_name();
    DeferOp save_meta([&sc_params] {
        std::unique_lock new_wlock(sc_params.new_tablet->get_header_lock());
        sc_params.new_tablet->save_meta();
    });

    std::unique_ptr<SchemaChange> sc_procedure;
    auto chunk_changer = sc_params.chunk_changer.get();
    if (sc_params.sc_sorting) {
        LOG(INFO) << "doing schema change with sorting for base_tablet " << sc_params.base_tablet->full_name();
        size_t memory_limitation =
                static_cast<size_t>(config::memory_limitation_per_thread_for_schema_change) * 1024 * 1024 * 1024;
        sc_procedure = std::make_unique<SchemaChangeWithSorting>(chunk_changer, memory_limitation);
    } else if (sc_params.sc_directly) {
        LOG(INFO) << "doing directly schema change for base_tablet " << sc_params.base_tablet->full_name();
        sc_procedure = std::make_unique<SchemaChangeDirectly>(chunk_changer);
    } else {
        LOG(INFO) << "doing linked schema change for base_tablet " << sc_params.base_tablet->full_name();
        sc_procedure = std::make_unique<LinkedSchemaChange>(chunk_changer);
    }

    if (sc_procedure == nullptr) {
        LOG(WARNING) << "failed to malloc SchemaChange. "
                     << "malloc_size=" << sizeof(SchemaChangeWithSorting);
        return Status::InternalError("failed to malloc SchemaChange");
    }

    Status status;

    for (int i = 0; i < sc_params.rowset_readers.size(); ++i) {
        VLOG(3) << "begin to convert a history rowset. version=" << sc_params.rowsets_to_change[i]->version();

        TabletSharedPtr new_tablet = sc_params.new_tablet;
        TabletSharedPtr base_tablet = sc_params.base_tablet;
        RowsetWriterContext writer_context(kDataFormatV2, config::storage_format_version);
        writer_context.rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.tablet_uid = new_tablet->tablet_uid();
        writer_context.tablet_id = new_tablet->tablet_id();
        writer_context.partition_id = new_tablet->partition_id();
        writer_context.tablet_schema_hash = new_tablet->schema_hash();
        writer_context.rowset_type = sc_params.new_tablet->tablet_meta()->preferred_rowset_type();
        writer_context.rowset_path_prefix = new_tablet->schema_hash_path();
        writer_context.tablet_schema = &new_tablet->tablet_schema();
        writer_context.rowset_state = VISIBLE;
        writer_context.version = sc_params.rowsets_to_change[i]->version();
        writer_context.segments_overlap = sc_params.rowsets_to_change[i]->rowset_meta()->segments_overlap();

        if (sc_params.sc_sorting) {
            writer_context.write_tmp = true;
        }

        std::unique_ptr<RowsetWriter> rowset_writer;
        status = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        if (!status.ok()) {
            return Status::InternalError("build rowset writer failed");
        }

        if (config::enable_schema_change_v2) {
            auto st = sc_procedure->processV2(sc_params.rowset_readers[i].get(), rowset_writer.get(), new_tablet,
                                              base_tablet, sc_params.rowsets_to_change[i]);
            if (!st.ok()) {
                LOG(WARNING) << "failed to process the schema change. from tablet "
                             << base_tablet->get_tablet_info().to_string() << " to tablet "
                             << new_tablet->get_tablet_info().to_string() << " version=" << sc_params.version.first
                             << "-" << sc_params.version.second << " error " << st;
                return st;
            }
        } else {
            if (!sc_procedure->process(sc_params.rowset_readers[i].get(), rowset_writer.get(), new_tablet, base_tablet,
                                       sc_params.rowsets_to_change[i])) {
                LOG(WARNING) << "failed to process the version."
                             << " version=" << sc_params.version.first << "-" << sc_params.version.second;
                return Status::InternalError("process failed");
            }
        }
        sc_params.rowset_readers[i]->close();
        auto new_rowset = rowset_writer->build();
        if (!new_rowset.ok()) {
            LOG(WARNING) << "failed to build rowset: " << new_rowset.status() << ". exit alter process";
            break;
        }
        LOG(INFO) << "new rowset has " << (*new_rowset)->num_segments() << " segments";
        status = sc_params.new_tablet->add_rowset(*new_rowset, false);
        if (status.is_already_exist()) {
            LOG(WARNING) << "version already exist, version revert occurred. "
                         << "tablet=" << sc_params.new_tablet->full_name() << ", version='" << sc_params.version.first
                         << "-" << sc_params.version.second;
            StorageEngine::instance()->add_unused_rowset(*new_rowset);
            status = Status::OK();
        } else if (!status.ok()) {
            LOG(WARNING) << "failed to register new version. "
                         << " tablet=" << sc_params.new_tablet->full_name() << ", version=" << sc_params.version.first
                         << "-" << sc_params.version.second;
            StorageEngine::instance()->add_unused_rowset(*new_rowset);
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

Status SchemaChangeHandler::_parse_request(
        const std::shared_ptr<Tablet>& base_tablet, const std::shared_ptr<Tablet>& new_tablet,
        ChunkChanger* chunk_changer, bool* sc_sorting, bool* sc_directly,
        const std::unordered_map<std::string, AlterMaterializedViewParam>& materialized_function_map) {
    std::map<ColumnId, ColumnId> base_to_new;
    for (int i = 0; i < new_tablet->tablet_schema().num_columns(); ++i) {
        const TabletColumn& new_column = new_tablet->tablet_schema().column(i);
        std::string column_name(new_column.name());
        ColumnMapping* column_mapping = chunk_changer->get_mutable_column_mapping(i);

        if (materialized_function_map.find(column_name) != materialized_function_map.end()) {
            AlterMaterializedViewParam mvParam = materialized_function_map.find(column_name)->second;
            column_mapping->materialized_function = mvParam.mv_expr;
            int32_t column_index = base_tablet->field_index(mvParam.origin_column_name);
            if (column_index >= 0) {
                column_mapping->ref_column = column_index;
                base_to_new[column_index] = i;
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
            base_to_new[column_index] = i;
            continue;
        }

        // to handle new added column
        {
            column_mapping->ref_column = -1;

            if (i < base_tablet->num_short_key_columns()) {
                *sc_directly = true;
            }

            if (!_init_column_mapping(column_mapping, new_column, new_column.default_value()).ok()) {
                return Status::InternalError("init column mapping failed");
            }

            VLOG(3) << "A column with default value will be added after schema changing. "
                    << "column=" << column_name << ", default_value=" << new_column.default_value();
            continue;
        }
    }

    // base tablet schema: k1 k2 k3 v1 v2
    // new tablet schema: k3 k1 v2
    // base reader schema: k1 k3 v2
    // selected_column_index: 0 2 4
    // ref_column: 2 0 4
    // ref_base_reader_column_index: 1 0 2
    auto selected_column_indexs = chunk_changer->get_mutable_selected_column_indexs();
    int32_t index = 0;
    for (const auto& iter : base_to_new) {
        ColumnMapping* column_mapping = chunk_changer->get_mutable_column_mapping(iter.second);
        // new tablet column index -> base reader column index
        column_mapping->ref_base_reader_column_index = index++;
        // selected column index from base tablet for base reader
        selected_column_indexs->emplace_back(iter.first);
    }

    // Check if re-aggregation is needed.
    *sc_sorting = false;
    // If the reference sequence of the Key column is out of order, it needs to be reordered
    int num_default_value = 0;

    for (int i = 0; i < new_tablet->num_key_columns(); ++i) {
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

    if (!base_tablet->delete_predicates().empty()) {
        // there exists delete condition in header, can't do linked schema change
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
    int64_t max_continuous_version = new_tablet->max_continuous_version();
    LOG(INFO) << "find max continuous version of tablet=" << new_tablet->full_name()
              << ", version=" << max_continuous_version;
    if (max_continuous_version >= request.alter_version) {
        return Status::OK();
    } else {
        return Status::InternalError("version missed");
    }
}

} // namespace starrocks::vectorized
