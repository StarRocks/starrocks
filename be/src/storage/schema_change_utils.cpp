// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/schema_change_utils.h"

#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "types/bitmap_value.h"
#include "types/hll.h"
#include "util/percentile_value.h"

namespace starrocks {

ChunkChanger::ChunkChanger(const TabletSchema& tablet_schema) {
    _schema_mapping.resize(tablet_schema.num_columns());
}

ChunkChanger::~ChunkChanger() {
    _schema_mapping.clear();
    for (auto it : _mc_exprs) {
        if (it.second != nullptr) {
            it.second->close(_state);
        }
    }
}

void ChunkChanger::init_runtime_state(TQueryOptions query_options, TQueryGlobals query_globals) {
    _state = _obj_pool.add(
            new RuntimeState(TUniqueId(), TUniqueId(), query_options, query_globals, ExecEnv::GetInstance()));
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
        case TYPE_TINYINT:                                                          \
            TYPE_REINTERPRET_CAST(from_type, int8_t);                               \
        case TYPE_UNSIGNED_TINYINT:                                                 \
            TYPE_REINTERPRET_CAST(from_type, uint8_t);                              \
        case TYPE_SMALLINT:                                                         \
            TYPE_REINTERPRET_CAST(from_type, int16_t);                              \
        case TYPE_UNSIGNED_SMALLINT:                                                \
            TYPE_REINTERPRET_CAST(from_type, uint16_t);                             \
        case TYPE_INT:                                                              \
            TYPE_REINTERPRET_CAST(from_type, int32_t);                              \
        case TYPE_UNSIGNED_INT:                                                     \
            TYPE_REINTERPRET_CAST(from_type, uint32_t);                             \
        case TYPE_BIGINT:                                                           \
            TYPE_REINTERPRET_CAST(from_type, int64_t);                              \
        case TYPE_UNSIGNED_BIGINT:                                                  \
            TYPE_REINTERPRET_CAST(from_type, uint64_t);                             \
        case TYPE_LARGEINT:                                                         \
            TYPE_REINTERPRET_CAST(from_type, int128_t);                             \
        case TYPE_DOUBLE:                                                           \
            TYPE_REINTERPRET_CAST(from_type, double);                               \
        default:                                                                    \
            LOG(WARNING) << "the column type which was altered to was unsupported." \
                         << " origin_type=" << logical_type_to_string(ref_type)     \
                         << ", alter_type=" << logical_type_to_string(new_type);    \
            return false;                                                           \
        }                                                                           \
        break;                                                                      \
    }

#define COLUMN_APPEND_DATUM()                                                     \
    for (size_t row_index = 0; row_index < base_chunk->num_rows(); ++row_index) { \
        new_col->append_datum(dst_datum);                                         \
    }

struct ConvertTypeMapHash {
    size_t operator()(const std::pair<LogicalType, LogicalType>& pair) const { return (pair.first + 31) ^ pair.second; }
};

class ConvertTypeResolver {
    DECLARE_SINGLETON(ConvertTypeResolver);

public:
    bool convert_type_exist(const LogicalType from_type, const LogicalType to_type) const {
        return _convert_type_set.find(std::make_pair(from_type, to_type)) != _convert_type_set.end();
    }

    template <LogicalType from_type, LogicalType to_type>
    void add_convert_type_mapping() {
        _convert_type_set.emplace(std::make_pair(from_type, to_type));
    }

private:
    typedef std::pair<LogicalType, LogicalType> convert_type_pair;
    std::unordered_set<convert_type_pair, ConvertTypeMapHash> _convert_type_set;

    DISALLOW_COPY(ConvertTypeResolver);
};

ConvertTypeResolver::ConvertTypeResolver() {
    // from varchar type
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_TINYINT>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_SMALLINT>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_INT>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_BIGINT>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_LARGEINT>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_FLOAT>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_DOUBLE>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_DATE_V1>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_DATE>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_DECIMAL32>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_DECIMAL64>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_DECIMAL128>();
    add_convert_type_mapping<TYPE_VARCHAR, TYPE_JSON>();

    // to varchar type
    add_convert_type_mapping<TYPE_TINYINT, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_SMALLINT, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_INT, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_BIGINT, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_LARGEINT, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_FLOAT, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_DOUBLE, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_DECIMAL, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_DECIMALV2, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_DECIMAL32, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_DECIMAL64, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_DECIMAL128, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_CHAR, TYPE_VARCHAR>();
    add_convert_type_mapping<TYPE_JSON, TYPE_VARCHAR>();

    add_convert_type_mapping<TYPE_DATE_V1, TYPE_DATETIME_V1>();
    add_convert_type_mapping<TYPE_DATE_V1, TYPE_DATETIME>();
    add_convert_type_mapping<TYPE_DATE, TYPE_DATETIME_V1>();
    add_convert_type_mapping<TYPE_DATE, TYPE_DATETIME>();

    add_convert_type_mapping<TYPE_DATETIME_V1, TYPE_DATE_V1>();
    add_convert_type_mapping<TYPE_DATETIME_V1, TYPE_DATE>();
    add_convert_type_mapping<TYPE_DATETIME, TYPE_DATE_V1>();
    add_convert_type_mapping<TYPE_DATETIME, TYPE_DATE>();

    add_convert_type_mapping<TYPE_FLOAT, TYPE_DOUBLE>();

    add_convert_type_mapping<TYPE_INT, TYPE_DATE_V1>();
    add_convert_type_mapping<TYPE_INT, TYPE_DATE>();

    add_convert_type_mapping<TYPE_DATE_V1, TYPE_DATE>();
    add_convert_type_mapping<TYPE_DATE, TYPE_DATE_V1>();
    add_convert_type_mapping<TYPE_DATETIME_V1, TYPE_DATETIME>();
    add_convert_type_mapping<TYPE_DATETIME, TYPE_DATETIME_V1>();
    add_convert_type_mapping<TYPE_DECIMAL, TYPE_DECIMALV2>();
    add_convert_type_mapping<TYPE_DECIMALV2, TYPE_DECIMAL>();
    add_convert_type_mapping<TYPE_DECIMAL, TYPE_DECIMAL128>();
    add_convert_type_mapping<TYPE_DECIMALV2, TYPE_DECIMAL128>();

    add_convert_type_mapping<TYPE_DECIMAL32, TYPE_DECIMAL32>();
    add_convert_type_mapping<TYPE_DECIMAL32, TYPE_DECIMAL64>();
    add_convert_type_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128>();

    add_convert_type_mapping<TYPE_DECIMAL64, TYPE_DECIMAL32>();
    add_convert_type_mapping<TYPE_DECIMAL64, TYPE_DECIMAL64>();
    add_convert_type_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128>();

    add_convert_type_mapping<TYPE_DECIMAL128, TYPE_DECIMAL32>();
    add_convert_type_mapping<TYPE_DECIMAL128, TYPE_DECIMAL64>();
    add_convert_type_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>();
}

ConvertTypeResolver::~ConvertTypeResolver() = default;

bool ChunkChanger::change_chunk(ChunkPtr& base_chunk, ChunkPtr& new_chunk, const TabletMetaSharedPtr& base_tablet_meta,
                                const TabletMetaSharedPtr& new_tablet_meta, MemPool* mem_pool) {
    if (new_chunk->num_columns() != _schema_mapping.size()) {
        LOG(WARNING) << "new chunk does not match with schema mapping rules. "
                     << "base_tablet_id=" << base_tablet_meta->tablet_id()
                     << ", new_tablet_id=" << new_tablet_meta->tablet_id()
                     << ", chunk_schema_size=" << new_chunk->num_columns()
                     << ", mapping_schema_size=" << _schema_mapping.size();
        return false;
    }
    if (_has_mv_expr_context) {
        if (base_chunk->num_columns() != _slot_id_to_index_map.size()) {
            LOG(WARNING) << "base chunk does not match with _slot_id_to_index_map mapping rules. "
                         << "base_tablet_id=" << base_tablet_meta->tablet_id()
                         << ", new_tablet_id=" << new_tablet_meta->tablet_id()
                         << ", base_chunk_size=" << base_chunk->num_columns()
                         << ", slot_id_to_index_map's size=" << _slot_id_to_index_map.size();
            return false;
        }
        // init for expression evaluation only
        for (auto& iter : _slot_id_to_index_map) {
            base_chunk->set_slot_id_to_index(iter.first, iter.second);
        }
    }

    for (size_t i = 0; i < new_chunk->num_columns(); ++i) {
        int ref_column = _schema_mapping[i].ref_column;
        if (ref_column >= 0) {
            if (_schema_mapping[i].mv_expr_ctx != nullptr) {
                auto new_col_status = (_schema_mapping[i].mv_expr_ctx)->evaluate(base_chunk.get());
                if (!new_col_status.ok()) {
                    return false;
                }
                auto new_col = new_col_status.value();
                auto new_schema = new_chunk->schema();
                if (!new_schema->field(i)->is_nullable() && new_col->is_nullable()) {
                    LOG(WARNING) << "schema of column(" << new_schema->field(i)->name()
                                 << ") is not null but data contains null";
                    return false;
                }
                // NOTE: Unpack const column first to avoid generating NullColumn<ConstColumn> result.
                new_col = ColumnHelper::unpack_and_duplicate_const_column(new_col->size(), new_col);
                if (new_schema->field(i)->is_nullable()) {
                    new_col = ColumnHelper::cast_to_nullable_column(new_col);
                }
                new_chunk->columns()[i] = std::move(new_col);
            } else {
                LogicalType ref_type = base_tablet_meta->tablet_schema().column(ref_column).type();
                LogicalType new_type = new_tablet_meta->tablet_schema().column(i).type();

                int reftype_precision = base_tablet_meta->tablet_schema().column(ref_column).precision();
                int reftype_scale = base_tablet_meta->tablet_schema().column(ref_column).scale();
                int newtype_precision = new_tablet_meta->tablet_schema().column(i).precision();
                int newtype_scale = new_tablet_meta->tablet_schema().column(i).scale();

                ColumnPtr& base_col = base_chunk->get_column_by_index(ref_column);
                ColumnPtr& new_col = new_chunk->get_column_by_index(i);

                if (new_type == ref_type &&
                    (!is_decimalv3_field_type(new_type) ||
                     (reftype_precision == newtype_precision && reftype_scale == newtype_scale))) {
                    if (new_type == TYPE_CHAR) {
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
                        LOG(WARNING) << "failed to get type converter, from_type=" << ref_type << ", to_type"
                                     << new_type << ", base_tablet_id=" << base_tablet_meta->tablet_id()
                                     << ", new_tablet_id=" << new_tablet_meta->tablet_id();
                        return false;
                    }

                    Field ref_field = ChunkHelper::convert_field(ref_column,
                                                                 base_tablet_meta->tablet_schema().column(ref_column));
                    Field new_field = ChunkHelper::convert_field(i, new_tablet_meta->tablet_schema().column(i));

                    Status st = converter->convert_column(ref_field.type().get(), *base_col, new_field.type().get(),
                                                          new_col.get(), mem_pool);
                    if (!st.ok()) {
                        LOG(WARNING) << "failed to convert " << logical_type_to_string(ref_type) << " to "
                                     << logical_type_to_string(new_type)
                                     << ", base_tablet_id=" << base_tablet_meta->tablet_id()
                                     << ", new_tablet_id=" << new_tablet_meta->tablet_id();
                        return false;
                    }
                } else {
                    // copy and alter the field
                    switch (ref_type) {
                    case TYPE_TINYINT:
                        CONVERT_FROM_TYPE(int8_t);
                    case TYPE_UNSIGNED_TINYINT:
                        CONVERT_FROM_TYPE(uint8_t);
                    case TYPE_SMALLINT:
                        CONVERT_FROM_TYPE(int16_t);
                    case TYPE_UNSIGNED_SMALLINT:
                        CONVERT_FROM_TYPE(uint16_t);
                    case TYPE_INT:
                        CONVERT_FROM_TYPE(int32_t);
                    case TYPE_UNSIGNED_INT:
                        CONVERT_FROM_TYPE(uint32_t);
                    case TYPE_BIGINT:
                        CONVERT_FROM_TYPE(int64_t);
                    case TYPE_UNSIGNED_BIGINT:
                        CONVERT_FROM_TYPE(uint64_t);
                    default:
                        LOG(WARNING) << "the column type which was altered from was unsupported."
                                     << " from_type=" << ref_type << ", to_type=" << new_type
                                     << ", base_tablet_id=" << base_tablet_meta->tablet_id()
                                     << ", new_tablet_id=" << new_tablet_meta->tablet_id();
                        ;
                        return false;
                    }
                    if (new_type < ref_type) {
                        LOG(INFO) << "type degraded while altering column. "
                                  << "column=" << new_tablet_meta->tablet_schema().column(i).name()
                                  << ", origin_type=" << logical_type_to_string(ref_type)
                                  << ", alter_type=" << logical_type_to_string(new_type)
                                  << ", base_tablet_id=" << base_tablet_meta->tablet_id()
                                  << ", new_tablet_id=" << new_tablet_meta->tablet_id();
                        ;
                    }
                }
            }
        } else {
            ColumnPtr& new_col = new_chunk->get_column_by_index(i);
            for (size_t row_index = 0; row_index < base_chunk->num_rows(); ++row_index) {
                new_col->append_datum(_schema_mapping[i].default_value_datum);
            }
        }
    }
    return true;
}

bool ChunkChanger::change_chunk_v2(ChunkPtr& base_chunk, ChunkPtr& new_chunk, const Schema& base_schema,
                                   const Schema& new_schema, MemPool* mem_pool) {
    if (new_chunk->num_columns() != _schema_mapping.size()) {
        LOG(WARNING) << "new chunk does not match with schema mapping rules. "
                     << "chunk_schema_size=" << new_chunk->num_columns()
                     << ", mapping_schema_size=" << _schema_mapping.size();
        return false;
    }
    if (_has_mv_expr_context) {
        if (base_chunk->num_columns() != _slot_id_to_index_map.size()) {
            LOG(WARNING) << "base chunk does not match with _slot_id_to_index_map mapping rules. "
                         << "base_chunk_size=" << base_chunk->num_columns()
                         << ", slot_id_to_index_map's size=" << _slot_id_to_index_map.size();
            return false;
        }
        // init for expression evaluation only
        for (auto& iter : _slot_id_to_index_map) {
            base_chunk->set_slot_id_to_index(iter.first, iter.second);
        }
    }

    for (size_t i = 0; i < new_chunk->num_columns(); ++i) {
        int ref_column = _schema_mapping[i].ref_column;
        if (ref_column >= 0) {
            if (_schema_mapping[i].mv_expr_ctx != nullptr) {
                auto new_col_status = (_schema_mapping[i].mv_expr_ctx)->evaluate(base_chunk.get());
                if (!new_col_status.ok()) {
                    return false;
                }
                auto new_col = new_col_status.value();
                if (!new_schema.field(i)->is_nullable() && new_col->is_nullable()) {
                    LOG(WARNING) << "schema of column(" << new_schema.field(i)->name()
                                 << ") is not null but data contains null";
                    return false;
                }
                // NOTE: Unpack const column first to avoid generating NullColumn<ConstColumn> result.
                new_col = ColumnHelper::unpack_and_duplicate_const_column(new_col->size(), new_col);
                if (new_schema.field(i)->is_nullable()) {
                    new_col = ColumnHelper::cast_to_nullable_column(new_col);
                }
                new_chunk->columns()[i] = std::move(new_col);
            } else {
                DCHECK(_slot_id_to_index_map.find(ref_column) != _slot_id_to_index_map.end());
                int base_index = _slot_id_to_index_map[ref_column];
                const TypeInfoPtr& ref_type_info = base_schema.field(base_index)->type();
                const TypeInfoPtr& new_type_info = new_schema.field(i)->type();

                int reftype_precision = ref_type_info->precision();
                int reftype_scale = ref_type_info->scale();
                int newtype_precision = new_type_info->precision();
                int newtype_scale = new_type_info->scale();
                auto ref_type = ref_type_info->type();
                auto new_type = new_type_info->type();

                ColumnPtr& base_col = base_chunk->get_column_by_index(base_index);
                ColumnPtr& new_col = new_chunk->get_column_by_index(i);
                if (new_type == ref_type &&
                    (!is_decimalv3_field_type(new_type) ||
                     (reftype_precision == newtype_precision && reftype_scale == newtype_scale))) {
                    if (new_col->is_nullable() != base_col->is_nullable()) {
                        new_col->append(*base_col.get());
                    } else {
                        new_col = base_col;
                    }
                } else if (ConvertTypeResolver::instance()->convert_type_exist(ref_type, new_type)) {
                    auto converter = get_type_converter(ref_type, new_type);
                    if (converter == nullptr) {
                        LOG(WARNING) << "failed to get type converter, from_type=" << ref_type << ", to_type"
                                     << new_type;
                        return false;
                    }
                    Status st = converter->convert_column(ref_type_info.get(), *base_col, new_type_info.get(),
                                                          new_col.get(), mem_pool);
                    if (!st.ok()) {
                        LOG(WARNING) << "failed to convert " << logical_type_to_string(ref_type) << " to "
                                     << logical_type_to_string(new_type);
                        return false;
                    }
                } else {
                    // copy and alter the field
                    switch (ref_type) {
                    case TYPE_TINYINT:
                        CONVERT_FROM_TYPE(int8_t);
                    case TYPE_UNSIGNED_TINYINT:
                        CONVERT_FROM_TYPE(uint8_t);
                    case TYPE_SMALLINT:
                        CONVERT_FROM_TYPE(int16_t);
                    case TYPE_UNSIGNED_SMALLINT:
                        CONVERT_FROM_TYPE(uint16_t);
                    case TYPE_INT:
                        CONVERT_FROM_TYPE(int32_t);
                    case TYPE_UNSIGNED_INT:
                        CONVERT_FROM_TYPE(uint32_t);
                    case TYPE_BIGINT:
                        CONVERT_FROM_TYPE(int64_t);
                    case TYPE_UNSIGNED_BIGINT:
                        CONVERT_FROM_TYPE(uint64_t);
                    default:
                        LOG(WARNING) << "the column type which was altered from was unsupported."
                                     << " from_type=" << ref_type << ", to_type=" << new_type;
                        return false;
                    }
                    if (new_type < ref_type) {
                        LOG(INFO) << "type degraded while altering column. "
                                  << "column=" << new_schema.field(i)->name()
                                  << ", origin_type=" << logical_type_to_string(ref_type)
                                  << ", alter_type=" << logical_type_to_string(new_type);
                    }
                }
            }
        } else {
            ColumnPtr& new_col = new_chunk->get_column_by_index(i);
            for (size_t row_index = 0; row_index < base_chunk->num_rows(); ++row_index) {
                new_col->append_datum(_schema_mapping[i].default_value_datum);
            }
        }
    }
    return true;
}

Status ChunkChanger::fill_materialized_columns(ChunkPtr& new_chunk) {
    if (_mc_exprs.size() == 0) {
        return Status::OK();
    }

    // init for expression evaluation only
    for (size_t i = 0; i < new_chunk->num_columns(); ++i) {
        new_chunk->set_slot_id_to_index(i, i);
    }

    for (auto it : _mc_exprs) {
        ASSIGN_OR_RETURN(ColumnPtr tmp, it.second->evaluate(new_chunk.get()));
        if (tmp->only_null()) {
            // Only null column maybe lost type info, we append null
            // for the chunk instead of swapping the tmp column.
            std::dynamic_pointer_cast<NullableColumn>(new_chunk->get_column_by_index(it.first))->reset_column();
            std::dynamic_pointer_cast<NullableColumn>(new_chunk->get_column_by_index(it.first))
                    ->append_nulls(new_chunk->num_rows());
        } else if (tmp->is_nullable()) {
            new_chunk->get_column_by_index(it.first).swap(tmp);
        } else {
            // materialized column must be a nullable column. If tmp is not nullable column,
            // new_chunk can not swap it directly
            // Unpack normal const column
            ColumnPtr output_column = ColumnHelper::unpack_and_duplicate_const_column(new_chunk->num_rows(), tmp);
            std::dynamic_pointer_cast<NullableColumn>(new_chunk->get_column_by_index(it.first))
                    ->swap_by_data_column(output_column);
        }
    }

    // reset the slot-index map for compatibility
    new_chunk->reset_slot_id_to_index();

    return Status::OK();
}

Status ChunkChanger::prepare() {
    // base tablet schema: k1 k2 k3 v1 v2
    // new tablet schema: k3 k1 v2
    // base reader schema: k1 k3 v2
    // selected_column_index: 0 2 4
    // ref_column: 2 0 4
    int32_t index = 0;
    for (int i = 0; i < _schema_mapping.size(); ++i) {
        ColumnMapping* column_mapping = get_mutable_column_mapping(i);
        if (column_mapping == nullptr) {
            return Status::InternalError("referenced column was missing: " + i);
        }
        int32_t ref_column = column_mapping->ref_column;
        if (ref_column < 0) {
            continue;
        }
        if (_slot_id_to_index_map.find(ref_column) == _slot_id_to_index_map.end()) {
            _slot_id_to_index_map.emplace(ref_column, index++);
            _selected_column_indexes.emplace_back(ref_column);
        }
    }
    return Status::OK();
}

#undef CONVERT_FROM_TYPE
#undef TYPE_REINTERPRET_CAST
#undef ASSIGN_DEFAULT_VALUE
#undef COLUMN_APPEND_DATUM

void SchemaChangeUtils::init_materialized_params(const TAlterTabletReqV2& request,
                                                 MaterializedViewParamMap* materialized_view_param_map) {
    DCHECK(materialized_view_param_map != nullptr);
    if (!request.__isset.materialized_view_params) {
        return;
    }

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

        if (item.__isset.mv_expr) {
            mv_param.mv_expr = std::make_unique<starrocks::TExpr>(item.mv_expr);
        }
        materialized_view_param_map->insert(std::make_pair(item.column_name, std::move(mv_param)));
    }
}

Status SchemaChangeUtils::parse_request(const TabletSchema& base_schema, const TabletSchema& new_schema,
                                        ChunkChanger* chunk_changer,
                                        const MaterializedViewParamMap& materialized_view_param_map,
                                        bool has_delete_predicates, bool* sc_sorting, bool* sc_directly,
                                        std::unordered_set<int>* materialized_column_idxs) {
    std::map<ColumnId, ColumnId> base_to_new;
    for (int i = 0; i < new_schema.num_columns(); ++i) {
        const TabletColumn& new_column = new_schema.column(i);
        std::string column_name(new_column.name());
        ColumnMapping* column_mapping = chunk_changer->get_mutable_column_mapping(i);

        if (materialized_view_param_map.find(column_name) != materialized_view_param_map.end()) {
            auto& mvParam = materialized_view_param_map.find(column_name)->second;
            if (mvParam.mv_expr != nullptr) {
                chunk_changer->set_has_mv_expr_context(true);
                RuntimeState* runtime_state = chunk_changer->get_runtime_state();
                if (runtime_state == nullptr) {
                    return Status::InternalError("change materialized view but query_options/query_globals is not set");
                }
                RETURN_IF_ERROR(Expr::create_expr_tree(chunk_changer->get_object_pool(), *(mvParam.mv_expr),
                                                       &(column_mapping->mv_expr_ctx), runtime_state));
                RETURN_IF_ERROR(column_mapping->mv_expr_ctx->prepare(runtime_state));
                RETURN_IF_ERROR(column_mapping->mv_expr_ctx->open(runtime_state));
            }

            int32_t column_index = base_schema.field_index(mvParam.origin_column_name);
            if (column_index >= 0) {
                column_mapping->ref_column = column_index;
                base_to_new[column_index] = i;
                continue;
            } else {
                LOG(WARNING) << "referenced column was missing. "
                             << "[column=" << column_name << " referenced_column=" << column_index << "]"
                             << "[original_column_name=" << mvParam.origin_column_name << "]";
                return Status::InternalError("referenced column was missing");
            }
        }

        int32_t column_index = base_schema.field_index(column_name);
        // if materialized_column_idxs contain column_index, it means that
        // MODIFY MATERIALIZED COLUMN is executed. The value for the new schema
        // must be re-compute by the new expression so the column mapping can not be set.
        if (column_index >= 0 && ((materialized_column_idxs == nullptr) ||
                                  materialized_column_idxs->find(column_index) == materialized_column_idxs->end())) {
            column_mapping->ref_column = column_index;
            base_to_new[column_index] = i;
            continue;
        }

        // to handle new added column
        {
            column_mapping->ref_column = -1;

            if (i < base_schema.num_short_key_columns()) {
                *sc_directly = true;
            }

            if (!init_column_mapping(column_mapping, new_column, new_column.default_value()).ok()) {
                LOG(WARNING) << "init column mapping failed. column=" << new_column.name();
                return Status::InternalError("init column mapping failed");
            }

            VLOG(3) << "A column with default value will be added after schema changing. "
                    << "column=" << column_name << ", default_value=" << new_column.default_value();
            continue;
        }
    }

    // initialized chunk charger state.
    RETURN_IF_ERROR(chunk_changer->prepare());

    // Check if re-aggregation is needed.
    *sc_sorting = false;
    // If the reference sequence of the Key column is out of order, it needs to be reordered
    int num_default_value = 0;

    for (int i = 0; i < new_schema.num_key_columns(); ++i) {
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

    if (base_schema.keys_type() != new_schema.keys_type()) {
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
    if (new_schema.keys_type() != KeysType::DUP_KEYS && new_schema.num_key_columns() < base_schema.num_key_columns()) {
        // this is a table with aggregate key type, and num of key columns in new schema
        // is less, which means the data in new tablet should be more aggregated.
        // so we use sorting schema change to sort and merge the data.
        *sc_sorting = true;
        return Status::OK();
    }

    if (base_schema.num_short_key_columns() != new_schema.num_short_key_columns()) {
        // the number of short_keys changed, can't do linked schema change
        *sc_directly = true;
        return Status::OK();
    }

    for (size_t i = 0; i < new_schema.num_columns(); ++i) {
        ColumnMapping* column_mapping = chunk_changer->get_mutable_column_mapping(i);
        if (column_mapping->ref_column < 0) {
            continue;
        } else {
            auto& new_column = new_schema.column(i);
            auto& ref_column = base_schema.column(column_mapping->ref_column);
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

    if (has_delete_predicates) {
        // there exists delete condition in header, can't do linked schema change
        *sc_directly = true;
    }

    return Status::OK();
}

Status SchemaChangeUtils::init_column_mapping(ColumnMapping* column_mapping, const TabletColumn& column_schema,
                                              const std::string& value) {
    if (column_schema.is_nullable() && value.length() == 0) {
        column_mapping->default_value_datum.set_null();
    } else {
        auto field_type = column_schema.type();
        auto type_info = get_type_info(column_schema);

        switch (field_type) {
        case TYPE_HLL: {
            column_mapping->default_hll = std::make_unique<HyperLogLog>(value);
            column_mapping->default_value_datum.set_hyperloglog(column_mapping->default_hll.get());
            break;
        }
        case TYPE_OBJECT: {
            column_mapping->default_bitmap = std::make_unique<BitmapValue>(value);
            column_mapping->default_value_datum.set_bitmap(column_mapping->default_bitmap.get());
            break;
        }
        case TYPE_PERCENTILE: {
            column_mapping->default_percentile = std::make_unique<PercentileValue>(value);
            column_mapping->default_value_datum.set_percentile(column_mapping->default_percentile.get());
            break;
        }
        case TYPE_JSON: {
            column_mapping->default_json = std::make_unique<JsonValue>(value);
            column_mapping->default_value_datum.set_json(column_mapping->default_json.get());
            break;
        }
        default:
            return datum_from_string(type_info.get(), &column_mapping->default_value_datum, value, nullptr);
        }
    }

    return Status::OK();
}

} // namespace starrocks
