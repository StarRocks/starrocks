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
#include "column/column_viewer.h"
#include "column/datum_convert.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "types/bitmap_value.h"
#include "types/hll.h"
#include "util/percentile_value.h"

namespace starrocks {

ChunkChanger::ChunkChanger(const TabletSchema& base_schema, const TabletSchema& new_schema)
        : _base_schema(base_schema) {
    _schema_mapping.resize(new_schema.num_columns());
}

ChunkChanger::ChunkChanger(const TabletSchema& base_schema, const TabletSchema& new_schema,
                           std::vector<std::string>& base_table_column_names, TAlterJobType::type alter_job_type)
        : _base_schema(base_schema),
          _base_table_column_names(base_table_column_names),
          _alter_job_type(alter_job_type) {
    _schema_mapping.resize(new_schema.num_columns());
}

ChunkChanger::~ChunkChanger() {
    _schema_mapping.clear();
    for (auto it : _gc_exprs) {
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

Buffer<uint8_t> ChunkChanger::_execute_where_expr(ChunkPtr& chunk) {
    DCHECK(_where_expr != nullptr);
    ColumnPtr filter_col = _where_expr->evaluate(chunk.get()).value();

    size_t size = filter_col->size();
    Buffer<uint8_t> filter(size, 0);
    ColumnViewer<TYPE_BOOLEAN> col(filter_col);
    for (size_t i = 0; i < size; ++i) {
        filter[i] = !col.is_null(i) && col.value(i);
    }
    return filter;
}

bool ChunkChanger::change_chunk_v2(ChunkPtr& base_chunk, ChunkPtr& new_chunk, const Schema& base_schema,
                                   const Schema& new_schema, MemPool* mem_pool) {
    if (new_chunk->num_columns() != _schema_mapping.size()) {
        LOG(WARNING) << "new chunk does not match with schema mapping rules. "
                     << "chunk_schema_size=" << new_chunk->num_columns()
                     << ", mapping_schema_size=" << _schema_mapping.size();
        return false;
    }
    if (_alter_job_type == TAlterJobType::ROLLUP) {
        for (auto slot : _query_slots) {
            size_t column_index = base_chunk->schema()->get_field_index_by_name(slot->col_name());
            if (column_index != -1) {
                VLOG(2) << "col name:" << slot->col_name() << ", slot_id:" << slot->id()
                        << ", column_index:" << column_index;
                base_chunk->set_slot_id_to_index(slot->id(), column_index);
            }
        }
        if (_where_expr) {
            auto filter = _execute_where_expr(base_chunk);
            // If no filtered rows are left, return directly
            if (SIMD::count_nonzero(filter) == 0) {
                base_chunk->set_num_rows(0);
                return true;
            }
            base_chunk->filter(filter);
        }
        DCHECK(!base_chunk->is_empty());
    }

    for (size_t i = 0; i < new_chunk->num_columns(); ++i) {
        int ref_column = _schema_mapping[i].ref_column;
        const TypeInfoPtr& new_type_info = new_schema.field(i)->type();

        // For rollup, if new column has expr, just evalute the mv expr, otherwise do the normal schema change.
        if (_alter_job_type == TAlterJobType::ROLLUP && _schema_mapping[i].mv_expr_ctx != nullptr) {
            VLOG(2) << "This rollup column has mv expr, i=" << i << ", ref_column=" << ref_column
                    << ", new_type=" << new_type_info->type();
            // init for expression evaluation only
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

            if (new_col->only_null()) {
                // unfold only null columns to avoid no default values.
                auto unfold_col = new_col->clone_empty();
                if (!unfold_col->append_nulls(new_col->size())) {
                    return false;
                }
                new_col = std::move(unfold_col);
            } else {
                // NOTE: Unpack const column first to avoid generating NullColumn<ConstColumn> result.
                new_col = ColumnHelper::unpack_and_duplicate_const_column(new_col->size(), new_col);
            }

            if (new_schema.field(i)->is_nullable()) {
                new_col = ColumnHelper::cast_to_nullable_column(new_col);
            }
#ifdef BE_TEST
            VLOG(2) << "evaluate result:" << new_col->debug_string();
#endif
            new_chunk->columns()[i] = std::move(new_col);
            continue;
        }

        if (ref_column >= 0) {
            DCHECK(_column_ref_mapping.find(ref_column) != _column_ref_mapping.end());
            int base_index = _column_ref_mapping.at(ref_column);
            const TypeInfoPtr& ref_type_info = base_schema.field(base_index)->type();

            int reftype_precision = ref_type_info->precision();
            int reftype_scale = ref_type_info->scale();
            int newtype_precision = new_type_info->precision();
            int newtype_scale = new_type_info->scale();
            auto ref_type = ref_type_info->type();
            auto new_type = new_type_info->type();
            VLOG(2) << "i=" << i << ", ref_column=" << ref_column << ", base_index=" << base_index
                    << ", ref_type=" << ref_type << ", new_type=" << new_type;

            ColumnPtr& base_col = base_chunk->get_column_by_index(base_index);
            ColumnPtr& new_col = new_chunk->get_column_by_index(i);
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
                                 << " from_type=" << logical_type_to_string(ref_type)
                                 << ", to_type=" << logical_type_to_string(new_type);
                    return false;
                }
                if (new_type < ref_type) {
                    LOG(INFO) << "type degraded while altering column. "
                              << "column=" << new_schema.field(i)->name()
                              << ", origin_type=" << logical_type_to_string(ref_type)
                              << ", alter_type=" << logical_type_to_string(new_type);
                }
            }
        } else {
            VLOG(2) << "no ref column: i=" << i << ", ref_column=" << ref_column;
            ColumnPtr& new_col = new_chunk->get_column_by_index(i);
            for (size_t row_index = 0; row_index < base_chunk->num_rows(); ++row_index) {
                new_col->append_datum(_schema_mapping[i].default_value_datum);
            }
        }
    }
    return true;
}

Status ChunkChanger::fill_generated_columns(ChunkPtr& new_chunk) {
    if (_gc_exprs.size() == 0) {
        return Status::OK();
    }

    // init for expression evaluation only
    for (size_t i = 0; i < new_chunk->num_columns(); ++i) {
        new_chunk->set_slot_id_to_index(i, i);
    }

    for (auto it : _gc_exprs) {
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
            // generated column must be a nullable column. If tmp is not nullable column,
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
    if (_alter_job_type == TAlterJobType::ROLLUP) {
        int real_idx = 0;
        DCHECK(!_base_table_column_names.empty());
        for (auto& ref_column_name : _base_table_column_names) {
            size_t index = _base_schema.field_index(ref_column_name);
            if (index == -1) {
                return Status::InternalError("referenced column was missing in base table: " + ref_column_name);
            }
            _selected_column_indexes.emplace_back(index);
            // construct column ref mapping from real base table columns which may be subset of all base table columns.
            _column_ref_mapping.insert({index, real_idx++});
        }

        for (int i = 0; i < _schema_mapping.size(); ++i) {
            ColumnMapping* column_mapping = get_mutable_column_mapping(i);
            if (column_mapping == nullptr) {
                return Status::InternalError("referenced column was missing: " + std::to_string(i));
            }
            int32_t ref_column = column_mapping->ref_column;
            if (ref_column < 0) {
                continue;
            }
            auto ref_column_name = _base_schema.column(ref_column).name();
            if (_column_ref_mapping.find(ref_column) == _column_ref_mapping.end()) {
                return Status::InternalError("referenced column was missing in base table: " +
                                             std::string(ref_column_name));
            }
        }
    } else {
        // base tablet schema: k1 k2 k3 v1 v2
        // new tablet schema: k3 k1 v2
        // base reader schema: k1 k3 v2
        // selected_column_index: 0 2 4
        // ref_column: 2 0 4
        int index = 0;
        for (int i = 0; i < _schema_mapping.size(); ++i) {
            ColumnMapping* column_mapping = get_mutable_column_mapping(i);
            if (column_mapping == nullptr) {
                return Status::InternalError("referenced column was missing: " + std::to_string(i));
            }
            int32_t ref_column = column_mapping->ref_column;
            if (ref_column < 0) {
                continue;
            }
            _selected_column_indexes.emplace_back(ref_column);
            // NOTE: construct column ref mapping (column_ref to real base table index) because base table's
            // column orders
            _column_ref_mapping.emplace(ref_column, index++);
        }
    }
    return Status::OK();
}

Status ChunkChanger::append_generated_columns(ChunkPtr& read_chunk, ChunkPtr& new_chunk,
                                              const std::vector<uint32_t>& all_ref_columns_ids,
                                              int base_schema_columns) {
    if (_gc_exprs.size() == 0) {
        return Status::OK();
    }

    // init for expression evaluation only
    DCHECK(all_ref_columns_ids.size() == read_chunk->num_columns());
    for (size_t i = 0; i < read_chunk->num_columns(); ++i) {
        read_chunk->set_slot_id_to_index(all_ref_columns_ids[i], i);
    }

    auto tmp_new_chunk = new_chunk->clone_empty();

    for (auto it : _gc_exprs) {
        // cid for new partial schema
        int cid = it.first - base_schema_columns;
        ASSIGN_OR_RETURN(ColumnPtr tmp, it.second->evaluate(read_chunk.get()));
        if (tmp->only_null()) {
            // Only null column maybe lost type info, we append null
            // for the chunk instead of swapping the tmp column.
            std::dynamic_pointer_cast<NullableColumn>(tmp_new_chunk->get_column_by_index(cid))->reset_column();
            std::dynamic_pointer_cast<NullableColumn>(tmp_new_chunk->get_column_by_index(cid))
                    ->append_nulls(read_chunk->num_rows());
        } else if (tmp->is_nullable()) {
            tmp_new_chunk->get_column_by_index(cid).swap(tmp);
        } else {
            // generated column must be a nullable column. If tmp is not nullable column,
            // it maybe a constant column or some other column type
            // Unpack normal const column
            ColumnPtr output_column = ColumnHelper::unpack_and_duplicate_const_column(read_chunk->num_rows(), tmp);
            std::dynamic_pointer_cast<NullableColumn>(tmp_new_chunk->get_column_by_index(cid))
                    ->swap_by_data_column(output_column);
        }
    }

    // append into the new_chunk
    new_chunk->append(*tmp_new_chunk.get());

    // reset the slot-index map for compatibility
    read_chunk->reset_slot_id_to_index();

    return Status::OK();
}

#undef CONVERT_FROM_TYPE
#undef TYPE_REINTERPRET_CAST
#undef ASSIGN_DEFAULT_VALUE
#undef COLUMN_APPEND_DATUM

void SchemaChangeUtils::init_materialized_params(const TAlterTabletReqV2& request,
                                                 MaterializedViewParamMap* materialized_view_param_map,
                                                 std::unique_ptr<TExpr>& where_expr) {
    DCHECK(materialized_view_param_map != nullptr);
    if (request.__isset.materialized_view_params) {
        for (const auto& item : request.materialized_view_params) {
            AlterMaterializedViewParam mv_param;
            mv_param.column_name = item.column_name;
            // origin_column_name is always be set now,
            // but origin_column_name may be not set in some materialized view function. eg:count(1)
            if (item.__isset.origin_column_name) {
                mv_param.origin_column_name = item.origin_column_name;
            }

            if (item.__isset.mv_expr) {
                mv_param.mv_expr = std::make_unique<starrocks::TExpr>(item.mv_expr);
            }
            materialized_view_param_map->insert(std::make_pair(item.column_name, std::move(mv_param)));
        }
    }

    if (request.__isset.where_expr) {
        where_expr = std::make_unique<starrocks::TExpr>(request.where_expr);
    }
}

Status SchemaChangeUtils::parse_request(const TabletSchema& base_schema, const TabletSchema& new_schema,
                                        ChunkChanger* chunk_changer,
                                        const MaterializedViewParamMap& materialized_view_param_map,
                                        const std::unique_ptr<TExpr>& where_expr, bool has_delete_predicates,
                                        bool* sc_sorting, bool* sc_directly,
                                        std::unordered_set<int>* generated_column_idxs) {
    RETURN_IF_ERROR(parse_request_normal(base_schema, new_schema, chunk_changer, materialized_view_param_map,
                                         where_expr, has_delete_predicates, sc_sorting, sc_directly,
                                         generated_column_idxs));
    if (base_schema.keys_type() == KeysType::PRIMARY_KEYS) {
        return parse_request_for_pk(base_schema, new_schema, sc_sorting, sc_directly);
    }
    return Status::OK();
}

Status SchemaChangeUtils::parse_request_normal(const TabletSchema& base_schema, const TabletSchema& new_schema,
                                               ChunkChanger* chunk_changer,
                                               const MaterializedViewParamMap& materialized_view_param_map,
                                               const std::unique_ptr<TExpr>& where_expr, bool has_delete_predicates,
                                               bool* sc_sorting, bool* sc_directly,
                                               std::unordered_set<int>* generated_column_idxs) {
    bool is_modify_generated_column = false;
    for (int i = 0; i < new_schema.num_columns(); ++i) {
        const TabletColumn& new_column = new_schema.column(i);
        std::string column_name(new_column.name());
        ColumnMapping* column_mapping = chunk_changer->get_mutable_column_mapping(i);

        if (materialized_view_param_map.find(column_name) != materialized_view_param_map.end()) {
            auto& mvParam = materialized_view_param_map.find(column_name)->second;
            if (mvParam.mv_expr != nullptr) {
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
                continue;
            } else {
                LOG(WARNING) << "referenced column was missing. "
                             << "[column=" << column_name << " referenced_column=" << column_index << "]"
                             << "[original_column_name=" << mvParam.origin_column_name << "]";
                return Status::InternalError("referenced column was missing");
            }
        }

        int32_t column_index = base_schema.field_index(column_name);
        // if generated_column_idxs contain column_index, it means that
        // MODIFY GENERATED COLUMN is executed. The value for the new schema
        // must be re-compute by the new expression so the column mapping can not be set.
        if (column_index >= 0 && ((generated_column_idxs == nullptr) ||
                                  generated_column_idxs->find(column_index) == generated_column_idxs->end())) {
            column_mapping->ref_column = column_index;
            continue;
        }

        if (!is_modify_generated_column) {
            is_modify_generated_column = (generated_column_idxs != nullptr) &&
                                         (generated_column_idxs->find(column_index) != generated_column_idxs->end());
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

    if (where_expr) {
        RETURN_IF_ERROR(chunk_changer->prepare_where_expr(*where_expr));
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
            if (new_column.type() != ref_column.type() || is_modify_generated_column) {
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
    if (where_expr) {
        *sc_directly = true;
    }

    return Status::OK();
}

Status SchemaChangeUtils::parse_request_for_pk(const TabletSchema& base_schema, const TabletSchema& new_schema,
                                               bool* sc_sorting, bool* sc_directly) {
    const auto& base_sort_key_idxes = base_schema.sort_key_idxes();
    const auto& new_sort_key_idxes = new_schema.sort_key_idxes();
    std::vector<int32_t> base_sort_key_unique_ids;
    std::vector<int32_t> new_sort_key_unique_ids;
    for (auto idx : base_sort_key_idxes) {
        base_sort_key_unique_ids.emplace_back(base_schema.column(idx).unique_id());
    }
    for (auto idx : new_sort_key_idxes) {
        new_sort_key_unique_ids.emplace_back(new_schema.column(idx).unique_id());
    }

    if (new_sort_key_unique_ids.size() > base_sort_key_unique_ids.size()) {
        // new sort keys' size is greater than base sort keys, must be sc_sorting
        *sc_sorting = true;
        *sc_directly = false;
    } else {
        auto base_iter = base_sort_key_unique_ids.cbegin();
        auto new_iter = new_sort_key_unique_ids.cbegin();
        // check wheather new sort keys are just subset of base sort keys
        while (new_iter != new_sort_key_unique_ids.cend() && *base_iter == *new_iter) {
            ++base_iter;
            ++new_iter;
        }
        if (new_iter != new_sort_key_unique_ids.cend()) {
            *sc_sorting = true;
            *sc_directly = false;
        }
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
