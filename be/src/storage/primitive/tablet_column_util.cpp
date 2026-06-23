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

#include "storage/primitive/tablet_column_util.h"

#include <chrono>
#include <ctime>
#include <memory>
#include <vector>

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/cast_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/expr_factory.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "runtime/runtime_state.h"
#include "storage/primitive/aggregate_type.h"
#include "storage/primitive/tablet_column_type_utils.h"
#include "types/agg_state_desc.h"
#include "types/collection.h"

namespace starrocks {

// Old version StarRocks use `TColumnType` to save type info, convert it into `TTypeDesc`.
// NOTE: This is only used for some legacy UT
void convert_to_new_version(TColumn* tcolumn) {
    if (!tcolumn->__isset.type_desc) {
        tcolumn->__set_index_len(tcolumn->column_type.index_len);

        TScalarType scalar_type;
        scalar_type.__set_type(tcolumn->column_type.type);
        scalar_type.__set_len(tcolumn->column_type.len);
        scalar_type.__set_precision(tcolumn->column_type.precision);
        scalar_type.__set_scale(tcolumn->column_type.scale);

        tcolumn->type_desc.types.resize(1);
        tcolumn->type_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        tcolumn->type_desc.types.back().__set_scalar_type(scalar_type);
        tcolumn->__isset.type_desc = true;
    }
}
static StorageAggregateType t_aggregation_type_to_field_aggregation_method(TAggregationType::type agg_type) {
    switch (agg_type) {
    case TAggregationType::NONE:
        return STORAGE_AGGREGATE_NONE;
    case TAggregationType::MAX:
        return STORAGE_AGGREGATE_MAX;
    case TAggregationType::MIN:
        return STORAGE_AGGREGATE_MIN;
    case TAggregationType::REPLACE:
        return STORAGE_AGGREGATE_REPLACE;
    case TAggregationType::REPLACE_IF_NOT_NULL:
        return STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL;
    case TAggregationType::BITMAP_UNION:
        return STORAGE_AGGREGATE_BITMAP_UNION;
    case TAggregationType::HLL_UNION:
        return STORAGE_AGGREGATE_HLL_UNION;
    case TAggregationType::SUM:
        return STORAGE_AGGREGATE_SUM;
    case TAggregationType::PERCENTILE_UNION:
        return STORAGE_AGGREGATE_PERCENTILE_UNION;
    case TAggregationType::AGG_STATE_UNION:
        return STORAGE_AGGREGATE_AGG_STATE_UNION;
    }
    return STORAGE_AGGREGATE_NONE;
}

// This function is used to initialize ColumnPB for subfield like element of Array.
static void init_column_pb_for_sub_field(ColumnPB* field) {
    const int32_t kFakeUniqueId = -1;

    field->set_unique_id(kFakeUniqueId);
    field->set_is_key(false);
    field->set_is_nullable(true);
    field->set_aggregation(get_string_by_aggregation_type(STORAGE_AGGREGATE_NONE));
}

// Because thrift doesn't support nested definition. So we use list to flatten the nested.
// In thrift, types and index will be used together to present one type.
// After this function returned, index will point to the next position to be parsed.
static Status type_desc_to_pb(const std::vector<TTypeNode>& types, int* index, ColumnPB* column_pb) {
    const TTypeNode& curr_type_node = types[*index];
    ++(*index);
    switch (curr_type_node.type) {
    case TTypeNodeType::SCALAR: {
        auto& scalar = curr_type_node.scalar_type;

        LogicalType field_type = thrift_to_type(scalar.type);
        column_pb->set_type(logical_type_to_string(field_type));
        column_pb->set_length(get_tablet_column_field_length_by_type(field_type, scalar.len));
        column_pb->set_index_length(column_pb->length());
        column_pb->set_frac(curr_type_node.scalar_type.scale);
        column_pb->set_precision(curr_type_node.scalar_type.precision);
        return Status::OK();
    }
    case TTypeNodeType::ARRAY: {
        column_pb->set_type(logical_type_to_string(TYPE_ARRAY));

        // FIXME: I'm not sure if these fields are necessary, just keep it to be safe
        column_pb->set_length(get_tablet_column_field_length_by_type(TYPE_ARRAY, sizeof(Collection)));
        column_pb->set_index_length(column_pb->length());

        // Currently, All array element is nullable
        auto field_pb = column_pb->add_children_columns();
        init_column_pb_for_sub_field(field_pb);
        RETURN_IF_ERROR(type_desc_to_pb(types, index, field_pb));
        field_pb->set_name("element");
        return Status::OK();
    }
    case TTypeNodeType::STRUCT: {
        column_pb->set_type(logical_type_to_string(TYPE_STRUCT));

        // FIXME: I'm not sure if these fields are necessary, just keep it to be safe
        column_pb->set_length(get_tablet_column_field_length_by_type(TYPE_STRUCT, sizeof(Collection)));
        column_pb->set_index_length(column_pb->length());

        auto& fields = curr_type_node.struct_fields;
        for (const auto& field : fields) {
            auto field_pb = column_pb->add_children_columns();
            init_column_pb_for_sub_field(field_pb);
            // All struct fields all nullable now
            RETURN_IF_ERROR(type_desc_to_pb(types, index, field_pb));
            field_pb->set_name(field.name);
            if (field.__isset.id && field.id >= 0) {
                field_pb->set_unique_id(field.id);
            }
        }
        return Status::OK();
    }
    case TTypeNodeType::MAP: {
        column_pb->set_type(logical_type_to_string(TYPE_MAP));

        // FIXME: I'm not sure if these fields are necessary, just keep it to be safe
        column_pb->set_length(get_tablet_column_field_length_by_type(TYPE_MAP, sizeof(Collection)));
        column_pb->set_index_length(column_pb->length());

        {
            auto key_pb = column_pb->add_children_columns();
            init_column_pb_for_sub_field(key_pb);
            RETURN_IF_ERROR(type_desc_to_pb(types, index, key_pb));
            key_pb->set_name("key");
        }
        {
            auto value_pb = column_pb->add_children_columns();
            init_column_pb_for_sub_field(value_pb);
            RETURN_IF_ERROR(type_desc_to_pb(types, index, value_pb));
            value_pb->set_name("value");
        }
        return Status::OK();
    }
    }
    return Status::InternalError("Unreachable path");
}

Status t_column_to_pb_column(int32_t unique_id, const TColumn& t_column, ColumnPB* column_pb) {
    DCHECK(t_column.__isset.type_desc);
    const std::vector<TTypeNode>& types = t_column.type_desc.types;
    int index = 0;
    RETURN_IF_ERROR(type_desc_to_pb(types, &index, column_pb));
    if (index != types.size()) {
        LOG(WARNING) << "Schema not match, size:" << types.size() << ", index=" << index;
        return Status::InternalError("Failed to parse type, number of schema elements not match");
    }
    column_pb->set_unique_id(unique_id);
    column_pb->set_name(t_column.column_name);
    column_pb->set_is_key(t_column.is_key);
    column_pb->set_is_nullable(t_column.is_allow_null);
    column_pb->set_has_bitmap_index(t_column.has_bitmap_index);
    column_pb->set_is_auto_increment(t_column.is_auto_increment);
    if (t_column.is_key) {
        auto agg_method = STORAGE_AGGREGATE_NONE;
        column_pb->set_aggregation(get_string_by_aggregation_type(agg_method));
    } else {
        auto agg_method = t_aggregation_type_to_field_aggregation_method(t_column.aggregation_type);
        column_pb->set_aggregation(get_string_by_aggregation_type(agg_method));
    }

    if (types[0].type == TTypeNodeType::SCALAR && types[0].scalar_type.type == TPrimitiveType::VARCHAR) {
        int32_t index_len = t_column.__isset.index_len ? t_column.index_len : 10;
        column_pb->set_index_length(index_len);
    }
    // Default value
    if (t_column.__isset.default_value) {
        column_pb->set_default_value(t_column.default_value);
    } else if (t_column.__isset.default_expr) {
        // Fallback strategy:
        // In most paths, BE will preprocess `default_expr` (TExpr) into `default_value` before reaching here.
        // However, ColumnPB only persists `default_value`, so if some caller forgets the preprocessing step,
        // the default may be lost (especially for complex types). Convert `default_expr` -> `default_value` here
        // only when `default_value` is not set to make schema conversion robust.
        auto converted = convert_default_expr_to_json_string(t_column.default_expr);
        if (converted.ok()) {
            column_pb->set_default_value(converted.value());
        } else {
            LOG(WARNING) << "Failed to convert default_expr to JSON String for column '" << t_column.column_name
                         << "': " << converted.status().to_string();
        }
    }
    if (t_column.__isset.is_bloom_filter_column) {
        column_pb->set_is_bf_column(t_column.is_bloom_filter_column);
    }
    // agg state type desc
    if (t_column.__isset.agg_state_desc) {
        auto& agg_state_desc = t_column.agg_state_desc;
        auto* agg_state_pb = column_pb->mutable_agg_state_desc();
        AggStateDesc::thrift_to_protobuf(agg_state_desc, agg_state_pb);
    }
    return Status::OK();
}

// Helper function to create a minimal RuntimeState for constant expression evaluation
static std::unique_ptr<RuntimeState> create_temp_runtime_state() {
    TQueryGlobals query_globals;
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    std::tm tm_buf;
    localtime_r(&now_time_t, &tm_buf);
    char time_str[64];
    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &tm_buf);

    query_globals.now_string = time_str;
    query_globals.timestamp_ms = now_ms;
    query_globals.time_zone = "UTC";

    auto state = std::make_unique<RuntimeState>(query_globals);
    state->init_instance_mem_tracker();

    return state;
}

StatusOr<std::string> convert_default_expr_to_json_string(const TExpr& t_expr) {
    auto state = create_temp_runtime_state();
    ObjectPool pool;

    ExprContext* ctx = nullptr;
    RETURN_IF_ERROR(ExprFactory::create_expr_tree(&pool, t_expr, &ctx, state.get()));

    if (ctx == nullptr || ctx->root() == nullptr) {
        return Status::InternalError("Failed to create expression tree from TExpr");
    }

    RETURN_IF_ERROR(ctx->prepare(state.get()));
    RETURN_IF_ERROR(ctx->open(state.get()));

    ColumnPtr column;
    auto eval_result = ctx->root()->evaluate_const(ctx);
    if (!eval_result.ok()) {
        ctx->close(state.get());
        return eval_result.status();
    }
    column = eval_result.value();

    if (column == nullptr || column->size() == 0) {
        ctx->close(state.get());
        return Status::InternalError("Failed to evaluate default expression: empty result");
    }

    if (column->is_constant()) {
        auto const_col = down_cast<const ConstColumn*>(column.get());
        column = const_col->data_column()->clone();
    }

    DCHECK_EQ(column->size(), 1) << "Default constant expression should produce exactly one value";

    ASSIGN_OR_RETURN(auto json_str, cast_type_to_json_str(column, 0, true));
    ctx->close(state.get());

    return json_str;
}

Status preprocess_default_expr_for_tcolumns(std::vector<TColumn>& columns) {
    for (auto& column : columns) {
        if (column.__isset.default_expr) {
            auto result = convert_default_expr_to_json_string(column.default_expr);
            if (result.ok()) {
                column.default_value = result.value();
                column.__isset.default_value = true;
            } else {
                LOG(ERROR) << "Failed to convert default_expr to JSON String for column '" << column.column_name
                           << "': " << result.status().to_string();
            }
        }
    }

    return Status::OK();
}

} // namespace starrocks
