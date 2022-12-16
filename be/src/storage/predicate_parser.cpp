// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/predicate_parser.h"

#include "exprs/expr_context.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/column_expr_predicate.h"
#include "storage/tablet_schema.h"
#include "storage/type_utils.h"
#include "storage/vectorized_column_predicate.h"

namespace starrocks::vectorized {

bool PredicateParser::can_pushdown(const ColumnPredicate* predicate) const {
    RETURN_IF(predicate->column_id() >= _schema.num_columns(), false);
    const TabletColumn& column = _schema.column(predicate->column_id());
    return _schema.keys_type() == KeysType::PRIMARY_KEYS ||
           column.aggregation() == FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE;
}

bool PredicateParser::can_pushdown(const SlotDescriptor* slot_desc) const {
    const size_t index = _schema.field_index(slot_desc->col_name());
    CHECK(index <= _schema.num_columns());
    const TabletColumn& column = _schema.column(index);
    return _schema.keys_type() == KeysType::PRIMARY_KEYS ||
           column.aggregation() == FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE;
}

ColumnPredicate* PredicateParser::parse_thrift_cond(const TCondition& condition) const {
    const size_t index = _schema.field_index(condition.column_name);
    RETURN_IF(index >= _schema.num_columns(), nullptr);
    const TabletColumn& col = _schema.column(index);
    auto precision = col.precision();
    auto scale = col.scale();
    auto type = TypeUtils::to_storage_format_v2(col.type());
    auto&& type_info = get_type_info(type, precision, scale);

    ColumnPredicate* pred = nullptr;
    if ((condition.condition_op == "*=" || condition.condition_op == "=") && condition.condition_values.size() == 1) {
        pred = new_column_eq_predicate(type_info, index, condition.condition_values[0]);
    } else if (condition.condition_op == "!=" && condition.condition_values.size() == 1) {
        pred = new_column_ne_predicate(type_info, index, condition.condition_values[0]);
    } else if (condition.condition_op == "<<") {
        pred = new_column_lt_predicate(type_info, index, condition.condition_values[0]);
    } else if (condition.condition_op == "<=") {
        pred = new_column_le_predicate(type_info, index, condition.condition_values[0]);
    } else if (condition.condition_op == ">>") {
        pred = new_column_gt_predicate(type_info, index, condition.condition_values[0]);
    } else if (condition.condition_op == ">=") {
        pred = new_column_ge_predicate(type_info, index, condition.condition_values[0]);
    } else if (condition.condition_op == "*=" && condition.condition_values.size() > 1) {
        pred = new_column_in_predicate(type_info, index, condition.condition_values);
    } else if (condition.condition_op == "!*=" && condition.condition_values.size() > 1) {
        pred = new_column_not_in_predicate(type_info, index, condition.condition_values);
    } else if (condition.condition_op == "!=" && condition.condition_values.size() > 1) {
        pred = new_column_not_in_predicate(type_info, index, condition.condition_values);
    } else if ((condition.condition_op.size() == 4 && strcasecmp(condition.condition_op.c_str(), " is ") == 0) ||
               (condition.condition_op.size() == 2 && strcasecmp(condition.condition_op.c_str(), "is") == 0)) {
        bool is_null = strcasecmp(condition.condition_values[0].c_str(), "null") == 0;
        pred = new_column_null_predicate(type_info, index, is_null);
    } else {
        LOG(WARNING) << "unknown condition: " << condition.condition_op;
        return pred;
    }

    if (type == OLAP_FIELD_TYPE_CHAR) {
        pred->padding_zeros(col.length());
    }
    return pred;
}

StatusOr<ColumnPredicate*> PredicateParser::parse_expr_ctx(const SlotDescriptor& slot_desc, RuntimeState* state,
                                                           ExprContext* expr_ctx) const {
    const size_t column_id = _schema.field_index(slot_desc.col_name());
    RETURN_IF(column_id >= _schema.num_columns(), nullptr);
    const TabletColumn& col = _schema.column(column_id);
    auto precision = col.precision();
    auto scale = col.scale();
    auto type = TypeUtils::to_storage_format_v2(col.type());
    auto&& type_info = get_type_info(type, precision, scale);
    return ColumnExprPredicate::make_column_expr_predicate(type_info, column_id, state, expr_ctx, &slot_desc);
}

} // namespace starrocks::vectorized
