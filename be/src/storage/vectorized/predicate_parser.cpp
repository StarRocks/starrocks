// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/predicate_parser.h"

#include "chunk_helper.h"
#include "gen_cpp/InternalService_types.h"
#include "storage/tablet_schema.h"
#include "storage/vectorized/column_predicate.h"
#include "storage/vectorized/type_utils.h"

namespace starrocks::vectorized {

bool PredicateParser::can_pushdown(const ColumnPredicate* predicate) const {
    RETURN_IF(predicate->column_id() >= _schema.num_columns(), false);
    const TabletColumn& column = _schema.column(predicate->column_id());
    return _schema.keys_type() == KeysType::PRIMARY_KEYS ||
           column.aggregation() == FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE;
}

ColumnPredicate* PredicateParser::parse(const TCondition& condition) const {
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

} // namespace starrocks::vectorized
