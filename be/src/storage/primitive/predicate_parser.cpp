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

#include "storage/primitive/predicate_parser.h"

#include <strings.h>

#include "common/logging.h"
#include "exprs/expr_context.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/primitive/column_expr_predicate.h"
#include "storage/primitive/column_predicate_factory.h"

namespace starrocks {

namespace predicate_parser_detail {

ColumnPredicate* create_column_predicate(const TCondition& condition, TypeInfoPtr& type_info, ColumnId index) {
    ColumnPredicate* pred = nullptr;
    if ((condition.condition_op == "*=" || condition.condition_op == "=") && condition.condition_values.size() == 1) {
        pred = new_column_eq_predicate(type_info, index, condition.condition_values[0]);
    } else if ((condition.condition_op == "!*=" || condition.condition_op == "!=") &&
               condition.condition_values.size() == 1) {
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
    return pred;
}

ColumnPredicate* create_column_predicate(const GeneralCondition& condition, TypeInfoPtr& type_info, ColumnId index) {
    ColumnPredicate* pred = nullptr;
    if ((condition.condition_op == "*=" || condition.condition_op == "=") && condition.condition_values.size() == 1) {
        pred = new_column_eq_predicate_from_datum(type_info, index, condition.condition_values[0]);
    } else if ((condition.condition_op == "!*=" || condition.condition_op == "!=") &&
               condition.condition_values.size() == 1) {
        pred = new_column_ne_predicate_from_datum(type_info, index, condition.condition_values[0]);
    } else if (condition.condition_op == "<<") {
        pred = new_column_lt_predicate_from_datum(type_info, index, condition.condition_values[0]);
    } else if (condition.condition_op == "<=") {
        pred = new_column_le_predicate_from_datum(type_info, index, condition.condition_values[0]);
    } else if (condition.condition_op == ">>") {
        pred = new_column_gt_predicate_from_datum(type_info, index, condition.condition_values[0]);
    } else if (condition.condition_op == ">=") {
        pred = new_column_ge_predicate_from_datum(type_info, index, condition.condition_values[0]);
    } else if (condition.condition_op == "*=" && condition.condition_values.size() > 1) {
        pred = new_column_in_predicate_from_datum(type_info, index, condition.condition_values);
    } else if ((condition.condition_op == "!*=" || condition.condition_op == "!=") &&
               condition.condition_values.size() > 1) {
        pred = new_column_not_in_predicate_from_datum(type_info, index, condition.condition_values);
    } else if ((condition.condition_op.size() == 4 && strcasecmp(condition.condition_op.c_str(), " is ") == 0) ||
               (condition.condition_op.size() == 2 && strcasecmp(condition.condition_op.c_str(), "is") == 0)) {
        const bool is_null = condition.is_null();
        pred = new_column_null_predicate(type_info, index, is_null);
    } else {
        LOG(WARNING) << "unknown condition: " << condition.condition_op;
        return pred;
    }
    return pred;
}

} // namespace predicate_parser_detail

bool ConnectorPredicateParser::can_pushdown(const ColumnPredicate* predicate) const {
    return true;
}

bool ConnectorPredicateParser::can_pushdown(const SlotDescriptor* slot_desc) const {
    return true;
}

bool ConnectorPredicateParser::can_pushdown(const ConstPredicateNodePtr& pred_tree) const {
    return true;
}

template <typename ConditionType>
ColumnPredicate* ConnectorPredicateParser::t_parse_thrift_cond(const ConditionType& condition) const {
    uint8_t precision = 0;
    uint8_t scale = 0;
    LogicalType type = LogicalType::TYPE_UNKNOWN;
    size_t index = 0;

    for (const SlotDescriptor* slot : *_slot_desc) {
        if (slot->col_name() == condition.column_name) {
            type = slot->type().type;
            precision = slot->type().precision;
            scale = slot->type().scale;
            index = slot->id();
            break;
        }
    }
    // column not found
    RETURN_IF(type == LogicalType::TYPE_UNKNOWN, nullptr);

    auto&& type_info = get_type_info(type, precision, scale);

    return predicate_parser_detail::create_column_predicate(condition, type_info, index);
}

StatusOr<ColumnPredicate*> ConnectorPredicateParser::parse_thrift_cond(const TCondition& condition) const {
    return t_parse_thrift_cond(condition);
}

StatusOr<ColumnPredicate*> ConnectorPredicateParser::parse_thrift_cond(const GeneralCondition& condition) const {
    return t_parse_thrift_cond(condition);
}

StatusOr<ColumnPredicate*> ConnectorPredicateParser::parse_expr_ctx(const SlotDescriptor& slot_desc,
                                                                    RuntimeState* state, ExprContext* expr_ctx) const {
    uint8_t precision = 0;
    uint8_t scale = 0;
    LogicalType type = LogicalType::TYPE_UNKNOWN;
    size_t column_id = 0;

    for (const SlotDescriptor* slot : *_slot_desc) {
        if (slot->col_name() == slot_desc.col_name()) {
            type = slot->type().type;
            precision = slot->type().precision;
            scale = slot->type().scale;
            column_id = slot->id();
            break;
        }
    }
    RETURN_IF(type == LogicalType::TYPE_UNKNOWN, nullptr);

    auto&& type_info = get_type_info(type, precision, scale);
    return ColumnExprPredicate::make_column_expr_predicate(type_info, column_id, state, expr_ctx, &slot_desc);
}

uint32_t ConnectorPredicateParser::column_id(const SlotDescriptor& slot_desc) const {
    return slot_desc.id();
}

} // namespace starrocks
