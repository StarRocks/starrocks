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

#include "storage/predicate_parser.h"

#include <algorithm>

#include "exprs/expr_context.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/primitive/column_expr_predicate.h"
#include "storage/primitive/predicate_tree/predicate_tree.hpp"
#include "storage/tablet_schema.h"

namespace starrocks {

bool OlapPredicateParser::can_pushdown(const ColumnPredicate* predicate) const {
    RETURN_IF(predicate->column_id() >= _schema->num_columns(), false);
    const TabletColumn& column = _schema->column(predicate->column_id());
    return _schema->keys_type() == KeysType::PRIMARY_KEYS ||
           column.aggregation() == StorageAggregateType::STORAGE_AGGREGATE_NONE;
}

bool OlapPredicateParser::can_pushdown(const SlotDescriptor* slot_desc) const {
    const size_t index = _schema->field_index(slot_desc->col_name());
    CHECK(index <= _schema->num_columns());
    const TabletColumn& column = _schema->column(index);
    return _schema->keys_type() == KeysType::PRIMARY_KEYS ||
           column.aggregation() == StorageAggregateType::STORAGE_AGGREGATE_NONE;
}

struct CanPushDownVisitor {
    bool operator()(const PredicateColumnNode& node) const { return parent->can_pushdown(node.col_pred()); }

    template <CompoundNodeType Type>
    bool operator()(const PredicateCompoundNode<Type>& node) const {
        return std::all_of(node.children().begin(), node.children().end(),
                           [this](const auto& child) { return child.visit(*this); });
    }

    const PredicateParser* parent;
};

bool OlapPredicateParser::can_pushdown(const ConstPredicateNodePtr& pred_tree) const {
    return pred_tree.visit(CanPushDownVisitor{this});
}

template <typename ConditionType>
StatusOr<ColumnPredicate*> OlapPredicateParser::t_parse_thrift_cond(const ConditionType& condition) const {
    const size_t index = _schema->field_index(condition.column_name);
    RETURN_IF(index >= _schema->num_columns(), Status::Unknown("unknown column " + condition.column_name));
    const TabletColumn& col = _schema->column(index);
    auto precision = col.precision();
    auto scale = col.scale();
    auto type = col.type();
    auto&& type_info = get_type_info(type, precision, scale);

    ColumnPredicate* pred = predicate_parser_detail::create_column_predicate(condition, type_info, index);
    RETURN_IF(pred == nullptr, Status::Unknown("unknown condition"));

    if (type == TYPE_CHAR) {
        pred->padding_zeros(col.length());
    }
    return pred;
}

StatusOr<ColumnPredicate*> OlapPredicateParser::parse_thrift_cond(const TCondition& condition) const {
    return t_parse_thrift_cond(condition);
}

StatusOr<ColumnPredicate*> OlapPredicateParser::parse_thrift_cond(const GeneralCondition& condition) const {
    return t_parse_thrift_cond(condition);
}

StatusOr<ColumnPredicate*> OlapPredicateParser::parse_expr_ctx(const SlotDescriptor& slot_desc, RuntimeState* state,
                                                               ExprContext* expr_ctx) const {
    const size_t column_id = _schema->field_index(slot_desc.col_name());
    RETURN_IF(column_id >= _schema->num_columns(), nullptr);
    const TabletColumn& col = _schema->column(column_id);
    auto precision = col.precision();
    auto scale = col.scale();
    auto type = col.type();
    auto&& type_info = get_type_info(type, precision, scale);
    return ColumnExprPredicate::make_column_expr_predicate(type_info, column_id, state, expr_ctx, &slot_desc);
}

uint32_t OlapPredicateParser::column_id(const SlotDescriptor& slot_desc) const {
    return _schema->field_index(slot_desc.col_name());
}

} // namespace starrocks
