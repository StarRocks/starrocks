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

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/global_types.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"

namespace starrocks::bench {

inline TTypeDesc make_scalar_type(TPrimitiveType::type type) {
    TTypeDesc desc;
    TTypeNode node;
    node.__set_type(TTypeNodeType::SCALAR);
    TScalarType scalar;
    scalar.__set_type(type);
    node.__set_scalar_type(scalar);
    desc.types.push_back(node);
    return desc;
}

inline TExpr make_slot_ref_expr(TupleId tuple_id, SlotId slot_id, const TTypeDesc& type, bool nullable = true) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_type(type);
    node.__set_num_children(0);
    node.__set_is_nullable(nullable);
    TSlotRef slot_ref;
    slot_ref.__set_slot_id(slot_id);
    slot_ref.__set_tuple_id(tuple_id);
    node.__set_slot_ref(slot_ref);

    TExpr expr;
    expr.nodes.push_back(std::move(node));
    return expr;
}

inline TExpr make_bigint_literal_expr(int64_t value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::INT_LITERAL);
    node.__set_type(make_scalar_type(TPrimitiveType::BIGINT));
    node.__set_num_children(0);
    node.__set_is_nullable(false);
    TIntLiteral literal;
    literal.__set_value(value);
    node.__set_int_literal(literal);

    TExpr expr;
    expr.nodes.push_back(std::move(node));
    return expr;
}

inline TExpr make_null_literal_expr(const TTypeDesc& type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::NULL_LITERAL);
    node.__set_type(type);
    node.__set_num_children(0);
    node.__set_is_nullable(true);

    TExpr expr;
    expr.nodes.push_back(std::move(node));
    return expr;
}

inline TExpr make_builtin_aggregate_expr(const std::string& function_name,
                                         const std::vector<TTypeDesc>& function_arg_types, const TTypeDesc& return_type,
                                         const std::vector<TExpr>& arguments, bool ignore_nulls = false,
                                         bool nullable = true) {
    TExprNode aggregate;
    aggregate.__set_node_type(TExprNodeType::AGG_EXPR);
    aggregate.__set_num_children(arguments.size());
    aggregate.__set_type(return_type);
    aggregate.__set_has_nullable_child(nullable);
    aggregate.__set_is_nullable(nullable);

    TAggregateExpr aggregate_expr;
    aggregate_expr.__set_is_merge_agg(false);
    aggregate.__set_agg_expr(aggregate_expr);

    TFunction function;
    TFunctionName name;
    name.__set_function_name(function_name);
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_arg_types(function_arg_types);
    function.__set_ret_type(return_type);
    function.__set_has_var_args(false);
    function.__set_ignore_nulls(ignore_nulls);
    aggregate.__set_fn(function);

    TExpr expr;
    expr.nodes.push_back(std::move(aggregate));
    for (const auto& argument : arguments) {
        expr.nodes.insert(expr.nodes.end(), argument.nodes.begin(), argument.nodes.end());
    }
    return expr;
}

inline TAnalyticWindowBoundary make_rows_offset_boundary(TAnalyticWindowBoundaryType::type type, int64_t offset) {
    TAnalyticWindowBoundary boundary;
    boundary.__set_type(type);
    boundary.__set_rows_offset_value(offset);
    return boundary;
}

inline TAnalyticWindow make_rows_window(const std::optional<TAnalyticWindowBoundary>& start,
                                        const std::optional<TAnalyticWindowBoundary>& end) {
    TAnalyticWindow window;
    window.__set_type(TAnalyticWindowType::ROWS);
    if (start.has_value()) {
        window.__set_window_start(start.value());
    }
    if (end.has_value()) {
        window.__set_window_end(end.value());
    }
    return window;
}

inline TPlanNode make_analytic_plan_node(const std::vector<TExpr>& analytic_functions, TupleId buffered_tuple_id,
                                         const TAnalyticWindow& window, int32_t node_id = 0, int64_t limit = -1) {
    TAnalyticNode analytic_node;
    analytic_node.__set_window(window);
    analytic_node.__set_buffered_tuple_id(buffered_tuple_id);
    analytic_node.__set_analytic_functions(analytic_functions);

    TPlanNode plan_node;
    plan_node.__set_node_id(node_id);
    plan_node.__set_node_type(TPlanNodeType::ANALYTIC_EVAL_NODE);
    plan_node.__set_limit(limit);
    plan_node.__set_analytic_node(analytic_node);
    return plan_node;
}

} // namespace starrocks::bench
