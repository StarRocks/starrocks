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

#include "exec/aggregate/agg_state_bounded.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "types/logical_type.h"

namespace starrocks {
namespace {

constexpr int kFuncVersion = 1;

TExpr make_agg_expr_typed(const std::string& function_name, const TypeDescriptor& ret_type,
                          const std::vector<TypeDescriptor>& arg_types, bool is_nullable = false) {
    TFunctionName fn_name;
    fn_name.__set_function_name(function_name);
    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    std::vector<TTypeDesc> targs;
    targs.reserve(arg_types.size());
    for (const auto& arg : arg_types) {
        targs.push_back(arg.to_thrift());
    }
    fn.__set_arg_types(targs);
    fn.__set_ret_type(ret_type.to_thrift());
    TExprNode node;
    node.__set_fn(fn);
    node.__set_is_nullable(is_nullable);
    TExpr expr;
    expr.nodes.push_back(node);
    return expr;
}

TExpr make_agg_expr(const std::string& function_name, LogicalType ret_type, const std::vector<LogicalType>& arg_types,
                    bool is_nullable = false) {
    std::vector<TypeDescriptor> args;
    args.reserve(arg_types.size());
    for (auto lt : arg_types) {
        args.push_back(TypeDescriptor::from_logical_type(lt));
    }
    return make_agg_expr_typed(function_name, TypeDescriptor::from_logical_type(ret_type), args, is_nullable);
}

} // namespace

TEST(AggStateBoundedTest, pod_states_are_bounded) {
    EXPECT_TRUE(all_agg_states_bounded({make_agg_expr("count", TYPE_BIGINT, {})}, kFuncVersion));
    EXPECT_TRUE(all_agg_states_bounded({make_agg_expr("sum", TYPE_BIGINT, {TYPE_BIGINT})}, kFuncVersion));
    EXPECT_TRUE(all_agg_states_bounded({make_agg_expr("min", TYPE_BIGINT, {TYPE_BIGINT})}, kFuncVersion));
    EXPECT_TRUE(all_agg_states_bounded({make_agg_expr("max", TYPE_BIGINT, {TYPE_BIGINT})}, kFuncVersion));
    EXPECT_TRUE(all_agg_states_bounded({make_agg_expr("any_value", TYPE_BIGINT, {TYPE_BIGINT})}, kFuncVersion));
    EXPECT_TRUE(all_agg_states_bounded({make_agg_expr("avg", TYPE_DOUBLE, {TYPE_DOUBLE})}, kFuncVersion));
}

TEST(AggStateBoundedTest, variance_family_is_bounded) {
    // The exact case from the PR review: variance/stddev states are (count, mean, m2),
    // trivially destructible, so they keep the small-limit pruning optimization.
    EXPECT_TRUE(all_agg_states_bounded({make_agg_expr("variance", TYPE_DOUBLE, {TYPE_DOUBLE})}, kFuncVersion));
    EXPECT_TRUE(all_agg_states_bounded({make_agg_expr("stddev", TYPE_DOUBLE, {TYPE_DOUBLE})}, kFuncVersion));
}

TEST(AggStateBoundedTest, nullable_wrapper_preserves_boundedness) {
    EXPECT_TRUE(all_agg_states_bounded({make_agg_expr("sum", TYPE_BIGINT, {TYPE_BIGINT}, /*is_nullable=*/true)},
                                       kFuncVersion));
}

TEST(AggStateBoundedTest, collect_style_states_are_unbounded) {
    const TypeDescriptor bigint = TypeDescriptor::from_logical_type(TYPE_BIGINT);
    const TypeDescriptor array_bigint = TypeDescriptor::create_array_type(bigint);
    EXPECT_FALSE(all_agg_states_bounded({make_agg_expr_typed("array_agg", array_bigint, {bigint})}, kFuncVersion));
    EXPECT_FALSE(all_agg_states_bounded({make_agg_expr("group_concat", TYPE_VARCHAR, {TYPE_VARCHAR})}, kFuncVersion));
    EXPECT_FALSE(
            all_agg_states_bounded({make_agg_expr("multi_distinct_count", TYPE_BIGINT, {TYPE_BIGINT})}, kFuncVersion));
    EXPECT_FALSE(all_agg_states_bounded({make_agg_expr("bitmap_union", TYPE_OBJECT, {TYPE_OBJECT})}, kFuncVersion));
    EXPECT_FALSE(all_agg_states_bounded({make_agg_expr("hll_raw_agg", TYPE_HLL, {TYPE_HLL})}, kFuncVersion));
}

TEST(AggStateBoundedTest, non_pod_bounded_states_stay_conservative) {
    // String min/max holds a single value but its state owns heap memory, so it is
    // conservatively treated as unbounded (loses the pruning optimization only).
    EXPECT_FALSE(all_agg_states_bounded({make_agg_expr("min", TYPE_VARCHAR, {TYPE_VARCHAR})}, kFuncVersion));
    EXPECT_FALSE(all_agg_states_bounded({make_agg_expr("max", TYPE_VARCHAR, {TYPE_VARCHAR})}, kFuncVersion));
}

TEST(AggStateBoundedTest, one_unbounded_state_taints_the_whole_list) {
    const TypeDescriptor bigint = TypeDescriptor::from_logical_type(TYPE_BIGINT);
    const TypeDescriptor array_bigint = TypeDescriptor::create_array_type(bigint);
    EXPECT_FALSE(all_agg_states_bounded({make_agg_expr("sum", TYPE_BIGINT, {TYPE_BIGINT}),
                                         make_agg_expr_typed("array_agg", array_bigint, {bigint})},
                                        kFuncVersion));
}

TEST(AggStateBoundedTest, unresolvable_functions_are_treated_as_unbounded) {
    EXPECT_FALSE(
            all_agg_states_bounded({make_agg_expr("some_future_agg_func", TYPE_BIGINT, {TYPE_BIGINT})}, kFuncVersion));
}

TEST(AggStateBoundedTest, agg_state_combinators_are_treated_as_unbounded) {
    TExpr expr = make_agg_expr("sum_union", TYPE_BIGINT, {TYPE_BIGINT});
    expr.nodes[0].fn.__set_agg_state_desc(TAggStateDesc());
    EXPECT_FALSE(all_agg_states_bounded({expr}, kFuncVersion));
}

TEST(AggStateBoundedTest, empty_function_list_is_bounded) {
    EXPECT_TRUE(all_agg_states_bounded({}, kFuncVersion));
}

TEST(AggStateBoundedTest, malformed_exprs_are_treated_as_unbounded) {
    TExpr no_nodes;
    EXPECT_FALSE(all_agg_states_bounded({no_nodes}, kFuncVersion));

    TExprNode bare_node;
    TExpr no_fn;
    no_fn.nodes.push_back(bare_node);
    EXPECT_FALSE(all_agg_states_bounded({no_fn}, kFuncVersion));

    // arg-less non-count function cannot be resolved.
    EXPECT_FALSE(all_agg_states_bounded({make_agg_expr("sum", TYPE_BIGINT, {})}, kFuncVersion));
}

} // namespace starrocks
