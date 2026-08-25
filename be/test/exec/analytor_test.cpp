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

#include "exec/analytor.h"

#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "common/config_exec_fwd.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {
class AnalytorTest : public ::testing::Test {
public:
    void SetUp() override { config::vector_chunk_size = 1024; }
};

// NOLINTNEXTLINE
TEST_F(AnalytorTest, find_peer_group_end) {
    TPlanNode plan_node;
    Analytor analytor(plan_node, nullptr, false);

    int32_t v;
    auto c1 = Int32Column::create();
    v = 1;
    c1->append_value_multiple_times(&v, 10);
    v = 2;
    c1->append_value_multiple_times(&v, 10);

    analytor._input_rows += 20;
    analytor._order_columns.emplace_back(std::move(c1));
    analytor._partition.is_real = true;
    analytor._partition.end = 20;

    analytor._find_peer_group_end();
    ASSERT_TRUE(analytor._peer_group.is_real);
    ASSERT_EQ(analytor._peer_group.end, 10);
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, reset_state_for_next_partition) {
    TPlanNode plan_node;
    Analytor analytor(plan_node, nullptr, false);

    analytor._partition.start = 10;
    analytor._partition.is_real = true;
    analytor._partition.end = 20;
    analytor._reset_state_for_next_partition();
    ASSERT_EQ(analytor._partition.start, 20);
    ASSERT_EQ(analytor._partition.end, 20);
    ASSERT_EQ(analytor._current_row_position, 20);
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, find_partition_end) {
    TPlanNode plan_node;
    Analytor analytor1(plan_node, nullptr, false);

    int32_t v;
    auto c1 = Int32Column::create();
    v = 1;
    c1->append_value_multiple_times(&v, 10);
    v = 2;
    c1->append_value_multiple_times(&v, 10);

    auto c2 = Int32Column::create();
    v = 3;
    c2->append_value_multiple_times(&v, 5);
    v = 4;
    c2->append_value_multiple_times(&v, 15);

    analytor1._input_rows += 20;
    analytor1._input_eos = true;
    analytor1._partition_columns.emplace_back(std::move(c1));
    analytor1._partition_columns.emplace_back(std::move(c2));

    analytor1._current_row_position = analytor1._partition.end;
    analytor1._find_partition_end();
    ASSERT_TRUE(analytor1._partition.is_real);
    ASSERT_EQ(analytor1._partition.end, 5);

    analytor1._reset_state_for_next_partition();

    analytor1._current_row_position = analytor1._partition.end;
    analytor1._find_partition_end();
    ASSERT_TRUE(analytor1._partition.is_real);
    ASSERT_EQ(analytor1._partition.end, 10);

    analytor1._reset_state_for_next_partition();

    analytor1._current_row_position = analytor1._partition.end;
    analytor1._find_partition_end();
    ASSERT_TRUE(analytor1._partition.is_real);
    ASSERT_EQ(analytor1._partition.end, 20);

    // partition columns is empty
    Analytor analytor2(plan_node, nullptr, false);
    analytor2._input_rows += 20;
    analytor1._input_eos = true;

    analytor2._current_row_position = analytor2._partition.end;
    analytor2._find_partition_end();
    ASSERT_FALSE(analytor2._partition.is_real);
    ASSERT_EQ(analytor2._partition.end, 20);

    // input rows = 0
    Analytor analytor3(plan_node, nullptr, false);
    analytor3._input_rows = 0;
    analytor1._input_eos = true;

    analytor2._current_row_position = analytor2._partition.end;
    analytor3._find_partition_end();
    ASSERT_FALSE(analytor3._partition.is_real);
    ASSERT_EQ(analytor3._partition.end, 0);
}

namespace {
TExpr make_window_function(const std::string& name,
                           TFunctionBinaryType::type binary_type = TFunctionBinaryType::BUILTIN,
                           TPrimitiveType::type ret_type = TPrimitiveType::BIGINT) {
    TFunctionName fn_name;
    fn_name.__set_function_name(name);
    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(binary_type);
    TScalarType scalar_type;
    scalar_type.__set_type(ret_type);
    if (ret_type == TPrimitiveType::VARCHAR) {
        scalar_type.__set_len(10);
    }
    TTypeNode type_node;
    type_node.__set_type(TTypeNodeType::SCALAR);
    type_node.__set_scalar_type(scalar_type);
    TTypeDesc ret;
    ret.types.push_back(type_node);
    fn.__set_ret_type(ret);
    TExprNode node;
    node.__set_fn(fn);
    TExpr expr;
    expr.nodes.push_back(node);
    return expr;
}

TPlanNode make_analytic_plan_node(const std::vector<TExpr>& functions) {
    TAnalyticNode analytic_node;
    analytic_node.__set_analytic_functions(functions);
    TPlanNode plan_node;
    plan_node.__set_analytic_node(analytic_node);
    return plan_node;
}
} // namespace

// NOLINTNEXTLINE
TEST_F(AnalytorTest, tnode_supports_spill_frames) {
    // No window clause: frame is the whole partition, eligible.
    TPlanNode plan_node = make_analytic_plan_node({make_window_function("sum")});
    ASSERT_TRUE(Analytor::tnode_supports_spill(plan_node));

    // Window clause with neither bound set (UNBOUNDED ~ UNBOUNDED): eligible.
    TAnalyticWindow window;
    window.__set_type(TAnalyticWindowType::ROWS);
    plan_node.analytic_node.__set_window(window);
    ASSERT_TRUE(Analytor::tnode_supports_spill(plan_node));

    // Any explicit bound makes the frame non-whole-partition: not eligible.
    TAnalyticWindowBoundary window_start;
    window_start.__set_type(TAnalyticWindowBoundaryType::PRECEDING);
    plan_node.analytic_node.window.__set_window_start(window_start);
    ASSERT_FALSE(Analytor::tnode_supports_spill(plan_node));
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, tnode_supports_spill_functions) {
    // Whitelisted aggregates over the whole-partition frame are eligible.
    for (const auto& name : {"sum", "avg", "count", "max", "min", "first_value", "last_value"}) {
        TPlanNode plan_node = make_analytic_plan_node({make_window_function(name)});
        ASSERT_TRUE(Analytor::tnode_supports_spill(plan_node)) << name;
    }

    // Rank-family / partition-size / offset functions are not.
    for (const auto& name : {"ntile", "cume_dist", "percent_rank", "rank", "row_number", "lead", "lag"}) {
        TPlanNode plan_node = make_analytic_plan_node({make_window_function(name)});
        ASSERT_FALSE(Analytor::tnode_supports_spill(plan_node)) << name;
    }

    // One ineligible function disqualifies the whole node.
    TPlanNode plan_node = make_analytic_plan_node({make_window_function("sum"), make_window_function("ntile")});
    ASSERT_FALSE(Analytor::tnode_supports_spill(plan_node));

    // Non-builtin (UDAF) functions are not eligible.
    plan_node = make_analytic_plan_node({make_window_function("sum", TFunctionBinaryType::SRJAR)});
    ASSERT_FALSE(Analytor::tnode_supports_spill(plan_node));

    // Variable-size result types are eligible too (append-only result
    // columns); their per-partition results stay resident until pass 2
    // completes — the per-session opt-out is the ANALYTIC mask bit.
    plan_node = make_analytic_plan_node(
            {make_window_function("max", TFunctionBinaryType::BUILTIN, TPrimitiveType::VARCHAR)});
    ASSERT_TRUE(Analytor::tnode_supports_spill(plan_node));
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, whole_partition_frame_flag) {
    // Mirrors the constructor sites that mark _is_whole_partition_frame.
    TPlanNode plan_node = make_analytic_plan_node({make_window_function("sum")});
    Analytor no_window(plan_node, nullptr, false);
    ASSERT_TRUE(no_window._is_whole_partition_frame);
    ASSERT_TRUE(no_window._need_partition_materializing);

    TAnalyticWindow window;
    window.__set_type(TAnalyticWindowType::ROWS);
    plan_node.analytic_node.__set_window(window);
    Analytor unbounded_rows(plan_node, nullptr, false);
    ASSERT_TRUE(unbounded_rows._is_whole_partition_frame);

    TAnalyticWindowBoundary window_start;
    window_start.__set_type(TAnalyticWindowBoundaryType::CURRENT_ROW);
    plan_node.analytic_node.window.__set_window_start(window_start);
    TAnalyticWindowBoundary window_end;
    window_end.__set_type(TAnalyticWindowBoundaryType::CURRENT_ROW);
    plan_node.analytic_node.window.__set_window_end(window_end);
    Analytor bounded(plan_node, nullptr, false);
    ASSERT_FALSE(bounded._is_whole_partition_frame);
}

} // namespace starrocks
