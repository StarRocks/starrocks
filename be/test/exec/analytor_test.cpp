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
#include "common/config_exec_flow_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {
class AnalytorTest : public ::testing::Test {
public:
    void SetUp() override { config::vector_chunk_size = 1024; }
};

namespace {

TPlanNode make_rows_window_plan_node(TAnalyticWindowBoundaryType::type start_type, int64_t start_offset,
                                     TAnalyticWindowBoundaryType::type end_type, int64_t end_offset) {
    TAnalyticWindowBoundary start;
    start.type = start_type;
    if (start_type != TAnalyticWindowBoundaryType::CURRENT_ROW) {
        start.__set_rows_offset_value(start_offset);
    }
    TAnalyticWindowBoundary end;
    end.type = end_type;
    if (end_type != TAnalyticWindowBoundaryType::CURRENT_ROW) {
        end.__set_rows_offset_value(end_offset);
    }

    TAnalyticWindow window;
    window.type = TAnalyticWindowType::ROWS;
    window.__set_window_start(start);
    window.__set_window_end(end);

    TPlanNode plan_node;
    plan_node.analytic_node.__set_window(window);
    return plan_node;
}

// _remove_unused_rows() updates a few profile counters before it touches the buffered columns, so a bare
// Analytor needs them wired up before the removal path can be exercised.
void prepare_removal_counters(Analytor* analytor, RuntimeProfile* profile) {
    analytor->_peak_buffered_rows = ADD_PEAK_COUNTER(profile, "PeakBufferedRows", TUnit::UNIT);
    analytor->_remove_unused_rows_cnt = ADD_COUNTER(profile, "RemoveUnusedRowsCount", TUnit::UNIT);
    analytor->_remove_unused_total_rows = ADD_COUNTER(profile, "RemoveUnusedTotalRows", TUnit::UNIT);
    analytor->_column_resize_timer = ADD_TIMER(profile, "ColumnResizeTime");
}

// Fill in enough chunk positions that _remove_unused_rows() gets past its "not enough buffered chunks" guard,
// and return the position the removal would stop at.
int64_t fill_input_chunk_positions(Analytor* analytor, int64_t chunk_size) {
    const int64_t removable_chunk_num = config::pipeline_analytic_removable_chunk_num;
    const int64_t num_chunks = removable_chunk_num + 3;
    for (int64_t i = 0; i < num_chunks; i++) {
        analytor->_input_chunk_first_row_positions.emplace_back(i * chunk_size);
    }
    analytor->_input_rows = num_chunks * chunk_size;
    return removable_chunk_num * chunk_size;
}

} // namespace

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

// For `ROWS BETWEEN N FOLLOWING AND M FOLLOWING` the frame start runs ahead of the current row, so the
// frame start alone is not a safe lower bound of the rows that are still referenced: removing up to it
// would drop the current row as well and drive _current_row_position negative.
// NOLINTNEXTLINE
TEST_F(AnalytorTest, remove_unused_rows_keeps_current_row_of_following_frame) {
    constexpr int64_t kChunkSize = 4096;
    constexpr int64_t kStartOffset = 5000;

    TPlanNode plan_node = make_rows_window_plan_node(TAnalyticWindowBoundaryType::FOLLOWING, kStartOffset,
                                                     TAnalyticWindowBoundaryType::FOLLOWING, 15000);
    Analytor analytor(plan_node, nullptr, false);
    ASSERT_FALSE(analytor._need_partition_materializing);
    ASSERT_EQ(analytor._rows_start_offset, kStartOffset);

    RuntimeProfile profile("AnalytorTest");
    prepare_removal_counters(&analytor, &profile);
    const int64_t remove_end_position = fill_input_chunk_positions(&analytor, kChunkSize);

    // The evaluation cursor still lags behind the removal boundary, while the frame start is already past
    // it. Removing here used to leave _current_row_position at -2712.
    const int64_t current_row_position = remove_end_position - 2712;
    analytor._current_row_position = current_row_position;
    analytor._partition.end = analytor._input_rows;

    analytor._remove_unused_rows(nullptr);

    ASSERT_EQ(analytor._current_row_position, current_row_position);
    ASSERT_EQ(analytor._removed_from_buffer_rows, 0);
    ASSERT_EQ(analytor._removed_chunk_index, 0);
}

// The counterpart of the case above: for a frame that only looks backwards, the rows before the frame start
// must still be removed, otherwise the buffer would grow without bound.
// NOLINTNEXTLINE
TEST_F(AnalytorTest, remove_unused_rows_removes_rows_before_preceding_frame) {
    constexpr int64_t kChunkSize = 4096;

    TPlanNode plan_node = make_rows_window_plan_node(TAnalyticWindowBoundaryType::PRECEDING, 5000,
                                                     TAnalyticWindowBoundaryType::CURRENT_ROW, 0);
    Analytor analytor(plan_node, nullptr, false);
    ASSERT_EQ(analytor._rows_start_offset, -5000);

    RuntimeProfile profile("AnalytorTest");
    prepare_removal_counters(&analytor, &profile);
    const int64_t remove_end_position = fill_input_chunk_positions(&analytor, kChunkSize);

    const int64_t current_row_position = remove_end_position + 6000;
    analytor._current_row_position = current_row_position;
    analytor._partition.end = analytor._input_rows;

    analytor._remove_unused_rows(nullptr);

    ASSERT_EQ(analytor._removed_from_buffer_rows, remove_end_position);
    ASSERT_EQ(analytor._current_row_position, current_row_position - remove_end_position);
    ASSERT_EQ(analytor._removed_chunk_index, config::pipeline_analytic_removable_chunk_num);
}

} // namespace starrocks
