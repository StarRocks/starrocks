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

#include <algorithm>
#include <cstdlib>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"

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

class FirstLastValueStreamingTest : public ::testing::Test {
protected:
    using Value = std::optional<int32_t>;

    void SetUp() override {
        _old_streaming = config::pipeline_analytic_enable_streaming_process;
        _old_ignore_nulls_streaming = config::pipeline_analytic_enable_ignore_nulls_streaming;
        _old_removable_chunks = config::pipeline_analytic_removable_chunk_num;
        config::pipeline_analytic_enable_streaming_process = true;
        config::pipeline_analytic_removable_chunk_num = 2;
    }

    void TearDown() override {
        config::pipeline_analytic_enable_streaming_process = _old_streaming;
        config::pipeline_analytic_enable_ignore_nulls_streaming = _old_ignore_nulls_streaming;
        config::pipeline_analytic_removable_chunk_num = _old_removable_chunks;
    }

    static TAnalyticWindow rows_frame(std::optional<int64_t> start, std::optional<int64_t> end) {
        TAnalyticWindow window;
        window.__set_type(TAnalyticWindowType::ROWS);
        auto boundary = [](int64_t offset) {
            TAnalyticWindowBoundary result;
            result.__set_type(offset < 0 ? TAnalyticWindowBoundaryType::PRECEDING
                                         : offset > 0 ? TAnalyticWindowBoundaryType::FOLLOWING
                                                      : TAnalyticWindowBoundaryType::CURRENT_ROW);
            if (offset != 0) result.__set_rows_offset_value(std::abs(offset));
            return result;
        };
        if (start) window.__set_window_start(boundary(*start));
        if (end) window.__set_window_end(boundary(*end));
        return window;
    }

    // Evaluate the SQL frame independently, including clipping at partition boundaries.
    static std::vector<Value> reference(const std::vector<Value>& input, const std::vector<int32_t>& keys,
                                        std::optional<int64_t> start, std::optional<int64_t> end, bool first,
                                        bool ignore_nulls) {
        std::vector<Value> result(input.size());
        int64_t partition_start = 0;
        while (partition_start < input.size()) {
            int64_t partition_end = partition_start + 1;
            while (partition_end < input.size() && keys[partition_end] == keys[partition_start]) ++partition_end;
            for (int64_t row = partition_start; row < partition_end; ++row) {
                const int64_t begin = start ? std::max(partition_start, row + *start) : partition_start;
                const int64_t finish = end ? std::min(partition_end, row + *end + 1) : partition_end;
                for (int64_t pos = first ? begin : finish - 1; pos >= begin && pos < finish; pos += first ? 1 : -1) {
                    if (!ignore_nulls || input[pos]) {
                        result[row] = input[pos];
                        break;
                    }
                }
            }
            partition_start = partition_end;
        }
        return result;
    }

    void run(const std::vector<Value>& input, const std::vector<int32_t>& keys, const TAnalyticWindow& window,
             bool first, bool ignore_nulls, bool streaming, const std::vector<Value>& expected,
             const std::string& expected_mode, int64_t chunk_rows = 7, bool bounded_memory = false,
             bool order_by_value = false) {
        config::pipeline_analytic_enable_ignore_nulls_streaming = streaming;
        ObjectPool pool;
        RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
        state.init_instance_mem_tracker();
        TDescriptorTableBuilder descriptors;
        TTupleDescriptorBuilder input_tuple;
        input_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).build());
        input_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).build());
        input_tuple.build(&descriptors);
        TTupleDescriptorBuilder output_tuple;
        output_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).build());
        output_tuple.build(&descriptors);
        DescriptorTbl* desc_tbl = nullptr;
        ASSERT_OK(DescriptorTbl::create(&state, &pool, descriptors.desc_tbl(), &desc_tbl, config::vector_chunk_size));
        state.set_desc_tbl(desc_tbl);
        const auto* in_tuple = desc_tbl->get_tuple_descriptor(0);
        const auto* out_tuple = desc_tbl->get_tuple_descriptor(1);
        const SlotId value_slot = in_tuple->slots()[0]->id();
        const SlotId key_slot = in_tuple->slots()[1]->id();
        const SlotId result_slot = out_tuple->slots()[0]->id();
        const auto int_type = TypeDescriptor(TYPE_INT).to_thrift();
        auto slot_ref = [&](SlotId slot) {
            TExprNode node;
            node.__set_node_type(TExprNodeType::SLOT_REF);
            node.__set_type(int_type);
            node.__set_num_children(0);
            node.__set_is_nullable(true);
            TSlotRef ref;
            ref.__set_tuple_id(0);
            ref.__set_slot_id(slot);
            node.__set_slot_ref(ref);
            return node;
        };
        TFunctionName name;
        name.__set_function_name(first ? "first_value" : "last_value");
        TFunction fn;
        fn.__set_name(name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        fn.__set_arg_types(std::vector<TTypeDesc>{int_type});
        fn.__set_ret_type(int_type);
        fn.__set_ignore_nulls(ignore_nulls);
        TExprNode agg;
        agg.__set_node_type(TExprNodeType::AGG_EXPR);
        agg.__set_type(int_type);
        agg.__set_num_children(1);
        agg.__set_is_nullable(true);
        agg.__set_has_nullable_child(true);
        agg.__set_fn(fn);
        TAggregateExpr aggregate_expr;
        aggregate_expr.__set_is_merge_agg(false);
        agg.__set_agg_expr(aggregate_expr);
        TExpr call;
        call.nodes = {agg, slot_ref(value_slot)};
        TExpr partition_expr;
        partition_expr.nodes = {slot_ref(key_slot)};
        TAnalyticNode node;
        node.__set_window(window);
        node.__set_buffered_tuple_id(0);
        node.__set_analytic_functions(std::vector<TExpr>{call});
        node.__set_partition_exprs(std::vector<TExpr>{partition_expr});
        if (window.type == TAnalyticWindowType::RANGE) {
            TExpr order_expr;
            order_expr.nodes = {slot_ref(order_by_value ? value_slot : key_slot)};
            node.__set_order_by_exprs(std::vector<TExpr>{order_expr});
        }
        TPlanNode plan;
        plan.__set_node_id(0);
        plan.__set_node_type(TPlanNodeType::ANALYTIC_EVAL_NODE);
        plan.__set_limit(-1);
        plan.__set_analytic_node(node);
        RuntimeProfile profile("FirstLastValueStreamingTest");
        Analytor analytor(plan, out_tuple, false);
        ASSERT_OK(analytor.prepare(&state, &pool, &profile));
        ASSERT_EQ(profile.get_info_string("ProcessMode"), std::optional<std::string>(expected_mode));
        ASSERT_OK(analytor.open(&state));
        std::vector<Value> result;
        auto drain = [&]() {
            while (ChunkPtr out = analytor.poll_chunk_buffer()) {
                const auto& col = out->get_column_by_slot_id(result_slot);
                for (size_t i = 0; i < col->size(); ++i) {
                    result.emplace_back(col->is_null(i) ? Value{} : Value{col->get(i).get_int32()});
                }
            }
        };
        for (size_t begin = 0; begin < input.size(); begin += chunk_rows) {
            auto values = NullableColumn::create(Int32Column::create(), NullColumn::create());
            auto partitions = NullableColumn::create(Int32Column::create(), NullColumn::create());
            const size_t end = std::min(input.size(), begin + chunk_rows);
            for (size_t i = begin; i < end; ++i) {
                if (input[i])
                    values->append_datum(Datum(*input[i]));
                else
                    values->append_nulls(1);
                partitions->append_datum(Datum(keys[i]));
            }
            auto chunk = std::make_shared<Chunk>();
            chunk->append_column(values, value_slot);
            chunk->append_column(partitions, key_slot);
            ASSERT_OK(analytor.process(&state, chunk));
            drain();
        }
        const size_t output_before_finish = result.size();
        ASSERT_OK(analytor.finish_process(&state));
        drain();
        ASSERT_EQ(result, expected);
        if (bounded_memory) {
            EXPECT_GT(output_before_finish, 0);
            EXPECT_GT(profile.get_counter("RemoveUnusedRowsCount")->value(), 0);
            EXPECT_LT(profile.get_counter("PeakBufferedRows")->value(), 8 * chunk_rows);
        }
        const bool waits_for_partition = expected_mode.find("Materializing/") == 0;
        const bool single_peer_group = window.type == TAnalyticWindowType::RANGE && !order_by_value;
        if ((waits_for_partition || single_peer_group) && keys.front() == keys.back()) {
            EXPECT_EQ(output_before_finish, 0);
            EXPECT_EQ(profile.get_counter("PeakBufferedRows")->value(), input.size());
        }
        analytor.close(&state);
    }

private:
    bool _old_streaming;
    bool _old_ignore_nulls_streaming;
    int32_t _old_removable_chunks;
};

TEST_F(FirstLastValueStreamingTest, RowsFramesMatchReferenceAndMaterializing) {
    const std::vector<std::pair<std::optional<int64_t>, std::optional<int64_t>>> frames = {
            {std::nullopt, 0}, {std::nullopt, -3}, {std::nullopt, 3}, {-4, -2}, {-3, 2}, {2, 4}, {0, 0}};
    std::vector<Value> input(300);
    std::vector<int32_t> keys(input.size());
    for (size_t i = 0; i < input.size(); ++i) {
        keys[i] = i < 101 ? 0 : i < 190 ? 1 : 2;
        // Include an entirely NULL partition and NULL runs across chunk and eviction boundaries.
        if (keys[i] != 1 && i % 13 == 8) input[i] = i;
    }
    for (bool first : {true, false}) {
        for (const auto& [start, end] : frames) {
            for (int64_t chunk_rows : {1, 7, 64}) {
                SCOPED_TRACE(::testing::Message() << "first=" << first << " start=" << start.value_or(-999)
                                                  << " end=" << *end << " chunk=" << chunk_rows);
                const auto expected = reference(input, keys, start, end, first, true);
                const std::string suffix = start ? "ByDefinition" : "Cumulative";
                run(input, keys, rows_frame(start, end), first, true, false, expected, "Materializing/" + suffix,
                    chunk_rows);
                run(input, keys, rows_frame(start, end), first, true, true, expected, "Streaming/" + suffix,
                    chunk_rows);
            }
        }
    }
}

TEST_F(FirstLastValueStreamingTest, CumulativeRowsEvictEvenAcrossLongNullRuns) {
    for (bool first : {true, false}) {
        for (int pattern = 0; pattern < 4; ++pattern) {
            std::vector<Value> input(4096);
            std::vector<int32_t> keys(input.size(), 0);
            for (size_t i = 0; i < input.size(); ++i) {
                if ((pattern == 1 && i == 0) || (pattern == 2 && i + 1 == input.size()) ||
                    (pattern == 3 && i % 31 == 0))
                    input[i] = i;
            }
            SCOPED_TRACE(::testing::Message() << "first=" << first << " pattern=" << pattern);
            const auto expected = reference(input, keys, std::nullopt, 0, first, true);
            run(input, keys, rows_frame(std::nullopt, 0), first, true, false, expected, "Materializing/Cumulative", 32);
            run(input, keys, rows_frame(std::nullopt, 0), first, true, true, expected, "Streaming/Cumulative", 32,
                true);
        }
    }
}

TEST_F(FirstLastValueStreamingTest, RangePeersAndUnboundedFramesKeepRequiredMaterialization) {
    const std::vector<Value> input = {std::nullopt, 10, std::nullopt, 20, std::nullopt, std::nullopt};
    const std::vector<int32_t> keys(input.size(), 0);
    for (bool first : {true, false}) {
        const auto expected = reference(input, keys, std::nullopt, std::nullopt, first, true);
        run(input, keys, rows_frame(std::nullopt, std::nullopt), first, true, true, expected,
            "Materializing/Cumulative");
        auto range = rows_frame(std::nullopt, 0);
        range.__set_type(TAnalyticWindowType::RANGE);
        // All ORDER BY values are peers, so the peer group cannot stream before end of input.
        run(input, keys, range, first, true, true, expected, "Streaming/Cumulative", 2);
        range.__set_window_start(range.window_end);
        run(input, keys, range, first, true, true, expected, "Materializing/ByDefinition", 2);
    }
}

TEST_F(FirstLastValueStreamingTest, CumulativeRangeStreamsAcrossPeerGroups) {
    std::vector<Value> input(4096);
    const std::vector<int32_t> keys(input.size(), 0);
    for (size_t i = 19; i < input.size(); ++i) input[i] = static_cast<int32_t>((i - 19) / 11);
    auto range = rows_frame(std::nullopt, 0);
    range.__set_type(TAnalyticWindowType::RANGE);
    for (bool first : {true, false}) {
        std::vector<Value> expected(input.size());
        for (size_t i = 19; i < input.size(); ++i) expected[i] = first ? Value{0} : input[i];
        // Nullable ORDER BY values start with one NULL peer group; subsequent peers cross chunk boundaries.
        run(input, keys, range, first, true, false, expected, "Materializing/Cumulative", 32, false, true);
        run(input, keys, range, first, true, true, expected, "Streaming/Cumulative", 32, true, true);
    }
}

TEST_F(FirstLastValueStreamingTest, GlobalDisableAndRespectNulls) {
    const std::vector<Value> input = {std::nullopt, 10, std::nullopt, 20, std::nullopt};
    const std::vector<int32_t> keys(input.size(), 0);
    for (bool first : {true, false}) {
        config::pipeline_analytic_enable_streaming_process = false;
        run(input, keys, rows_frame(std::nullopt, 0), first, true, true,
            reference(input, keys, std::nullopt, 0, first, true), "Materializing/Cumulative", 2);
        config::pipeline_analytic_enable_streaming_process = true;
        run(input, keys, rows_frame(std::nullopt, 0), first, false, false,
            reference(input, keys, std::nullopt, 0, first, false), "Streaming/Cumulative", 2);
    }
}

} // namespace starrocks
