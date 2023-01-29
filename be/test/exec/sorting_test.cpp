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

#include "exec/sorting/sorting.h"

#include <gtest/gtest.h>

#include <memory>
#include <random>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sort_permute.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "runtime/chunk_cursor.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "util/defer_op.h"

namespace starrocks {

static ColumnPtr build_sorted_column(const TypeDescriptor& type_desc, int slot_index, int32_t start, int32_t count,
                                     int32_t step) {
    DCHECK_EQ(TYPE_INT, type_desc.type);

    ColumnPtr column = ColumnHelper::create_column(type_desc, false);
    for (int i = 0; i < count; i++) {
        column->append_datum(Datum(start + step * i));
    }
    return column;
}

static void clear_exprs(std::vector<ExprContext*>& exprs) {
    for (ExprContext* ctx : exprs) {
        delete ctx;
    }
    exprs.clear();
}

static std::shared_ptr<RuntimeState> create_runtime_state() {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = config::vector_chunk_size;
    TQueryGlobals query_globals;
    auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
    runtime_state->init_instance_mem_tracker();
    return runtime_state;
}

using MergeParamType = std::tuple<std::vector<int>, std::vector<std::vector<int32_t>>>;
class MergeTestFixture : public testing::TestWithParam<MergeParamType> {};

TEST_P(MergeTestFixture, merge_sorter_chunks_two_way) {
    auto runtime_state = create_runtime_state();
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
    std::vector<int> sort_slots = std::get<0>(GetParam());
    std::vector<std::vector<int32_t>> sorting_data = std::get<1>(GetParam());
    int total_columns = sorting_data.size();
    ASSERT_TRUE(total_columns % 2 == 0);
    Columns left_columns;
    Columns right_columns;

    Chunk::SlotHashMap map;
    int left_rows = 0, right_rows = 0;
    int num_columns = sort_slots.empty() ? total_columns / 2 : sort_slots.size();
    if (sort_slots.empty()) {
        sort_slots.resize(num_columns);
        std::iota(sort_slots.begin(), sort_slots.end(), 0);
    }

    for (int i = 0; i < total_columns; i++) {
        ColumnPtr col = ColumnHelper::create_column(type_desc, false);
        auto& data = sorting_data[i];
        for (int j : data) {
            col->append_datum(Datum(j));
        }
        if (i < total_columns / 2) {
            left_rows = data.size();
            map[i] = i;
            left_columns.push_back(col);
        } else {
            right_rows = data.size();
            right_columns.push_back(col);
        }
    }
    auto left_chunk = std::make_unique<Chunk>(left_columns, map);
    auto right_chunk = std::make_unique<Chunk>(right_columns, map);
    Permutation perm;
    SortDescs sort_desc(std::vector<int>(num_columns, 1), std::vector<int>(num_columns, -1));

    std::vector<std::unique_ptr<ColumnRef>> exprs;
    std::vector<ExprContext*> sort_exprs;
    DeferOp defer([&]() { clear_exprs(sort_exprs); });
    for (int slot_index : sort_slots) {
        auto expr = std::make_unique<ColumnRef>(type_desc, slot_index);
        exprs.push_back(std::move(expr));
        sort_exprs.push_back(new ExprContext(exprs.back().get()));
    }

    ASSERT_OK(Expr::prepare(sort_exprs, runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, runtime_state.get()));

    size_t expected_size = left_rows + right_rows;
    ChunkPtr output;
    SortedRuns output_run;
    std::vector<ChunkUniquePtr> chunks;
    chunks.emplace_back(std::move(left_chunk));
    chunks.emplace_back(std::move(right_chunk));
    ASSERT_OK(merge_sorted_chunks(sort_desc, &sort_exprs, chunks, &output_run));
    output = output_run.assemble();
    ASSERT_EQ(expected_size, output->num_rows());

    std::vector<std::vector<int>> output_data;
    for (int i = 0; i < output->num_rows(); i++) {
        std::vector<int> row;
        for (int slot_index : sort_slots) {
            Column* output_col = output->get_column_by_index(slot_index).get();
            row.push_back(output_col->get(i).get_int32());
        }
        output_data.emplace_back(row);
    }
    auto output_string = [&]() {
        std::string str;
        for (auto& row : output_data) {
            str += fmt::format("({})", fmt::join(row, ","));
            str += ", ";
        }
        return str;
    };
    auto row_less = [&](const std::vector<int>& lhs, const std::vector<int>& rhs) {
        for (int i = 0; i < lhs.size(); i++) {
            if (lhs[i] != rhs[i]) {
                return lhs[i] <= rhs[i];
            }
        }
        return false;
    };

    ASSERT_EQ(expected_size, output->num_rows());
    ASSERT_TRUE(std::is_sorted(output_data.begin(), output_data.end(), row_less)) << "merged data: " << output_string();
}

INSTANTIATE_TEST_SUITE_P(
        MergeTest, MergeTestFixture,
        testing::Values(
                std::make_tuple(std::vector<int>(), std::vector<std::vector<int32_t>>{{1, 2, 3}, {1, 2, 3}}),
                std::make_tuple(std::vector<int>(), std::vector<std::vector<int32_t>>{{1, 2, 3}, {4, 5, 6}}),
                std::make_tuple(std::vector<int>(), std::vector<std::vector<int32_t>>{{4, 5, 6}, {1, 2, 3}}),
                std::make_tuple(std::vector<int>(), std::vector<std::vector<int32_t>>{{}, {1, 2, 3}}),
                std::make_tuple(std::vector<int>(), std::vector<std::vector<int32_t>>{{1, 2, 3}, {}}),
                std::make_tuple(std::vector<int>(), std::vector<std::vector<int32_t>>{{1, 3, 5}, {2, 4}}),
                std::make_tuple(std::vector<int>(), std::vector<std::vector<int32_t>>{{1, 3, 5}, {3, 4, 5}}),
                std::make_tuple(std::vector<int>(), std::vector<std::vector<int32_t>>{{1, 2, 2}, {2, 2, 3}}),
                std::make_tuple(std::vector<int>(), std::vector<std::vector<int32_t>>{{2, 2, 2}, {2, 2, 3}}),

                std::make_tuple(std::vector<int>(), std::vector<std::vector<int32_t>>{{1, 1, 1, 2, 2, 3, 4, 5, 6},
                                                                                      {1, 2, 2, 2, 3, 3, 6, 7, 8},
                                                                                      {1, 2, 2, 2, 3, 3, 6, 7, 8},
                                                                                      {2, 3, 4, 4, 4, 8, 9, 7, 11}}),

                std::make_tuple(std::vector<int>{0}, std::vector<std::vector<int32_t>>{{1, 1, 1, 2, 2, 3, 4, 5, 6},
                                                                                       {1, 2, 2, 2, 3, 3, 6, 7, 8},
                                                                                       {1, 2, 2, 2, 3, 3, 6, 7, 8},
                                                                                       {2, 3, 4, 4, 4, 7, 8, 9, 11}}),

                std::make_tuple(std::vector<int>{1}, std::vector<std::vector<int32_t>>{{9, 8, 7, 6, 2, 3, 4, 5, 6},
                                                                                       {1, 2, 2, 2, 3, 3, 6, 7, 8},
                                                                                       {10, 20, 9, 7, 3, 3, 6, 7, 8},
                                                                                       {2, 3, 4, 4, 4, 7, 8, 9, 11}})

                        ));

TEST(SortingTest, materialize_by_permutation_binary) {
    BinaryColumn::Ptr input1 = BinaryColumn::create();
    BinaryColumn::Ptr input2 = BinaryColumn::create();
    input1->append_string("star");
    input2->append_string("rock");

    ColumnPtr merged = BinaryColumn::create();
    Permutation perm{{0, 0}, {1, 0}};
    materialize_column_by_permutation(merged.get(), {input1, input2}, perm);
    ASSERT_EQ(2, merged->size());
    ASSERT_EQ("star", merged->get(0).get_slice());
    ASSERT_EQ("rock", merged->get(1).get_slice());
}

TEST(SortingTest, materialize_by_permutation_int) {
    Int32Column::Ptr input1 = Int32Column::create();
    Int32Column::Ptr input2 = Int32Column::create();
    input1->append(1024);
    input2->append(2048);

    ColumnPtr merged = Int32Column::create();
    Permutation perm{{0, 0}, {1, 0}};
    materialize_column_by_permutation(merged.get(), {input1, input2}, perm);
    ASSERT_EQ(2, merged->size());
    ASSERT_EQ(1024, merged->get(0).get_int32());
    ASSERT_EQ(2048, merged->get(1).get_int32());
}

TEST(SortingTest, steal_chunk) {
    ColumnPtr col1 = build_sorted_column(TypeDescriptor(TYPE_INT), 0, 0, 100, 1);
    ColumnPtr col2 = build_sorted_column(TypeDescriptor(TYPE_INT), 1, 0, 100, 1);
    Chunk::SlotHashMap slot_map{{0, 0}, {1, 1}};
    ChunkPtr chunk = std::make_shared<Chunk>(Columns{col1, col2}, slot_map);

    for (size_t chunk_size : std::vector<size_t>{1, 3, 4, 5, 7, 33, 101, 205}) {
        SortedRun run(chunk, chunk->columns());
        ChunkPtr sum = chunk->clone_empty();
        while (!run.empty()) {
            ChunkPtr stealed = run.steal_chunk(chunk_size);
            sum->append(*stealed);
        }
        ASSERT_EQ(chunk->num_rows(), sum->num_rows());
        ASSERT_EQ(chunk->num_columns(), sum->num_columns());
        ASSERT_TRUE(run.empty());
        ASSERT_TRUE(run.chunk == nullptr);
    }
}

TEST(SortingTest, sorted_runs) {
    ColumnPtr col1 = build_sorted_column(TypeDescriptor(TYPE_INT), 0, 0, 100, 1);
    ColumnPtr col2 = build_sorted_column(TypeDescriptor(TYPE_INT), 1, 0, 100, 1);
    Chunk::SlotHashMap slot_map{{0, 0}, {1, 1}};
    ChunkPtr chunk = std::make_shared<Chunk>(Columns{col1, col2}, slot_map);

    SortedRuns runs;
    runs.chunks.emplace_back(chunk, chunk->columns());
    runs.chunks.emplace_back(chunk, chunk->columns());

    ASSERT_EQ(2, runs.num_chunks());
    ASSERT_EQ(200, runs.num_rows());

    runs.resize(199);
    ASSERT_EQ(199, runs.num_rows());

    runs.resize(99);
    ASSERT_EQ(99, runs.num_rows());
    ASSERT_EQ(1, runs.num_chunks());

    ChunkPtr slice = runs.assemble();
    ASSERT_EQ(99, slice->num_rows());
}

TEST(SortingTest, merge_sorted_chunks) {
    auto runtime_state = create_runtime_state();
    std::vector<ChunkUniquePtr> input_chunks;
    Chunk::SlotHashMap slot_map{{0, 0}};

    std::vector<std::vector<int>> input_runs = {{-2074, -1691, -1400, -969, -767, -725},
                                                {-680, -571, -568},
                                                {-2118, -2065, -1328, -1103, -1099, -1093},
                                                {-950, -807, -604}};
    for (auto& input_numbers : input_runs) {
        ColumnPtr column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        for (int x : input_numbers) {
            column->append_datum(Datum((int32_t)x));
        }
        auto chunk = std::make_unique<Chunk>(Columns{column}, slot_map);
        input_chunks.emplace_back(std::move(chunk));
    }

    std::vector<std::unique_ptr<ColumnRef>> exprs;
    std::vector<ExprContext*> sort_exprs;
    exprs.push_back(std::make_unique<ColumnRef>(TypeDescriptor(TYPE_INT), 0));
    sort_exprs.push_back(new ExprContext(exprs.back().get()));
    ASSERT_OK(Expr::prepare(sort_exprs, runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, runtime_state.get()));

    DeferOp defer([&]() { clear_exprs(sort_exprs); });

    SortDescs sort_desc(std::vector<int>{1}, std::vector<int>{-1});
    SortedRuns output;
    merge_sorted_chunks(sort_desc, &sort_exprs, input_chunks, &output);
    ASSERT_TRUE(output.is_sorted(sort_desc));
}

TEST(SortingTest, merge_sorted_stream) {
    auto runtime_state = create_runtime_state();
    constexpr int num_columns = 3;
    constexpr int num_runs = 4;
    constexpr int num_chunks_per_run = 4;
    std::vector<std::unique_ptr<ColumnRef>> exprs;
    std::vector<ExprContext*> sort_exprs;
    std::vector<bool> asc_arr;
    std::vector<bool> null_first;
    Chunk::SlotHashMap map;
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
    SortDescs sort_desc(std::vector<int>{1, 1, 1}, std::vector<int>{-1, -1, -1});

    for (int i = 0; i < num_columns; i++) {
        auto expr = std::make_unique<ColumnRef>(type_desc, i);
        exprs.emplace_back(std::move(expr));
        sort_exprs.push_back(new ExprContext(exprs.back().get()));
        asc_arr.push_back(true);
        null_first.push_back(true);
        map[i] = i;
    }
    ASSERT_OK(Expr::prepare(sort_exprs, runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, runtime_state.get()));
    DeferOp defer([&]() { clear_exprs(sort_exprs); });

    std::vector<ChunkProvider> chunk_providers;
    std::vector<int> chunk_probe_index(num_runs, 0);
    std::vector<int> chunk_run_max(num_runs, 0);
    for (int run = 0; run < num_runs; run++) {
        ChunkProvider chunk_probe_supplier = [&, run](ChunkUniquePtr* output, bool* eos) -> bool {
            if (chunk_probe_index[run]++ > num_chunks_per_run) {
                *output = nullptr;
                *eos = true;
                return false;
            } else if (output && eos) {
                Columns columns;
                for (int col_idx = 0; col_idx < num_columns; col_idx++) {
                    auto column =
                            build_sorted_column(type_desc, col_idx, col_idx * 10 * chunk_probe_index[run], 10, col_idx);
                    columns.push_back(column);
                }
                *output = std::make_unique<Chunk>(columns, map);
            }
            return true;
        };
        chunk_providers.emplace_back(chunk_probe_supplier);
    }

    std::vector<std::unique_ptr<SimpleChunkSortCursor>> input_cursors;
    for (int run = 0; run < num_runs; run++) {
        input_cursors.push_back(std::make_unique<SimpleChunkSortCursor>(chunk_providers[run], &sort_exprs));
    }

    std::vector<ChunkUniquePtr> output_chunks;
    merge_sorted_cursor_cascade(sort_desc, std::move(input_cursors), [&](ChunkUniquePtr chunk) {
        output_chunks.push_back(std::move(chunk));
        return Status::OK();
    });

    for (auto& chunk : output_chunks) {
        for (int i = 0; i < chunk->num_rows(); i++) {
            fmt::print("row: {}\n", chunk->debug_row(i));
            if (i > 0) {
                int x = compare_chunk_row(sort_desc, chunk->columns(), chunk->columns(), i - 1, i);
                ASSERT_LE(x, 0);
            }
        }
    }
}

} // namespace starrocks
