// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/sorting/sorting.h"

#include <gtest/gtest.h>

#include <random>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/vectorized/sorting/merge.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exprs/vectorized/column_ref.h"
#include "runtime/chunk_cursor.h"
#include "util/defer_op.h"

namespace starrocks::vectorized {

class MergeTestFixture : public testing::TestWithParam<std::vector<std::vector<int32_t>>> {};

TEST_P(MergeTestFixture, merge_sorter_chunks_two_way) {
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
    std::vector<std::vector<int32_t>> params = GetParam();
    int total_columns = params.size();
    ASSERT_TRUE(total_columns % 2 == 0);
    Columns left_columns;
    Columns right_columns;

    Chunk::SlotHashMap map;
    int left_rows = 0, right_rows = 0;
    int num_columns = total_columns / 2;
    for (int i = 0; i < total_columns; i++) {
        ColumnPtr col = ColumnHelper::create_column(type_desc, false);
        auto& data = params[i];
        for (int j = 0; j < data.size(); j++) {
            col->append_datum(Datum(data[j]));
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
    ChunkPtr left_chunk = std::make_shared<Chunk>(left_columns, map);
    ChunkPtr right_chunk = std::make_shared<Chunk>(right_columns, map);
    Permutation perm;
    SortDescs sort_desc(std::vector<int>(num_columns, 1), std::vector<int>(num_columns, -1));
    merge_sorted_chunks_two_way(sort_desc, {left_chunk, left_chunk->columns()}, {right_chunk, right_chunk->columns()},
                                &perm);

    size_t expected_size = left_rows + right_rows;
    std::unique_ptr<Chunk> output = left_chunk->clone_empty();
    append_by_permutation(output.get(), std::vector<ChunkPtr>{left_chunk, right_chunk}, perm);
    ASSERT_EQ(expected_size, perm.size());

    std::vector<std::vector<int>> output_data;
    for (int i = 0; i < output->num_rows(); i++) {
        std::vector<int> row;
        for (int j = 0; j < num_columns; j++) {
            Column* output_col = output->get_column_by_index(j).get();
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

// clang-format off
INSTANTIATE_TEST_SUITE_P(
        MergeTest, MergeTestFixture,
        testing::Values(
        std::vector<std::vector<int32_t>>{
            {1, 2, 3},
            {1, 2, 3}
        },
        std::vector<std::vector<int32_t>>{
            {1, 2, 3},
            {4, 5, 6}
        },
        std::vector<std::vector<int32_t>>{
            {4, 5, 6},
            {1, 2, 3}
        },
        std::vector<std::vector<int32_t>>{
            {},
            {1, 2, 3}
        },
        std::vector<std::vector<int32_t>>{
            {1, 2, 3},
            {}
        },
        std::vector<std::vector<int32_t>>{
            {1, 3, 5},
            {2, 4}
        },
        std::vector<std::vector<int32_t>>{
            {1, 3, 5},
            {3, 4, 5}
        },
        std::vector<std::vector<int32_t>>{
            {1, 2, 2},
            {2, 2, 3}
        },
        std::vector<std::vector<int32_t>>{
            {2, 2, 2},
            {2, 2, 3}
        },
        std::vector<std::vector<int32_t>>{
                std::vector<int32_t>{1, 1, 1, 2, 2, 3, 4, 5, 6}, 
                std::vector<int32_t>{1, 2, 2, 2, 3, 3, 6, 7, 8},
                std::vector<int32_t>{1, 2, 2, 2, 3, 3, 6, 7, 8}, 
                std::vector<int32_t>{2, 3, 4, 4, 4, 8, 9, 7, 11}

        }
));
// clang-format on

static ColumnPtr build_sorted_column(TypeDescriptor type_desc, int slot_index, int32_t start, int32_t count,
                                     int32_t step) {
    DCHECK_EQ(TYPE_INT, type_desc.type);

    ColumnPtr column = ColumnHelper::create_column(type_desc, false);
    for (int i = 0; i < count; i++) {
        column->append_datum(Datum(start + step * i));
    }
    return column;
}

TEST(SortingTest, append_by_permutation_binary) {
    BinaryColumn::Ptr input1 = BinaryColumn::create();
    BinaryColumn::Ptr input2 = BinaryColumn::create();
    input1->append_string("star");
    input2->append_string("rock");

    ColumnPtr merged = BinaryColumn::create();
    Permutation perm{{0, 0}, {1, 0}};
    append_by_permutation(merged.get(), {input1, input2}, perm);
    ASSERT_EQ(2, merged->size());
    ASSERT_EQ("star", merged->get(0).get_slice());
    ASSERT_EQ("rock", merged->get(1).get_slice());
}

TEST(SortingTest, append_by_permutation_int) {
    Int32Column::Ptr input1 = Int32Column::create();
    Int32Column::Ptr input2 = Int32Column::create();
    input1->append(1024);
    input2->append(2048);

    ColumnPtr merged = Int32Column::create();
    Permutation perm{{0, 0}, {1, 0}};
    append_by_permutation(merged.get(), {input1, input2}, perm);
    ASSERT_EQ(2, merged->size());
    ASSERT_EQ(1024, merged->get(0).get_int32());
    ASSERT_EQ(2048, merged->get(1).get_int32());
}

void clear_sort_exprs(std::vector<ExprContext*>& exprs) {
    for (ExprContext* ctx : exprs) {
        delete ctx;
    }
    exprs.clear();
}

TEST(MergeTest, merge_sorted_stream) {
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
    DeferOp defer([&]() { clear_sort_exprs(sort_exprs); });

    std::vector<ChunkProvider> chunk_providers;
    std::vector<int> chunk_probe_index(num_runs, 0);
    std::vector<int> chunk_run_max(num_runs, 0);
    for (int run = 0; run < num_runs; run++) {
        ChunkProvider chunk_probe_supplier = [&, run](Chunk** output, bool* eos) -> bool {
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
                ChunkUniquePtr chunk = std::make_unique<Chunk>(columns, map);
                *output = chunk.release();
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

} // namespace starrocks::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
