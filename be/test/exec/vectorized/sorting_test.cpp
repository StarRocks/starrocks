// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/sorting/sorting.h"

#include <gtest/gtest.h>

#include <random>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exprs/vectorized/column_ref.h"
#include "runtime/vectorized/chunk_cursor.h"

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
    merge_sorted_chunks_two_way(sort_desc, left_chunk, right_chunk, &perm);

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

} // namespace starrocks::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
