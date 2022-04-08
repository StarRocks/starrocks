// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/sorting/sorting.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column_helper.h"

namespace starrocks::vectorized {

TEST(MergeTest, merge_sorter_chunks_two_way) {
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
    ColumnPtr col1 = ColumnHelper::create_column(type_desc, false);
    ColumnPtr col2 = ColumnHelper::create_column(type_desc, false);
    ColumnPtr col3 = ColumnHelper::create_column(type_desc, false);
    ColumnPtr col4 = ColumnHelper::create_column(type_desc, false);
    std::vector<int32_t> elements_col1{1, 1, 1, 2, 2, 3, 4, 5, 6};
    std::vector<int32_t> elements_col2{1, 2, 2, 2, 3, 3, 6, 7, 8};
    std::vector<int32_t> elements_col3{1, 2, 2, 2, 3, 3, 6, 7, 8};
    std::vector<int32_t> elements_col4{2, 3, 4, 4, 4, 8, 9, 7, 11};

    for (int i = 0; i < elements_col1.size(); i++) {
        col1->append_datum(Datum(elements_col1[i]));
        col2->append_datum(Datum(elements_col2[i]));
        col3->append_datum(Datum(elements_col3[i]));
        col4->append_datum(Datum(elements_col4[i]));
    }
    Chunk::SlotHashMap map;
    map[0] = 0;
    map[1] = 1;

    ChunkPtr chunk1 = std::make_shared<Chunk>(Columns{col1, col3}, map);
    ChunkPtr chunk2 = std::make_shared<Chunk>(Columns{col2, col4}, map);
    Permutation perm;
    merge_sorted_chunks_two_way(chunk1.get(), chunk2.get(), &perm);

    size_t expected_size = col1->size() + col2->size();
    std::unique_ptr<Chunk> output = chunk1->clone_empty();
    append_by_permutation(output.get(), std::vector<ChunkPtr>{chunk1, chunk2}, perm);
    ASSERT_EQ(expected_size, perm.size());
    Int32Column* output_column1 = down_cast<Int32Column*>(output->get_column_by_index(0).get());
    Int32Column* output_column2 = down_cast<Int32Column*>(output->get_column_by_index(0).get());
    Int32Column::Container& data1 = output_column1->get_data();
    Int32Column::Container& data2 = output_column2->get_data();
    std::vector<std::tuple<int32_t, int32_t>> rows;
    for (int i = 0; i < data1.size(); i++) {
        rows.emplace_back(data1[i], data2[i]);
    }

    ASSERT_EQ(expected_size, output_column1->size());
    ASSERT_EQ(expected_size, data1.size());
    ASSERT_TRUE(std::is_sorted(rows.begin(), rows.end(),
                               [](auto x, auto y) {
                                   if (std::get<0>(x) != std::get<0>(y)) {
                                       return std::get<0>(x) < std::get<0>(y);
                                   }
                                   return std::get<1>(x) < std::get<1>(y);
                               }))
            << "merged data: " << fmt::format("{}", fmt::join(data1, ", "));
}

} // namespace starrocks::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
