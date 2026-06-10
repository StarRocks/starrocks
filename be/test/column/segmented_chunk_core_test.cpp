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

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/chunk_slice.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/segmented_chunk.h"
#include "fmt/format.h"
#include "gtest/gtest.h"

namespace starrocks {

class SegmentedChunkCoreTest : public ::testing::Test {
protected:
    static SegmentedChunkPtr build_segmented_chunk() {
        auto segmented_chunk = SegmentedChunk::create(1 << 16);
        segmented_chunk->append_column(Int32Column::create(), 0);
        segmented_chunk->append_column(BinaryColumn::create(), 1);
        segmented_chunk->build_columns();

        int32_t row_id = 0;
        for (int chunk_idx = 0; chunk_idx < 100; chunk_idx++) {
            auto int_column = Int32Column::create();
            auto binary_column = BinaryColumn::create();
            int_column->reserve(4096);
            binary_column->reserve(4096);
            for (int row = 0; row < 4096; row++) {
                int_column->append(row_id++);
                binary_column->append(fmt::format("str{}", row_id));
            }

            auto chunk = std::make_shared<Chunk>();
            chunk->append_column(std::move(int_column), 0);
            chunk->append_column(std::move(binary_column), 1);
            segmented_chunk->append_chunk(std::move(chunk));
        }
        return segmented_chunk;
    }
};

TEST_F(SegmentedChunkCoreTest, cloneSelectiveAndReset) {
    auto segmented_chunk = build_segmented_chunk();

    ASSERT_TRUE(segmented_chunk->downgrade().ok());
    ASSERT_TRUE(segmented_chunk->upgrade_if_overflow().ok());

    EXPECT_EQ(409600, segmented_chunk->num_rows());
    EXPECT_EQ(7, segmented_chunk->num_segments());
    EXPECT_GT(segmented_chunk->memory_usage(), 0);
    EXPECT_EQ(2, segmented_chunk->columns().size());

    auto column0 = segmented_chunk->columns()[0];
    EXPECT_FALSE(column0->is_nullable());
    EXPECT_FALSE(column0->has_null());
    EXPECT_EQ(409600, column0->size());

    std::vector<uint32_t> indexes = {1, 2, 4, 10000, 20000};
    ColumnPtr cloned = column0->clone_selective(indexes.data(), 0, indexes.size());
    EXPECT_EQ("[1, 2, 4, 10000, 20000]", cloned->debug_string());

    const size_t memory_usage = segmented_chunk->memory_usage();
    segmented_chunk->reset();
    EXPECT_EQ(0, segmented_chunk->num_rows());
    EXPECT_EQ(7, segmented_chunk->num_segments());
    EXPECT_EQ(memory_usage, segmented_chunk->memory_usage());
}

TEST_F(SegmentedChunkCoreTest, sliceAndAppend) {
    auto segmented_chunk = build_segmented_chunk();
    SegmentedChunkSlice slice;
    slice.reset(segmented_chunk);

    size_t total_rows = 0;
    while (!slice.empty()) {
        auto chunk = slice.cutoff(1000);
        EXPECT_LE(chunk->num_rows(), 1000);
        const auto& slices = ColumnHelper::as_column<BinaryColumn>(chunk->get_column_by_index(1))->immutable_data();
        for (int i = 0; i < chunk->num_rows(); i++) {
            EXPECT_EQ(total_rows + i, chunk->get_column_by_index(0)->get(i).get_int32());
            EXPECT_EQ(fmt::format("str{}", total_rows + i + 1), slices[i].to_string());
        }
        total_rows += chunk->num_rows();
    }
    EXPECT_EQ(409600, total_rows);
    EXPECT_EQ(0, segmented_chunk->num_rows());
    EXPECT_EQ(7, segmented_chunk->num_segments());
    segmented_chunk->check_or_die();

    auto seg1 = build_segmented_chunk();
    auto seg2 = build_segmented_chunk();
    seg1->append(seg2, 1);
    EXPECT_EQ(409600 * 2 - 1, seg1->num_rows());
    seg1->check_or_die();

    std::vector<uint32_t> indexes = {1, 2, 4, 10000, 20000};
    auto column1 = seg1->columns()[1];
    ColumnPtr cloned = column1->clone_selective(indexes.data(), 0, indexes.size());
    EXPECT_EQ("['str2', 'str3', 'str5', 'str10001', 'str20001']", cloned->debug_string());
}

} // namespace starrocks
