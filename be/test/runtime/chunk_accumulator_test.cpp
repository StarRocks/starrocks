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

#include "runtime/chunk_accumulator.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "gtest/gtest.h"

namespace starrocks {

namespace {

ChunkPtr make_chunk(size_t num_rows) {
    auto chunk = std::make_shared<Chunk>();
    auto column = Int32Column::create();
    column->append_default(num_rows);
    chunk->append_column(std::move(column), 0);
    return chunk;
}

// A chunk with an INT column (slot 0) and a VARCHAR/binary column (slot 1).
ChunkPtr make_kv_chunk(const std::vector<std::pair<int32_t, std::string>>& rows) {
    auto chunk = std::make_shared<Chunk>();
    auto id_col = Int32Column::create();
    auto val_col = BinaryColumn::create();
    for (const auto& [id, val] : rows) {
        id_col->append_datum(id);
        val_col->append_datum(Slice(val));
    }
    chunk->append_column(std::move(id_col), 0);
    chunk->append_column(std::move(val_col), 1);
    return chunk;
}

} // namespace

TEST(ChunkAccumulatorTest, Accumulator) {
    constexpr size_t kDesiredSize = 4096;
    ChunkAccumulator accumulator(kDesiredSize);
    size_t input_rows = 0;
    size_t output_rows = 0;

    // push small chunks
    for (int i = 0; i < 10; i++) {
        auto chunk = make_chunk(1025);
        input_rows += 1025;

        static_cast<void>(accumulator.push(std::move(chunk)));
        if (ChunkPtr output = accumulator.pull()) {
            output_rows += output->num_rows();
            EXPECT_EQ(kDesiredSize, output->num_rows());
        }
    }

    // push large chunks
    for (int i = 0; i < 10; i++) {
        auto chunk = make_chunk(8888);
        input_rows += 8888;
        static_cast<void>(accumulator.push(std::move(chunk)));
    }

    accumulator.finalize();
    while (ChunkPtr output = accumulator.pull()) {
        EXPECT_LE(output->num_rows(), kDesiredSize);
        output_rows += output->num_rows();
    }
    EXPECT_EQ(input_rows, output_rows);

    // push empty chunks
    for (int i = 0; i < ChunkAccumulator::kAccumulateLimit; i++) {
        auto chunk = make_chunk(0);
        static_cast<void>(accumulator.push(std::move(chunk)));
    }
    EXPECT_TRUE(accumulator.reach_limit());
    auto output = accumulator.pull();
    EXPECT_EQ(nullptr, output);
    EXPECT_TRUE(accumulator.reach_limit());
}

// With a nonzero pre-append byte limit the accumulator emits the buffered chunk instead of merging
// an append that would cross the limit, even when the row count is still below the desired size.
TEST(ChunkAccumulatorTest, FlushesBeforeByteLimit) {
    constexpr size_t kDesiredSize = 3;

    auto first_input = make_kv_chunk({{1, "aaaa"}});
    auto second_input = make_kv_chunk({{2, "bbbb"}, {3, "cccc"}});

    ChunkAccumulator accumulator(kDesiredSize);
    // Exactly the combined size of the append that would otherwise build a three-row chunk, so the
    // second push must flush the buffered row rather than merge past the limit.
    accumulator.set_pre_append_byte_limit(first_input->bytes_usage() + second_input->bytes_usage());

    ASSERT_TRUE(accumulator.push(std::move(first_input)).ok());
    EXPECT_EQ(nullptr, accumulator.pull());

    ASSERT_TRUE(accumulator.push(std::move(second_input)).ok());
    auto first_output = accumulator.pull();
    ASSERT_NE(nullptr, first_output);
    ASSERT_EQ(1, first_output->num_rows());
    EXPECT_EQ(1, first_output->get_column_by_index(0)->get(0).get_int32());
    EXPECT_EQ("aaaa", first_output->get_column_by_index(1)->get(0).get_slice().to_string());

    accumulator.finalize();
    auto second_output = accumulator.pull();
    ASSERT_NE(nullptr, second_output);
    ASSERT_EQ(2, second_output->num_rows());
    EXPECT_EQ(2, second_output->get_column_by_index(0)->get(0).get_int32());
    EXPECT_EQ("bbbb", second_output->get_column_by_index(1)->get(0).get_slice().to_string());
    EXPECT_EQ(3, second_output->get_column_by_index(0)->get(1).get_int32());
    EXPECT_EQ("cccc", second_output->get_column_by_index(1)->get(1).get_slice().to_string());
    EXPECT_EQ(nullptr, accumulator.pull());
}

} // namespace starrocks
