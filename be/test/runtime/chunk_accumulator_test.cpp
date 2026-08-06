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

#include <cstdint>
#include <memory>
#include <random>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "gtest/gtest.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {

ChunkPtr make_chunk(size_t num_rows) {
    auto chunk = std::make_shared<Chunk>();
    auto column = Int32Column::create();
    column->append_default(num_rows);
    chunk->append_column(std::move(column), 0);
    return chunk;
}

ChunkPtr make_plain_json_chunk(size_t num_rows) {
    auto chunk = std::make_shared<Chunk>();
    auto column = JsonColumn::create();
    column->append_default(num_rows);
    chunk->append_column(std::move(column), 0);
    return chunk;
}

ChunkPtr make_flat_json_chunk(size_t num_rows) {
    auto chunk = std::make_shared<Chunk>();
    auto column = JsonColumn::create();
    std::vector<std::string> paths = {"a"};
    std::vector<LogicalType> types = {TYPE_BIGINT};
    MutableColumns flat_columns;
    flat_columns.emplace_back(Int64Column::create());
    column->set_flat_columns(paths, types, std::move(flat_columns));
    column->append_default(num_rows);
    chunk->append_column(std::move(column), 0);
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

// memory_usage() is capacity-based (Column::container_memory_usage counts capacity, and
// _tmp_chunk is created with clone_empty(_desired_size) which reserves the full desired
// size), so the tests below only assert relations: monotonicity, non-zero, and drain-to-zero.

TEST(ChunkAccumulatorTest, MemoryUsageEmpty) {
    ChunkAccumulator accumulator(4096);
    EXPECT_EQ(0u, accumulator.memory_usage());
}

TEST(ChunkAccumulatorTest, MemoryUsageMonotonicOnPush) {
    constexpr size_t kDesiredSize = 4096;
    ChunkAccumulator accumulator(kDesiredSize);
    size_t pushed_rows = 0;
    size_t prev_usage = 0;
    for (int i = 0; i < 20; i++) {
        ASSERT_TRUE(accumulator.push(make_chunk(1025)).ok());
        pushed_rows += 1025;
        size_t usage = accumulator.memory_usage();
        // Capacity accounting may keep the value flat while _tmp_chunk fills up,
        // but without a pull it must never decrease.
        EXPECT_GE(usage, prev_usage);
        EXPECT_GE(usage, pushed_rows * sizeof(int32_t));
        prev_usage = usage;
    }
}

TEST(ChunkAccumulatorTest, MemoryUsageDrainsToZero) {
    constexpr size_t kDesiredSize = 4096;
    ChunkAccumulator accumulator(kDesiredSize);
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(accumulator.push(make_chunk(1025)).ok());
    }
    EXPECT_GT(accumulator.memory_usage(), 0u);

    accumulator.finalize();
    while (ChunkPtr output = accumulator.pull()) {
    }
    EXPECT_EQ(0u, accumulator.memory_usage());
}

TEST(ChunkAccumulatorTest, MemoryUsageDrainsToZeroWithoutFinalize) {
    constexpr size_t kDesiredSize = 4096;
    ChunkAccumulator accumulator(kDesiredSize);
    // 3 * 3000 = 9000 rows: two full output chunks plus 808 rows left in _tmp_chunk
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(accumulator.push(make_chunk(3000)).ok());
    }
    while (ChunkPtr output = accumulator.pull()) {
    }
    // _tmp_chunk still holds the tail rows
    EXPECT_GT(accumulator.memory_usage(), 0u);

    accumulator.finalize();
    while (ChunkPtr output = accumulator.pull()) {
    }
    EXPECT_EQ(0u, accumulator.memory_usage());
}

TEST(ChunkAccumulatorTest, MemoryUsageResetClears) {
    constexpr size_t kDesiredSize = 4096;
    ChunkAccumulator accumulator(kDesiredSize);
    for (int i = 0; i < 5; i++) {
        ASSERT_TRUE(accumulator.push(make_chunk(3000)).ok());
    }
    EXPECT_GT(accumulator.memory_usage(), 0u);

    accumulator.reset();
    EXPECT_EQ(0u, accumulator.memory_usage());
}

TEST(ChunkAccumulatorTest, MemoryUsageLargeChunkSplit) {
    constexpr size_t kDesiredSize = 4096;
    constexpr size_t kInputRows = kDesiredSize * 3 + 7;
    ChunkAccumulator accumulator(kDesiredSize);
    ASSERT_TRUE(accumulator.push(make_chunk(kInputRows)).ok());
    EXPECT_GT(accumulator.memory_usage(), 0u);

    accumulator.finalize();
    size_t pulled_rows = 0;
    while (ChunkPtr output = accumulator.pull()) {
        pulled_rows += output->num_rows();
    }
    EXPECT_EQ(kInputRows, pulled_rows);
    EXPECT_EQ(0u, accumulator.memory_usage());
}

TEST(ChunkAccumulatorTest, MemoryUsageIncludesTmpChunk) {
    constexpr size_t kDesiredSize = 4096;
    ChunkAccumulator accumulator(kDesiredSize);
    ASSERT_TRUE(accumulator.push(make_chunk(100)).ok());
    // All the data lives in _tmp_chunk: _output is still empty
    EXPECT_TRUE(accumulator.empty());
    EXPECT_GT(accumulator.memory_usage(), 0u);
}

TEST(ChunkAccumulatorTest, MemoryUsageJsonSchemaMismatchFlush) {
    // A JSON schema mismatch makes push() flush the half-full _tmp_chunk into _output
    // through a dedicated branch (the fifth _memory_usage maintenance point), which the
    // Int32-only tests can never reach.
    constexpr size_t kDesiredSize = 4096;
    ChunkAccumulator accumulator(kDesiredSize);
    ASSERT_TRUE(accumulator.push(make_plain_json_chunk(100)).ok());
    // Plain vs flat JSON is schema-incompatible: the 100-row _tmp_chunk is flushed
    // to _output even though it never reached _desired_size
    ASSERT_TRUE(accumulator.push(make_flat_json_chunk(50)).ok());
    EXPECT_FALSE(accumulator.empty());

    ChunkPtr flushed = accumulator.pull();
    ASSERT_TRUE(flushed != nullptr);
    EXPECT_EQ(100u, flushed->num_rows());

    accumulator.finalize();
    size_t pulled_rows = flushed->num_rows();
    while (ChunkPtr output = accumulator.pull()) {
        pulled_rows += output->num_rows();
    }
    EXPECT_EQ(150u, pulled_rows);
    // A missed increment in the mismatch branch would underflow here instead of reaching zero
    EXPECT_EQ(0u, accumulator.memory_usage());
}

TEST(ChunkAccumulatorTest, MemoryUsageNoUnderflowOnRepeatedCycles) {
    constexpr size_t kDesiredSize = 4096;
    ChunkAccumulator accumulator(kDesiredSize);
    std::mt19937 rng(12345);
    std::uniform_int_distribution<size_t> dist(1, kDesiredSize * 2);
    for (int i = 0; i < 20; i++) {
        ASSERT_TRUE(accumulator.push(make_chunk(dist(rng))).ok());
        while (ChunkPtr output = accumulator.pull()) {
        }
        // An unbalanced decrement would wrap around to an astronomic value
        EXPECT_LT(accumulator.memory_usage(), SIZE_MAX / 2);
    }

    accumulator.finalize();
    while (ChunkPtr output = accumulator.pull()) {
    }
    EXPECT_EQ(0u, accumulator.memory_usage());
}

} // namespace starrocks
