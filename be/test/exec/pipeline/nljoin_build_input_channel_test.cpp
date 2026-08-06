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

#include <gtest/gtest.h>

#include <functional>
#include <memory>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/statusor.h"
#include "exec/pipeline/nljoin/nljoin_context.h"

namespace starrocks::pipeline {

namespace {

constexpr size_t kChunkSize = 4096;

ChunkPtr make_chunk(size_t num_rows) {
    auto chunk = std::make_shared<Chunk>();
    auto column = Int32Column::create();
    column->append_default(num_rows);
    chunk->append_column(std::move(column), 0);
    return chunk;
}

// Chunks must be non-null and non-empty: the iterator feeds Spiller::spill, which DCHECKs that.
size_t drain_rows(const std::function<StatusOr<ChunkPtr>()>& iter) {
    size_t rows = 0;
    while (true) {
        auto maybe_chunk = iter();
        if (!maybe_chunk.ok()) {
            EXPECT_TRUE(maybe_chunk.status().is_end_of_file());
            return rows;
        }
        ChunkPtr chunk = std::move(maybe_chunk.value());
        EXPECT_TRUE(chunk != nullptr);
        EXPECT_FALSE(chunk->is_empty());
        rows += chunk->num_rows();
    }
}

} // namespace

TEST(NJJoinBuildInputChannelTest, IteratorDrainsAllChunks) {
    NJJoinBuildInputChannel channel(kChunkSize);
    // 3 * 3000 = 9000 rows: two full 4096-row chunks, 808 left in the tail chunk
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(channel.add_chunk(make_chunk(3000)).ok());
    }

    size_t rows = drain_rows(channel.buffered_chunk_iterator(false));
    EXPECT_EQ(2 * kChunkSize, rows);

    // Without finalize the tail rows stay invisible: a fresh iterator is empty
    auto iter = channel.buffered_chunk_iterator(false);
    auto res = iter();
    ASSERT_FALSE(res.ok());
    EXPECT_TRUE(res.status().is_end_of_file());
}

TEST(NJJoinBuildInputChannelTest, IteratorFinalizeIncludesTmpChunk) {
    {
        NJJoinBuildInputChannel channel(kChunkSize);
        ASSERT_TRUE(channel.add_chunk(make_chunk(100)).ok());
        auto iter = channel.buffered_chunk_iterator(true);
        auto res = iter();
        ASSERT_TRUE(res.ok());
        EXPECT_EQ(100u, res.value()->num_rows());
        res = iter();
        ASSERT_FALSE(res.ok());
        EXPECT_TRUE(res.status().is_end_of_file());
    }
    {
        NJJoinBuildInputChannel channel(kChunkSize);
        ASSERT_TRUE(channel.add_chunk(make_chunk(100)).ok());
        auto iter = channel.buffered_chunk_iterator(false);
        auto res = iter();
        ASSERT_FALSE(res.ok());
        EXPECT_TRUE(res.status().is_end_of_file());
    }
}

TEST(NJJoinBuildInputChannelTest, IteratorEmptyChannel) {
    NJJoinBuildInputChannel channel(kChunkSize);
    auto iter = channel.buffered_chunk_iterator(true);
    auto res = iter();
    ASSERT_FALSE(res.ok());
    EXPECT_TRUE(res.status().is_end_of_file());
}

TEST(NJJoinBuildInputChannelTest, IteratorReturnsNonEmptyChunks) {
    NJJoinBuildInputChannel channel(kChunkSize);
    // Empty input must not surface as empty output chunks
    ASSERT_TRUE(channel.add_chunk(make_chunk(0)).ok());
    ASSERT_TRUE(channel.add_chunk(make_chunk(kChunkSize)).ok());
    ASSERT_TRUE(channel.add_chunk(make_chunk(0)).ok());
    ASSERT_TRUE(channel.add_chunk(make_chunk(1)).ok());

    size_t rows = drain_rows(channel.buffered_chunk_iterator(true));
    EXPECT_EQ(kChunkSize + 1, rows);
}

TEST(NJJoinBuildInputChannelTest, MemoryUsageDropsAfterDrain) {
    NJJoinBuildInputChannel channel(kChunkSize);
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(channel.add_chunk(make_chunk(3000)).ok());
    }
    EXPECT_GT(channel.memory_usage(), 0u);

    drain_rows(channel.buffered_chunk_iterator(true));
    EXPECT_EQ(0u, channel.memory_usage());
}

TEST(NJJoinBuildInputChannelTest, NumRowsUnaffectedByIterator) {
    NJJoinBuildInputChannel channel(kChunkSize);
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(channel.add_chunk(make_chunk(3000)).ok());
    }
    EXPECT_EQ(9000u, channel.num_rows());

    drain_rows(channel.buffered_chunk_iterator(true));
    // num_rows() counts the rows ever received, not the rows still buffered
    EXPECT_EQ(9000u, channel.num_rows());
}

TEST(NJJoinBuildInputChannelTest, CanKeepPushingAfterFinalize) {
    NJJoinBuildInputChannel channel(kChunkSize);
    ASSERT_TRUE(channel.add_chunk(make_chunk(100)).ok());
    EXPECT_EQ(100u, drain_rows(channel.buffered_chunk_iterator(true)));

    // finalize() is not terminal: the channel must keep accepting input
    ASSERT_TRUE(channel.add_chunk(make_chunk(50)).ok());
    EXPECT_EQ(50u, drain_rows(channel.buffered_chunk_iterator(true)));
    EXPECT_EQ(150u, channel.num_rows());
}

} // namespace starrocks::pipeline
