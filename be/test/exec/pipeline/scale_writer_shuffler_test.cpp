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

#include "exec/pipeline/exchange/scale_writer_shuffler.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <set>
#include <vector>

namespace starrocks::pipeline {

namespace {

constexpr int64_t MB = 1024L * 1024L;

// Build a hash vector where rows alternate among `num_partitions` distinct
// hash buckets (modulo'd against partition_count by the shuffler).
std::vector<uint32_t> make_uniform_hash(size_t num_rows, int32_t num_partitions) {
    std::vector<uint32_t> h(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        h[i] = static_cast<uint32_t>(i % num_partitions);
    }
    return h;
}

// Build a hash vector where `hot_partition` dominates with `hot_share` fraction
// of rows; the rest is distributed across the remaining partitions.
std::vector<uint32_t> make_skewed_hash(size_t num_rows, int32_t num_partitions, int32_t hot_partition,
                                       double hot_share) {
    std::vector<uint32_t> h(num_rows);
    size_t hot_rows = static_cast<size_t>(hot_share * num_rows);
    for (size_t i = 0; i < hot_rows; ++i) {
        h[i] = static_cast<uint32_t>(hot_partition);
    }
    for (size_t i = hot_rows; i < num_rows; ++i) {
        int32_t cold = static_cast<int32_t>(i % num_partitions);
        if (cold == hot_partition) {
            cold = (cold + 1) % num_partitions;
        }
        h[i] = static_cast<uint32_t>(cold);
    }
    return h;
}

} // namespace

TEST(ScaleWriterShufflerTest, SamePartitionSameChunkSameChannel) {
    // Within a single chunk, all rows of the same partition must route to the
    // same channel (per-chunk cache).
    ScaleWriterShuffler s(/*num_channels=*/4,
                          /*min_partition=*/1 * MB, /*min_total=*/10 * MB);

    const size_t num_rows = 64;
    std::vector<uint32_t> hash_values(num_rows);
    // First 32 rows all partition 3, next 32 rows all partition 5.
    for (size_t i = 0; i < 32; ++i) hash_values[i] = 3;
    for (size_t i = 32; i < 64; ++i) hash_values[i] = 5;

    std::vector<uint32_t> channels(num_rows, 0);
    s.exchange_shuffle(channels, hash_values, num_rows, /*chunk_bytes=*/1024);

    // All 32 rows of partition 3 → one channel; all 32 rows of partition 5 → one channel.
    EXPECT_EQ(std::set<uint32_t>(channels.begin(), channels.begin() + 32).size(), 1U);
    EXPECT_EQ(std::set<uint32_t>(channels.begin() + 32, channels.end()).size(), 1U);
}

TEST(ScaleWriterShufflerTest, CrossChunkRotationAfterSpread) {
    // After a rebalance triggers spreading the hot partition, subsequent
    // chunks of that partition rotate among the assigned channels.
    ScaleWriterShuffler s(/*num_channels=*/4,
                          /*min_partition=*/1 * MB, /*min_total=*/50 * MB);

    // Feed enough skewed data to trigger one rebalance.
    const size_t feed_rows = 200000;
    auto skewed = make_skewed_hash(feed_rows, /*num_partitions=*/8, /*hot=*/0, /*share=*/0.95);
    std::vector<uint32_t> channels(feed_rows, 0);
    // chunk_bytes large enough to cross the 50 MB trigger.
    s.exchange_shuffle(channels, skewed, feed_rows, /*chunk_bytes=*/60 * MB);

    // Now send 4 separate single-partition chunks of the hot partition; each
    // chunk should land on the next channel in the rotation.
    std::set<uint32_t> seen_channels;
    for (int chunk_no = 0; chunk_no < 4; ++chunk_no) {
        std::vector<uint32_t> hot_hash(8, 0);
        std::vector<uint32_t> chnl(8, 0);
        s.exchange_shuffle(chnl, hot_hash, 8, /*chunk_bytes=*/1024);
        seen_channels.insert(chnl[0]);
    }
    EXPECT_GT(seen_channels.size(), 1U) << "hot partition should rotate across chunks once spread";
}

TEST(ScaleWriterShufflerTest, UniformInputNoSpread) {
    // Uniform partition distribution. Even after crossing the data threshold,
    // each partition stays on a single channel.
    ScaleWriterShuffler s(/*num_channels=*/4,
                          /*min_partition=*/1 * MB, /*min_total=*/10 * MB);

    const size_t feed_rows = 100000;
    auto uniform = make_uniform_hash(feed_rows, /*num_partitions=*/8);
    std::vector<uint32_t> channels(feed_rows, 0);
    s.exchange_shuffle(channels, uniform, feed_rows, /*chunk_bytes=*/20 * MB);

    // Send a follow-up chunk of partition 0 only — should land on the same
    // channel as before (no spread happened).
    std::set<uint32_t> seen_for_p0;
    for (int chunk_no = 0; chunk_no < 4; ++chunk_no) {
        std::vector<uint32_t> hot_hash(8, 0);
        std::vector<uint32_t> chnl(8, 0);
        s.exchange_shuffle(chnl, hot_hash, 8, /*chunk_bytes=*/1024);
        seen_for_p0.insert(chnl[0]);
    }
    EXPECT_EQ(seen_for_p0.size(), 1U) << "uniform workload should not spread any partition";
}

TEST(ScaleWriterShufflerTest, HashIsModuloPartitionCount) {
    // Hash values < kPartitionCount map directly to that partition id. The
    // shuffler's modulo against kPartitionCount is a no-op here, but the
    // partition->channel mapping (partition % num_channels) still applies.
    ScaleWriterShuffler s(/*num_channels=*/4,
                          /*min_partition=*/1 * MB, /*min_total=*/10 * MB);

    const size_t num_rows = 16;
    std::vector<uint32_t> hash_values(num_rows);
    for (size_t i = 0; i < 8; ++i) hash_values[i] = 11;
    for (size_t i = 8; i < 16; ++i) hash_values[i] = 13;

    std::vector<uint32_t> channels(num_rows, 0);
    s.exchange_shuffle(channels, hash_values, num_rows, /*chunk_bytes=*/1024);

    // hash=11 -> partition 11 -> channel 11 % 4 = 3
    // hash=13 -> partition 13 -> channel 13 % 4 = 1
    EXPECT_EQ(channels[0], 3U);
    EXPECT_EQ(channels[8], 1U);
}

TEST(ScaleWriterShufflerTest, ChannelIdsWithinNumChannels) {
    // No channel id should ever exceed num_channels - 1.
    ScaleWriterShuffler s(/*num_channels=*/4,
                          /*min_partition=*/1 * MB, /*min_total=*/10 * MB);

    const size_t feed_rows = 50000;
    auto hashes = make_skewed_hash(feed_rows, /*num_partitions=*/16, /*hot=*/0, /*share=*/0.95);
    std::vector<uint32_t> channels(feed_rows, 0);
    s.exchange_shuffle(channels, hashes, feed_rows, /*chunk_bytes=*/60 * MB);

    for (auto c : channels) {
        EXPECT_LT(c, 4U) << "channel id must be < num_channels";
    }
}

} // namespace starrocks::pipeline
