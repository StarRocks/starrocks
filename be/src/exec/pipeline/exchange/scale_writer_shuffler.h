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

#pragma once

#include <cstdint>
#include <vector>

#include "exec/pipeline/exchange/skewed_partition_rebalancer.h"

namespace starrocks::pipeline {

// Stateful shuffler that wraps SkewedPartitionRebalancer to route rows to
// downstream channels with hot-partition spreading.
//
// Mirrors the contract of Shuffler::exchange_shuffle so the caller (typically
// ExchangeSinkOperator) can swap between plain hash shuffle and skew-aware
// shuffle by selection of the channel-id producer.
//
// Routing per row:
//   partition_id = hash_values[row] % partition_count
//   channel_id   = rebalancer.get_task_id(partition_id, persistent_index)
//
// Per-chunk optimization: within a single chunk, all rows of the same
// partition land on the same channel (per-chunk cache). Only across chunks
// does round-robin rotate among the partition's assigned channels.
//
// This class is NOT thread-safe (single-threaded, owned by one driver).
class ScaleWriterShuffler {
public:
    ScaleWriterShuffler(int32_t num_channels, int32_t partition_count,
                        int64_t min_partition_data_processed_rebalance_threshold,
                        int64_t min_data_processed_rebalance_threshold);

    // Produce per-row channel ids using rebalancer-driven assignment.
    // hash_values are pre-computed by the caller (crc32/fnv on partition cols).
    // chunk_bytes is the encoded byte size of the chunk; it feeds the
    // rebalancer's absolute-volume threshold gating.
    void exchange_shuffle(std::vector<uint32_t>& shuffle_channel_ids, const std::vector<uint32_t>& hash_values,
                          size_t num_rows, int64_t chunk_bytes);

    // Test-only accessor for the underlying rebalancer state.
    const SkewedPartitionRebalancer& rebalancer() const { return _rebalancer; }

private:
    const int32_t _num_channels;
    const int32_t _partition_count;

    SkewedPartitionRebalancer _rebalancer;

    // Persistent per-partition row index counter feeding rebalancer.get_task_id;
    // increments only on the first row of a partition within a chunk, so the
    // rotation granularity is per-chunk, not per-row.
    std::vector<int64_t> _partition_writer_indexes;

    // Per-chunk scratch: cleared at the start of each exchange_shuffle.
    std::vector<int32_t> _partition_writer_cache; // -1 means "not assigned yet this chunk"
    std::vector<int64_t> _partition_row_counts;
};

} // namespace starrocks::pipeline
