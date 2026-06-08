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

#include <glog/logging.h>

#include <algorithm>

namespace starrocks::pipeline {

ScaleWriterShuffler::ScaleWriterShuffler(int32_t num_channels,
                                         int64_t min_partition_data_processed_rebalance_threshold,
                                         int64_t min_data_processed_rebalance_threshold)
        : _num_channels(num_channels),
          _rebalancer(num_channels, min_partition_data_processed_rebalance_threshold,
                      min_data_processed_rebalance_threshold),
          _partition_writer_indexes(SkewedPartitionRebalancer::kPartitionCount, 0),
          _partition_writer_cache(SkewedPartitionRebalancer::kPartitionCount, -1),
          _partition_row_counts(SkewedPartitionRebalancer::kPartitionCount, 0) {
    DCHECK_GT(num_channels, 0);
}

void ScaleWriterShuffler::exchange_shuffle(std::vector<uint32_t>& shuffle_channel_ids,
                                           const std::vector<uint32_t>& hash_values, size_t num_rows,
                                           int64_t chunk_bytes) {
    // Reset per-chunk scratch.
    std::fill(_partition_writer_cache.begin(), _partition_writer_cache.end(), -1);
    std::fill(_partition_row_counts.begin(), _partition_row_counts.end(), 0);

    // Cheap if the data-processed threshold has not been crossed yet.
    _rebalancer.rebalance();

    for (size_t i = 0; i < num_rows; ++i) {
        const uint32_t partition_id =
                hash_values[i] % static_cast<uint32_t>(SkewedPartitionRebalancer::kPartitionCount);
        _partition_row_counts[partition_id]++;

        int32_t writer = _partition_writer_cache[partition_id];
        if (writer == -1) {
            writer = _rebalancer.get_task_id(partition_id, _partition_writer_indexes[partition_id]++);
            _partition_writer_cache[partition_id] = writer;
        }
        shuffle_channel_ids[i] = static_cast<uint32_t>(writer);
    }

    for (int32_t partition = 0; partition < SkewedPartitionRebalancer::kPartitionCount; ++partition) {
        if (_partition_row_counts[partition] > 0) {
            _rebalancer.add_partition_row_count(partition, _partition_row_counts[partition]);
        }
    }
    _rebalancer.add_data_processed(chunk_bytes);
}

} // namespace starrocks::pipeline
