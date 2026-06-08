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
//
// This file is ported from:
//   https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/
//     operator/output/SkewedPartitionRebalancer.java

#include "exec/pipeline/exchange/skewed_partition_rebalancer.h"

#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <optional>

namespace starrocks {

SkewedPartitionRebalancer::SkewedPartitionRebalancer(int32_t task_count,
                                                     int64_t min_partition_data_processed_rebalance_threshold,
                                                     int64_t min_data_processed_rebalance_threshold)
        : _task_count(task_count),
          _min_partition_data_processed_rebalance_threshold(min_partition_data_processed_rebalance_threshold),
          _min_data_processed_rebalance_threshold(
                  std::max(min_partition_data_processed_rebalance_threshold, min_data_processed_rebalance_threshold)),
          _partition_row_count(kPartitionCount, 0),
          _partition_data_size(kPartitionCount, 0),
          _partition_data_size_at_last_rebalance(kPartitionCount, 0),
          _partition_data_size_since_last_rebalance_per_task(kPartitionCount, 0),
          _estimated_task_bucket_data_size_since_last_rebalance(task_count * kTaskBucketCount, 0),
          _partition_assignments(kPartitionCount) {
    DCHECK_GT(task_count, 0);

    std::vector<int32_t> task_bucket_ids(task_count, 0);
    for (int32_t partition = 0; partition < kPartitionCount; partition++) {
        int32_t task_id = partition % task_count;
        int32_t bucket_id = task_bucket_ids[task_id]++ % kTaskBucketCount;
        _partition_assignments[partition].emplace_back(task_id, bucket_id);
    }
}

int32_t SkewedPartitionRebalancer::get_task_id(uint32_t partition_id, int64_t index) const {
    const auto& task_ids = _partition_assignments[partition_id];
    return task_ids[index % static_cast<int64_t>(task_ids.size())].task_id;
}

void SkewedPartitionRebalancer::add_data_processed(int64_t data_size) {
    _data_processed += data_size;
}

void SkewedPartitionRebalancer::add_partition_row_count(int32_t partition, int64_t row_count) {
    _partition_row_count[partition] += row_count;
}

void SkewedPartitionRebalancer::rebalance() {
    int64_t current_data_processed = _data_processed;
    if (_should_rebalance(current_data_processed)) {
        _rebalance_partitions(current_data_processed);
    }
}

int32_t SkewedPartitionRebalancer::max_assigned_tasks() const {
    int32_t mx = 0;
    for (const auto& a : _partition_assignments) {
        if (static_cast<int32_t>(a.size()) > mx) {
            mx = static_cast<int32_t>(a.size());
        }
    }
    return mx;
}

int32_t SkewedPartitionRebalancer::spread_partition_count() const {
    int32_t c = 0;
    for (const auto& a : _partition_assignments) {
        if (a.size() > 1) {
            ++c;
        }
    }
    return c;
}

std::vector<std::vector<int32_t>> SkewedPartitionRebalancer::get_partition_assignments() const {
    std::vector<std::vector<int32_t>> result;
    result.reserve(_partition_assignments.size());
    for (const auto& partition_assignment : _partition_assignments) {
        std::vector<int32_t> tasks;
        tasks.reserve(partition_assignment.size());
        for (const auto& tb : partition_assignment) {
            tasks.push_back(tb.task_id);
        }
        result.push_back(std::move(tasks));
    }
    return result;
}

void SkewedPartitionRebalancer::_calculate_partition_data_size(int64_t data_processed) {
    int64_t total_partition_row_count = 0;
    for (int32_t partition = 0; partition < kPartitionCount; partition++) {
        total_partition_row_count += _partition_row_count[partition];
    }
    if (total_partition_row_count == 0) {
        return;
    }
    for (int32_t partition = 0; partition < kPartitionCount; partition++) {
        // Use __int128 to avoid int64 overflow: at large scale (e.g. 100M rows
        // and 100 GB processed in one write, 1e8 * 1e11 = 1e19) the product
        // exceeds INT64_MAX (~9.22e18) and wraps to a negative value, which
        // would corrupt rebalance decisions.
        __int128 numerator = static_cast<__int128>(_partition_row_count[partition]) * data_processed;
        int64_t estimated = static_cast<int64_t>(numerator / total_partition_row_count);
        _partition_data_size[partition] = std::max(estimated, _partition_data_size[partition]);
    }
}

int64_t SkewedPartitionRebalancer::_calculate_task_bucket_data_size_since_last_rebalance(
        IndexedPriorityQueue<int32_t, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>& max_partitions) {
    int64_t estimated_data_size_since_last_rebalance = 0;
    for (const auto& partition : max_partitions) {
        estimated_data_size_since_last_rebalance += _partition_data_size_since_last_rebalance_per_task[partition];
    }
    return estimated_data_size_since_last_rebalance;
}

void SkewedPartitionRebalancer::_rebalance_based_on_task_bucket_skewness(
        IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>& max_task_buckets,
        IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>& min_task_buckets,
        std::vector<IndexedPriorityQueue<int32_t, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>>&
                task_bucket_max_partitions) {
    std::vector<int32_t> scaled_partitions;
    while (true) {
        std::optional<TaskBucket> max_task_bucket = max_task_buckets.poll();
        if (!max_task_bucket.has_value()) {
            break;
        }

        auto& max_partitions = task_bucket_max_partitions[max_task_bucket->id];
        if (max_partitions.is_empty()) {
            continue;
        }

        // Find candidate min task buckets that are skewed enough to receive load.
        std::vector<TaskBucket> min_skewed_task_buckets =
                _find_skewed_min_task_buckets(*max_task_bucket, min_task_buckets);
        if (min_skewed_task_buckets.empty()) {
            // No remaining min bucket is skewed vs current max; subsequent maxes
            // will be even less skewed, so we can stop.
            break;
        }

        while (true) {
            std::optional<int32_t> max_partition = max_partitions.poll();
            if (!max_partition.has_value()) {
                break;
            }
            int32_t max_partition_value = *max_partition;

            if (std::find(scaled_partitions.begin(), scaled_partitions.end(), max_partition_value) !=
                scaled_partitions.end()) {
                continue;
            }

            int32_t total_assigned_tasks = static_cast<int32_t>(_partition_assignments[max_partition_value].size());
            if (_partition_data_size[max_partition_value] >=
                _min_partition_data_processed_rebalance_threshold * total_assigned_tasks) {
                for (const TaskBucket& min_task_bucket : min_skewed_task_buckets) {
                    if (_rebalance_partition(max_partition_value, min_task_bucket, max_task_buckets,
                                             min_task_buckets)) {
                        scaled_partitions.push_back(max_partition_value);
                        ++_spread_events_count;
                        break;
                    }
                }
            } else {
                // Partitions are sorted by per-task data size; the rest are even
                // smaller and don't justify the cost of spreading them further.
                break;
            }
        }
    }
}

std::vector<SkewedPartitionRebalancer::TaskBucket> SkewedPartitionRebalancer::_find_skewed_min_task_buckets(
        const TaskBucket& max_task_bucket,
        const IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>& min_task_buckets) {
    std::vector<TaskBucket> min_skewed_task_buckets;
    const int64_t max_size = _estimated_task_bucket_data_size_since_last_rebalance[max_task_bucket.id];

    for (const auto& min_task_bucket : min_task_buckets) {
        const int64_t min_size = _estimated_task_bucket_data_size_since_last_rebalance[min_task_bucket.id];
        const double skewness = static_cast<double>(max_size - min_size) / static_cast<double>(max_size);
        if (skewness <= TASK_BUCKET_SKEWNESS_THRESHOLD || std::isnan(skewness)) {
            break;
        }
        if (max_task_bucket.task_id != min_task_bucket.task_id) {
            min_skewed_task_buckets.push_back(min_task_bucket);
        }
    }
    return min_skewed_task_buckets;
}

bool SkewedPartitionRebalancer::_rebalance_partition(
        int32_t partition_id, const TaskBucket& to_task_bucket,
        IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>& max_task_buckets,
        IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>& min_task_buckets) {
    auto& assignments = _partition_assignments[partition_id];
    const bool already_has_task =
            std::any_of(assignments.begin(), assignments.end(),
                        [&to_task_bucket](const TaskBucket& tb) { return tb.task_id == to_task_bucket.task_id; });
    if (already_has_task) {
        return false;
    }

    assignments.push_back(to_task_bucket);

    const int32_t new_task_count = static_cast<int32_t>(assignments.size());
    const int32_t old_task_count = new_task_count - 1;
    for (const TaskBucket& task_bucket : assignments) {
        if (task_bucket == to_task_bucket) {
            _estimated_task_bucket_data_size_since_last_rebalance[task_bucket.id] +=
                    (_partition_data_size_since_last_rebalance_per_task[partition_id] * old_task_count) /
                    new_task_count;
        } else {
            _estimated_task_bucket_data_size_since_last_rebalance[task_bucket.id] -=
                    _partition_data_size_since_last_rebalance_per_task[partition_id] / new_task_count;
        }
        max_task_buckets.add_or_update(task_bucket,
                                       _estimated_task_bucket_data_size_since_last_rebalance[task_bucket.id]);
        min_task_buckets.add_or_update(task_bucket,
                                       _estimated_task_bucket_data_size_since_last_rebalance[task_bucket.id]);
    }
    return true;
}

bool SkewedPartitionRebalancer::_should_rebalance(int64_t data_processed) const {
    return (data_processed - _data_processed_at_last_rebalance) >= _min_data_processed_rebalance_threshold;
}

void SkewedPartitionRebalancer::_rebalance_partitions(int64_t data_processed) {
    DCHECK(_should_rebalance(data_processed));
    ++_rebalance_pass_count;

    _calculate_partition_data_size(data_processed);

    for (int32_t partition = 0; partition < kPartitionCount; partition++) {
        int64_t data_size = _partition_data_size[partition];
        int64_t delta = data_size - _partition_data_size_at_last_rebalance[partition];
        _partition_data_size_since_last_rebalance_per_task[partition] =
                delta / static_cast<int64_t>(_partition_assignments[partition].size());
        _partition_data_size_at_last_rebalance[partition] = data_size;
    }

    std::vector<IndexedPriorityQueue<int32_t, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>>
            task_bucket_max_partitions(_task_count * kTaskBucketCount);

    for (int32_t partition = 0; partition < kPartitionCount; partition++) {
        const auto& task_assignments = _partition_assignments[partition];
        for (const auto& task_bucket : task_assignments) {
            task_bucket_max_partitions[task_bucket.id].add_or_update(
                    partition, _partition_data_size_since_last_rebalance_per_task[partition]);
        }
    }

    IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW> max_task_buckets;
    IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH> min_task_buckets;

    for (int32_t task_id = 0; task_id < _task_count; task_id++) {
        for (int32_t bucket_id = 0; bucket_id < kTaskBucketCount; bucket_id++) {
            TaskBucket tb(task_id, bucket_id);
            _estimated_task_bucket_data_size_since_last_rebalance[tb.id] =
                    _calculate_task_bucket_data_size_since_last_rebalance(task_bucket_max_partitions[tb.id]);
            max_task_buckets.add_or_update(tb, _estimated_task_bucket_data_size_since_last_rebalance[tb.id]);
            min_task_buckets.add_or_update(tb, _estimated_task_bucket_data_size_since_last_rebalance[tb.id]);
        }
    }

    _rebalance_based_on_task_bucket_skewness(max_task_buckets, min_task_buckets, task_bucket_max_partitions);
    _data_processed_at_last_rebalance = data_processed;
}

} // namespace starrocks
