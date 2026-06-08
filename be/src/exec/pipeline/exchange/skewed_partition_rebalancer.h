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

// Helps distribute big or skewed partitions across available tasks to improve
// the performance of partitioned writes.
//
// The rebalancer initializes a set of buckets per task (taskBucketCount) and
// tries to uniformly distribute partitions across those buckets. This mitigates
// two problems:
//   1. Skewness across tasks: a single hot partition no longer pins one task.
//   2. Scaling a few big partitions even without skewness: writing throughput
//      can grow without massively reshuffling overall resources.
//
// Example - before (3 tasks, 3 buckets/task, 2 skewed partitions):
//   Task1               Task2              Task3
//   Bucket1 (Part 1)    Bucket1 (Part 2)   Bucket1
//   Bucket2             Bucket2            Bucket2
//   Bucket3             Bucket3            Bucket3
//
// After rebalancing the two hot partitions get more task buckets:
//   Task1               Task2              Task3
//   Bucket1 (Part 1)    Bucket1 (Part 2)   Bucket1 (Part 1)
//   Bucket2 (Part 2)    Bucket2 (Part 1)   Bucket2 (Part 2)
//   Bucket3             Bucket3            Bucket3
//
// The "task" abstraction is intentionally generic: callers in different
// scaling layers (local driver-level scaling within a single BE, or global
// cross-task / cross-BE scaling) can wire the same rebalancer using whatever
// task identifier they own.
//
// This class is NOT thread-safe. Callers must serialize access (typically
// via the owning partitioner / exchanger which runs on a single driver).

#pragma once

#include <cstdint>
#include <vector>

#include "util/indexed_priority_queue.hpp"

namespace starrocks {

class SkewedPartitionRebalancer {
private:
    struct TaskBucket {
        int32_t task_id;
        int32_t id;

        TaskBucket(int32_t task_id_, int32_t bucket_id_)
                : task_id(task_id_), id(task_id_ * kTaskBucketCount + bucket_id_) {}

        bool operator==(const TaskBucket& other) const { return id == other.id; }
        bool operator<(const TaskBucket& other) const { return id < other.id; }
        bool operator>(const TaskBucket& other) const { return id > other.id; }
    };

public:
    // Hardcoded to 1, matching Trino's effective usage. The TaskBucket abstraction
    // is kept so the algorithm remains a faithful port of Trino's
    // SkewedPartitionRebalancer; with kTaskBucketCount==1 every task has exactly
    // one bucket and the indirection is free.
    static constexpr int32_t kTaskBucketCount = 1;

    // Matches Trino's SCALE_WRITERS_PARTITION_COUNT. Hash values are reduced
    // modulo this; larger values give no extra spreading granularity since
    // tasks are the actual unit of fan-out.
    static constexpr int32_t kPartitionCount = 4096;

    SkewedPartitionRebalancer(int32_t task_count,
                              int64_t min_partition_data_processed_rebalance_threshold,
                              int64_t min_data_processed_rebalance_threshold);

    int32_t partition_count() const { return kPartitionCount; }
    int32_t task_count() const { return _task_count; }

    // Hot path: called per-row to decide which task owns the given partition for
    // the given row index. The mapping is stable between rebalance() calls.
    int32_t get_task_id(uint32_t partition_id, int64_t index) const;

    // Hot path: callers accumulate per-chunk data size / row count statistics.
    void add_data_processed(int64_t data_size);
    void add_partition_row_count(int32_t partition, int64_t row_count);

    // Checkpoint: typically called between chunks. If the accumulated data
    // since the last rebalance exceeds the threshold, an actual rebalance
    // pass is performed; otherwise this is a cheap no-op.
    void rebalance();

    // Observability counters (cumulative since construction).
    int64_t rebalance_pass_count() const { return _rebalance_pass_count; }
    int64_t spread_events_count() const { return _spread_events_count; }
    int64_t total_data_processed() const { return _data_processed; }
    // Largest fan-out across all partitions: max(|assignments[p]|).
    int32_t max_assigned_tasks() const;
    // Distinct partitions that have been spread to more than one task.
    int32_t spread_partition_count() const;

    // Test-only helpers --------------------------------------------------
    // Returns the current partition -> [task_id...] assignment.
    std::vector<std::vector<int32_t>> get_partition_assignments() const;

private:
    void _calculate_partition_data_size(int64_t data_processed);
    int64_t _calculate_task_bucket_data_size_since_last_rebalance(
            IndexedPriorityQueue<int32_t, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>& max_partitions);
    void _rebalance_based_on_task_bucket_skewness(
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>& max_task_buckets,
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>& min_task_buckets,
            std::vector<IndexedPriorityQueue<int32_t, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>>&
                    task_bucket_max_partitions);
    std::vector<TaskBucket> _find_skewed_min_task_buckets(
            const TaskBucket& max_task_bucket,
            const IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>&
                    min_task_buckets);
    bool _rebalance_partition(
            int32_t partition_id, const TaskBucket& to_task_bucket,
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>& max_task_buckets,
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>& min_task_buckets);

    bool _should_rebalance(int64_t data_processed) const;
    void _rebalance_partitions(int64_t data_processed);

    static constexpr double TASK_BUCKET_SKEWNESS_THRESHOLD = 0.7;

    const int32_t _task_count;
    const int64_t _min_partition_data_processed_rebalance_threshold;
    const int64_t _min_data_processed_rebalance_threshold;

    std::vector<int64_t> _partition_row_count;
    int64_t _data_processed = 0;
    int64_t _data_processed_at_last_rebalance = 0;

    // Observability counters.
    int64_t _rebalance_pass_count = 0; // actual heavy passes (passed _should_rebalance gate)
    int64_t _spread_events_count = 0;  // partitions newly assigned a task this pass

    std::vector<int64_t> _partition_data_size;
    std::vector<int64_t> _partition_data_size_at_last_rebalance;
    std::vector<int64_t> _partition_data_size_since_last_rebalance_per_task;
    std::vector<int64_t> _estimated_task_bucket_data_size_since_last_rebalance;

    // partition_id -> list of TaskBuckets that currently own this partition.
    std::vector<std::vector<TaskBucket>> _partition_assignments;
};

} // namespace starrocks
