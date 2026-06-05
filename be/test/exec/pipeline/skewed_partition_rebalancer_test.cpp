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

#include "exec/pipeline/exchange/skewed_partition_rebalancer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <set>
#include <vector>

namespace starrocks {

namespace {

constexpr int64_t MB = 1024L * 1024L;
constexpr int64_t GB = 1024L * MB;

// Feeds the rebalancer a synthetic workload where `partition_data_sizes`
// describes how much data each partition has received since the last rebalance.
// Returns the new partition -> [task_id] assignment after the rebalance pass.
std::vector<std::vector<int32_t>> drive_one_rebalance(SkewedPartitionRebalancer& r,
                                                      const std::vector<int64_t>& partition_data_sizes,
                                                      int64_t total_data_processed) {
    // Convert data sizes into row counts (proxy used internally to estimate per
    // partition data size). 1 row == 1 byte makes assertions easier.
    for (int32_t p = 0; p < static_cast<int32_t>(partition_data_sizes.size()); ++p) {
        r.add_partition_row_count(p, partition_data_sizes[p]);
    }
    r.add_data_processed(total_data_processed);
    r.rebalance();
    return r.get_partition_assignments();
}

} // namespace

TEST(SkewedPartitionRebalancerTest, InitialAssignmentRoundRobin) {
    // 6 partitions across 3 tasks (bucket_count=1) → partitions distribute
    // round-robin: task 0 = {0, 3}, task 1 = {1, 4}, task 2 = {2, 5}.
    SkewedPartitionRebalancer r(/*partition_count=*/6, /*task_count=*/3,
                                /*min_partition_data_processed=*/1 * MB,
                                /*min_data_processed=*/10 * MB);

    auto assignments = r.get_partition_assignments();
    ASSERT_EQ(assignments.size(), 6);
    for (size_t p = 0; p < assignments.size(); ++p) {
        ASSERT_EQ(assignments[p].size(), 1) << "partition " << p << " should start with exactly 1 task";
        EXPECT_EQ(assignments[p][0], static_cast<int32_t>(p % 3)) << "partition " << p << " task assignment";
    }
}

TEST(SkewedPartitionRebalancerTest, GetTaskIdStableBeforeRebalance) {
    SkewedPartitionRebalancer r(4, 4, 1 * MB, 10 * MB);
    // Each partition has one task; get_task_id should always return that task.
    for (int32_t p = 0; p < 4; ++p) {
        for (int64_t idx = 0; idx < 100; ++idx) {
            EXPECT_EQ(r.get_task_id(p, idx), p);
        }
    }
}

TEST(SkewedPartitionRebalancerTest, NoRebalanceWhenBelowThreshold) {
    SkewedPartitionRebalancer r(4, 4, /*min_partition=*/1 * MB, /*min_total=*/10 * MB);

    // 1 MB of total data — well below the 10 MB trigger; no rebalance.
    auto assignments = drive_one_rebalance(r,
                                           /*partition_data_sizes=*/{900 * 1024, 100, 100, 100},
                                           /*total_data_processed=*/1 * MB);
    for (size_t p = 0; p < assignments.size(); ++p) {
        EXPECT_EQ(assignments[p].size(), 1) << "no rebalance expected for partition " << p;
    }
}

TEST(SkewedPartitionRebalancerTest, SkewedPartitionGetsSpreadAcrossTasks) {
    // 4 partitions / 4 tasks. Partition 0 dominates: ~100 MB of total. The cold
    // partitions are kept well below the per-partition rebalance threshold so
    // only the hot partition is eligible to spread.
    SkewedPartitionRebalancer r(/*partition_count=*/4, /*task_count=*/4,
                                /*min_partition=*/1 * MB, /*min_total=*/50 * MB);

    auto assignments = drive_one_rebalance(r,
                                           /*partition_data_sizes=*/{100 * MB, 100, 100, 100},
                                           /*total_data_processed=*/100 * MB);

    EXPECT_GT(assignments[0].size(), 1U) << "hot partition 0 should be spread to multiple tasks";

    // Other partitions should remain on their original task only.
    for (int32_t p = 1; p < 4; ++p) {
        EXPECT_EQ(assignments[p].size(), 1U) << "cold partition " << p << " should not be spread";
    }

    // Spread tasks for partition 0 should all be distinct.
    std::set<int32_t> unique(assignments[0].begin(), assignments[0].end());
    EXPECT_EQ(unique.size(), assignments[0].size()) << "partition 0 must not assign the same task twice";
}

TEST(SkewedPartitionRebalancerTest, GetTaskIdRotatesAcrossAssignedTasks) {
    // After spreading, get_task_id(p, idx) must rotate across the assigned tasks
    // so that subsequent rows of the hot partition land on different writers.
    SkewedPartitionRebalancer r(4, 4, 1 * MB, 50 * MB);

    drive_one_rebalance(r, {100 * MB, 1 * MB, 1 * MB, 1 * MB}, 103 * MB);
    auto assignments = r.get_partition_assignments();
    ASSERT_GT(assignments[0].size(), 1U);

    std::set<int32_t> observed;
    for (int64_t idx = 0; idx < 64; ++idx) {
        observed.insert(r.get_task_id(0, idx));
    }
    EXPECT_EQ(observed.size(), assignments[0].size()) << "get_task_id should cycle through every assigned task";
}

TEST(SkewedPartitionRebalancerTest, UniformPartitionsDoNotTriggerSpread) {
    // Uniform workload: rebalance pass runs (we cross the data threshold) but
    // no partition exceeds the skewness threshold, so the assignment must not
    // change.
    SkewedPartitionRebalancer r(/*partition_count=*/4, /*task_count=*/4,
                                /*min_partition=*/1 * MB, /*min_total=*/10 * MB);

    auto assignments = drive_one_rebalance(r, {25 * MB, 25 * MB, 25 * MB, 25 * MB}, 100 * MB);
    for (size_t p = 0; p < assignments.size(); ++p) {
        EXPECT_EQ(assignments[p].size(), 1U) << "uniform partition " << p << " should not be spread";
    }
}

TEST(SkewedPartitionRebalancerTest, PartitionBelowMinThresholdNotSpread) {
    // Total data is large (drives a rebalance pass) but no single partition
    // crosses the per-partition minimum. Spreading the load buys nothing here,
    // so no spread should happen.
    SkewedPartitionRebalancer r(/*partition_count=*/4, /*task_count=*/4,
                                /*min_partition=*/64 * MB, /*min_total=*/50 * MB);

    auto assignments = drive_one_rebalance(r, {10 * MB, 10 * MB, 10 * MB, 10 * MB}, 60 * MB);
    for (size_t p = 0; p < assignments.size(); ++p) {
        EXPECT_EQ(assignments[p].size(), 1U)
                << "partition " << p << " is below min_partition_threshold and must not be spread";
    }
}

TEST(SkewedPartitionRebalancerTest, AccessorsReflectConstructor) {
    SkewedPartitionRebalancer r(13, 5, 7 * MB, 17 * MB);
    EXPECT_EQ(r.partition_count(), 13);
    EXPECT_EQ(r.task_count(), 5);
}

TEST(SkewedPartitionRebalancerTest, MultipleRebalancePassesAccumulateSpread) {
    // First pass spreads partition 0 to a second task. Second pass (more data
    // arriving, partition 0 still hot) should spread it further.
    SkewedPartitionRebalancer r(/*partition_count=*/4, /*task_count=*/4,
                                /*min_partition=*/1 * MB, /*min_total=*/50 * MB);

    drive_one_rebalance(r, {100 * MB, 1 * MB, 1 * MB, 1 * MB}, 103 * MB);
    size_t spread_after_first = r.get_partition_assignments()[0].size();
    ASSERT_GE(spread_after_first, 2U);

    // Pour in another batch of skewed data and rebalance again.
    drive_one_rebalance(r, {100 * MB, 1 * MB, 1 * MB, 1 * MB}, /*total now=*/206 * MB);
    size_t spread_after_second = r.get_partition_assignments()[0].size();
    EXPECT_GE(spread_after_second, spread_after_first) << "additional rebalance pass should not shrink the spread";
}

TEST(SkewedPartitionRebalancerTest, NoInt64OverflowAtLargeScale) {
    // Regression: at large scale (e.g. ~100M rows, ~100 GB processed) the per-
    // partition data_size estimator (row_count[p] * data_processed) /
    // total_row_count can overflow int64_t (1e8 * 1e11 = 1e19 > 9.22e18). When
    // the intermediate wraps to a negative value, std::max(neg, prev) keeps the
    // stale prev and the hot partition never gets spread. The fix promotes the
    // multiply to __int128. This test exercises the overflow path.
    SkewedPartitionRebalancer r(/*partition_count=*/4, /*task_count=*/4,
                                /*min_partition=*/1 * MB, /*min_total=*/50 * MB);

    constexpr int64_t hot_rows = 100L * 1000 * 1000; // 100M rows on partition 0
    constexpr int64_t cold_rows = 1;                 // negligible on other partitions
    constexpr int64_t total_bytes = 100L * GB;       // 100 GB processed so far
    // Premise: hot_rows * total_bytes = 1e8 * ~1.07e11 = ~1.07e19, which
    // exceeds INT64_MAX (~9.22e18). The naive int64 multiply would wrap to a
    // negative value (signed overflow is UB, but on x86_64 GCC/Clang it wraps).

    drive_one_rebalance(r, {hot_rows, cold_rows, cold_rows, cold_rows}, total_bytes);

    // After fix, partition 0 is correctly recognized as massively over the
    // threshold and gets spread to additional tasks. Without the fix, the
    // overflow produces a negative estimate and the hot partition stays on 1
    // task.
    EXPECT_GT(r.get_partition_assignments()[0].size(), 1U)
            << "hot partition must be spread; if size == 1 the overflow regression has returned";
}

} // namespace starrocks
