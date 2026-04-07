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

#include "exec/workgroup/pipeline_executor_set.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"

namespace starrocks::workgroup {

PARALLEL_TEST(PipelineExecutorSetConfigTest, test_constructor) {
    CpuUtil::CpuIds empty_cpuids;
    CpuUtil::CpuIds cpuids{0, 1, 2, 3, 4, 5, 6, 7};
    /// check enable_cpu_borrowing
    {
        PipelineExecutorSetConfig config(2, 2, 2, 2, empty_cpuids, false, true, nullptr);
        ASSERT_FALSE(config.enable_cpu_borrowing);
    }

    {
        PipelineExecutorSetConfig config(2, 2, 2, 2, empty_cpuids, true, false, nullptr);
        ASSERT_FALSE(config.enable_cpu_borrowing);
    }

    {
        PipelineExecutorSetConfig config(2, 2, 2, 2, empty_cpuids, true, true, nullptr);
        ASSERT_TRUE(config.enable_cpu_borrowing);
    }

    /// check cpuids
    {
        PipelineExecutorSetConfig config(2, 2, 2, 2, empty_cpuids, false, true, nullptr);
        ASSERT_EQ(0, config.total_cpuids.size());
    }

    {
        PipelineExecutorSetConfig config(0, 2, 2, 2, cpuids, false, true, nullptr);
        ASSERT_EQ(0, config.total_cpuids.size());
    }

    {
        PipelineExecutorSetConfig config(2, 2, 2, 2, cpuids, false, true, nullptr);
        ASSERT_EQ(2, config.total_cpuids.size());
        for (int i = 0; i < 2; i++) {
            ASSERT_EQ(i, config.total_cpuids[i]);
        }
    }

    {
        PipelineExecutorSetConfig config(8, 2, 2, 2, cpuids, false, true, nullptr);
        ASSERT_EQ(8, config.total_cpuids.size());
        for (int i = 0; i < 8; i++) {
            ASSERT_EQ(i, config.total_cpuids[i]);
        }
    }

    {
        PipelineExecutorSetConfig config(100, 2, 2, 2, cpuids, false, true, nullptr);
        ASSERT_EQ(8, config.total_cpuids.size());
        for (int i = 0; i < 8; i++) {
            ASSERT_EQ(i, config.total_cpuids[i]);
        }
    }
}

// Tests for PipelineExecutorSet::calculate_num_threads and the thread-count accessors.
// These tests create a PipelineExecutorSet without calling start() to verify the
// thread-count scaling logic in isolation.

// Shared executor (no borrowed CPUs): thread count is scaled proportionally
// to the fraction of total cores assigned to this executor set.
TEST(PipelineExecutorSetTest, test_calculate_num_threads_proportional) {
    CpuUtil::CpuIds all_cpuids{0, 1, 2, 3, 4, 5, 6, 7};
    // 8 total cores, 16 total connector-scan threads.
    PipelineExecutorSetConfig conf(8, 8, 8, 16, all_cpuids, false, false, nullptr);

    // Executor covering 2 out of 8 CPUs, no borrowed CPUs → 8 * 2/8 = 2 for driver/scan, 16 * 2/8 = 4 for connector scan
    PipelineExecutorSet exec(conf, "test_shared", CpuUtil::CpuIds{0, 1}, /*borrowed=*/{});
    EXPECT_EQ(4u, exec.num_connector_scan_threads());
    EXPECT_EQ(2u, exec.num_driver_threads());
    EXPECT_EQ(2u, exec.num_scan_threads());
}

// Exclusive executor (shared with CPU borrowing): when borrowed_cpuids is non-empty
// the executor will use borrowed CPUs and should receive the full thread count so
// it can fully utilise those extra cores.
TEST(PipelineExecutorSetTest, test_calculate_num_threads_full_when_borrowed) {
    CpuUtil::CpuIds all_cpuids{0, 1, 2, 3, 4, 5, 6, 7};
    // 8 total cores, 16 total connector-scan threads.
    PipelineExecutorSetConfig conf(8, 8, 8, 16, all_cpuids, false, false, nullptr);

    // Executor with 2 own CPUs but also borrows {2,3,4,5} → returns num_total_driver_threads -> 8.
    PipelineExecutorSet exec(conf, "test_borrowed", CpuUtil::CpuIds{0, 1},
                             /*borrowed=*/{{2, 3, 4, 5}});
    EXPECT_EQ(16u, exec.num_connector_scan_threads());
    EXPECT_EQ(8u, exec.num_driver_threads());
    EXPECT_EQ(8u, exec.num_scan_threads());
}

// The result is floored at 1 even when num_total_threads is very small relative
// to the total core count.
TEST(PipelineExecutorSetTest, test_calculate_num_threads_minimum_one) {
    CpuUtil::CpuIds all_cpuids{0, 1, 2, 3, 4, 5, 6, 7};
    // 8 total cores, only 1 total connector-scan thread configured.
    PipelineExecutorSetConfig conf(8, 1, 1, 1, all_cpuids, false, false, nullptr);

    // 1 out of 8 CPUs → 1 * 1/8 = 0, but floor is 1.
    PipelineExecutorSet exec(conf, "test_min", CpuUtil::CpuIds{0}, /*borrowed=*/{});
    EXPECT_EQ(1u, exec.num_connector_scan_threads());
    EXPECT_EQ(1u, exec.num_driver_threads());
    EXPECT_EQ(1u, exec.num_scan_threads());
}

// After the total-thread-count configuration is updated (simulating
// change_num_connector_scan_threads), the accessor reflects the new value.
TEST(PipelineExecutorSetTest, test_calculate_num_threads_reflects_config_update) {
    CpuUtil::CpuIds all_cpuids{0, 1, 2, 3, 4, 5, 6, 7};
    // Mutable config: num_total_connector_scan_threads starts at 4.
    PipelineExecutorSetConfig conf(8, 4, 4, 4, all_cpuids, false, false, nullptr);

    PipelineExecutorSet exec(conf, "test_update", CpuUtil::CpuIds{0, 1, 2, 3}, /*borrowed=*/{});
    // 4 * 4/8 = 2 initially.
    EXPECT_EQ(2u, exec.num_connector_scan_threads());

    // Simulate the config update performed by change_num_connector_scan_threads.
    conf.num_total_connector_scan_threads = 8;
    // The accessor now returns the updated proportional value: 8 * 4/8 = 4.
    EXPECT_EQ(4u, exec.num_connector_scan_threads());
}

} // namespace starrocks::workgroup
