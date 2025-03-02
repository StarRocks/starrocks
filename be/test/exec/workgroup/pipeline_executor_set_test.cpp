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

#include "testutil/assert.h"
#include "testutil/parallel_test.h"

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

} // namespace starrocks::workgroup