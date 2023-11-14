// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "util/cpu_usage_info.h"

#include <gtest/gtest.h>

#include "testutil/parallel_test.h"

namespace starrocks {

PARALLEL_TEST(CpuUsageRecorderTest, normal) {
    CpuUsageRecorder recorder;
    ASSERT_EQ(0, recorder.cpu_used_permille());

    recorder.update_interval();
    int cpu_used_permille = recorder.cpu_used_permille();
    ASSERT_GE(cpu_used_permille, 0);
}

} // namespace starrocks