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
    ASSERT_LE(cpu_used_permille, 1000);
}

} // namespace starrocks