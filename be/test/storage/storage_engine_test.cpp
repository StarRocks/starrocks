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

#include "storage/storage_engine.h"

#include <gtest/gtest.h>

#include "testutil/parallel_test.h"

namespace starrocks {

PARALLEL_TEST(StorageEngineTest, test_garbage_sweep_interval_calculator) {
    config::min_garbage_sweep_interval = 100;
    config::max_garbage_sweep_interval = 10000;
    GarbageSweepIntervalCalculator calculator;

    struct TestCase {
        TestCase(int32_t original_min, int32_t original_max, bool expected_changed, int32_t expected_min,
                 int32_t expected_max)
                : original_min(original_min),
                  original_max(original_max),
                  expected_changed(expected_changed),
                  expected_min(expected_min),
                  expected_max(expected_max) {}

        const int32_t original_min;
        const int32_t original_max;

        const bool expected_changed;
        const int32_t expected_min;
        const int32_t expected_max;
    };
    TestCase test_cases[] = {{100, 2, true, 1, 2},  {-1, 4, true, 1, 4},  {-10, -2, true, 1, 1},
                             {0, 4, true, 1, 4},    {0, 0, true, 1, 1},   {2, 10, true, 2, 10},
                             {2, 10, false, 2, 10}, {3, 10, true, 3, 10}, {3, 11, true, 3, 11}};

    for (const auto& c : test_cases) {
        config::min_garbage_sweep_interval = c.original_min;
        config::max_garbage_sweep_interval = c.original_max;
        ASSERT_EQ(c.expected_changed, calculator.maybe_interval_updated());

        calculator.mutable_disk_usage() = 1000; // Make disk usage large enough to use min_interval as curr_interval.
        ASSERT_EQ(c.expected_min, calculator.curr_interval());
        calculator.mutable_disk_usage() = -1; // Make disk usage small enough to use max_interval as curr_interval.
        ASSERT_EQ(c.expected_max, calculator.curr_interval());

        for (double usage = -1; usage <= 2.0; usage += 0.1) {
            calculator.mutable_disk_usage() = usage;
            int32_t curr = calculator.curr_interval();
            ASSERT_GE(curr, c.expected_min);
            ASSERT_LE(curr, c.expected_max);
        }
    }
}

} // namespace starrocks
