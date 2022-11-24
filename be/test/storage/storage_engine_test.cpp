// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    }
}

} // namespace starrocks
