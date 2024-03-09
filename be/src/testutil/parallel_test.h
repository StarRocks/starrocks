// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

// In order to run tests more faster, we are using the gtest-parallel script(https://github.com/google/gtest-parallel)
// to execute test binaries. Since many of our test cases use globally-shared resources and cannot be run in parallel,
// gtest-parallel must be executed with the option `--serialize_test_cases`, which will run tests within the same test
// case sequentially.
// The PARALLEL_TEST is just a simple wrapper on TEST to give each test a unique case name, make them be able to run
// in parallel.
#define TOKENPASTE(x, y) x##y
#define TOKENPASTE2(x, y) TOKENPASTE(x, y)

#define PARALLEL_TEST(test_case_name, test_name) TEST(TOKENPASTE2(test_case_name, __LINE__), test_name)
#define GROUP_PARALLEL_TEST(group_name, test_case_name, test_name) \
    PARALLEL_TEST(test_case_name, group_name##_##test_name)

#define GROUP_TEST_F(group_name, test_case_name, test_name) TEST_F(test_case_name, group_name##_##test_name)

#ifdef NDEBUG
#define GROUP_SLOW_PARALLEL_TEST(test_case_name, test_name) GROUP_PARALLEL_TEST(SLOW, test_case_name, test_name)
#define GROUP_SLOW_TEST_F(test_case_name, test_name) GROUP_TEST_F(SLOW, test_case_name, test_name)
#else
#define GROUP_SLOW_PARALLEL_TEST(test_case_name, test_name) GROUP_PARALLEL_TEST(DISABLED, test_case_name, test_name)
#define GROUP_SLOW_TEST_F(test_case_name, test_name) GROUP_TEST_F(DISABLED, test_case_name, test_name)
#endif