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

#include "exec/pipeline/query_context.h"

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <limits>

namespace starrocks::pipeline {

class ComputeQueryMemLimitTestFixture
        : public ::testing::TestWithParam<std::tuple<int64_t, int64_t, size_t, int64_t, int64_t>> {};

TEST_P(ComputeQueryMemLimitTestFixture, compute_query_mem_limit) {
    auto [parent_mem_limit, per_instance_mem_limit, pipeline_dop, option_query_mem_limit, expected_result] = GetParam();

    QueryContext ctx;
    ctx.set_total_fragments(3);

    auto result =
            ctx.compute_query_mem_limit(parent_mem_limit, per_instance_mem_limit, pipeline_dop, option_query_mem_limit);
    ASSERT_EQ(expected_result, result);
}

INSTANTIATE_TEST_SUITE_P(QueryContextTest, ComputeQueryMemLimitTestFixture,
                         ::testing::Values(
                                 // Not set per_instance_mem_limit and option_query_mem_limit.
                                 std::make_tuple(100, -1, 4, -1, -1),
                                 // Set option_query_mem_limit.
                                 std::make_tuple(100, 2, 16, 4, 4), std::make_tuple(100, 2, 16, 101, 100),
                                 // Set per_instance_mem_limit.
                                 std::make_tuple(100, 2, 16, -1, 96), std::make_tuple(100, 4, 16, -1, 100),
                                 // Set per_instance_mem_limit, and exceed MAX_INT64.
                                 std::make_tuple(100, std::numeric_limits<int64_t>::max(), 16, -1, 100),
                                 std::make_tuple(-1, std::numeric_limits<int64_t>::max(), 16, -1,
                                                 std::numeric_limits<int64_t>::max())));

} // namespace starrocks::pipeline
