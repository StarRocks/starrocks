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

#include "runtime/stream_load/stream_load_context.h"

#include <gtest/gtest.h>

#include "runtime/exec_env.h"

namespace starrocks {

class StreamLoadContextTest : public testing::Test {
public:
    StreamLoadContextTest() = default;
    ~StreamLoadContextTest() override = default;

    void SetUp() override { _exec_env = ExecEnv::GetInstance(); }

protected:
    ExecEnv* _exec_env;
};

TEST_F(StreamLoadContextTest, calc_put_and_commit_rpc_timeout_ms) {
    // Test case structure: {timeout_second, expected_rpc_timeout_ms, description}
    struct TestCase {
        int32_t timeout_second;
        int32_t expected_rpc_timeout_ms;
        const char* description;
    };

    const int32_t default_timeout_ms = config::txn_commit_rpc_timeout_ms; // 60000

    TestCase test_cases[] = {
            // Default behavior: timeout_second = -1
            {-1, default_timeout_ms, "Default timeout when timeout_second is -1"},

            // Small timeout values where timeout_ms/2 < default_timeout_ms
            {30, 15000, "Small timeout (30s): timeout_ms/2 = 15000"},
            {10, 5000, "Very small timeout (10s): timeout_ms/2 = 5000"},

            // Large timeout values where timeout_ms/2 >= default_timeout_ms
            {200, 60000, "Large timeout (200s): min(100000, 60000) = 60000, max(50000, 60000) = 60000"},
            {1000, 250000, "Very large timeout (1000s): min(500000, 60000) = 60000, max(250000, 60000) = 250000"},

            // Boundary cases
            {120, 60000, "Boundary: timeout_ms/2 equals default (120s)"},
            {240, 60000, "Boundary: timeout_ms/4 equals default (240s)"},
            {180, 60000, "Boundary: timeout_ms/4 between timeout_ms/2 and default (180s)"},

            // Edge cases
            {0, 0, "Zero timeout: timeout_ms/2 = 0, timeout_ms/4 = 0"},
            {500, 125000, "Lower bound dominates: min(250000, 60000) = 60000, max(125000, 60000) = 125000"},
    };

    for (const auto& tc : test_cases) {
        SCOPED_TRACE(tc.description);
        StreamLoadContext ctx(_exec_env);
        ctx.timeout_second = tc.timeout_second;

        int32_t original_timeout_second = ctx.timeout_second;
        int32_t result = ctx.calc_put_and_commit_rpc_timeout_ms();

        EXPECT_EQ(tc.expected_rpc_timeout_ms, result) << "Failed for timeout_second=" << tc.timeout_second;
        EXPECT_EQ(original_timeout_second, ctx.timeout_second) << "Method should not modify timeout_second";
    }
}

} // namespace starrocks
