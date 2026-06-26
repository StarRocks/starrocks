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

#include "compute_env/load/stream_load_context.h"

#include <gtest/gtest.h>

#include <memory>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "compute_env/load/load_stream_mgr.h"
#include "compute_env/load/stream_load_pipe.h"

namespace starrocks {

TEST(StreamLoadContextTest, calc_put_and_commit_rpc_timeout_ms) {
    struct TestCase {
        int32_t timeout_second;
        int32_t expected_rpc_timeout_ms;
        const char* description;
    };

    const int32_t default_timeout_ms = config::stream_load_thrift_rpc_timeout_ms;

    TestCase test_cases[] = {
            {-1, default_timeout_ms, "Default timeout when timeout_second is -1"},
            {30, 15000, "Small timeout (30s): timeout_ms/2 = 15000"},
            {10, 5000, "Very small timeout (10s): timeout_ms/2 = 5000"},
            {200, 60000, "Large timeout (200s): min(100000, 60000) = 60000, max(50000, 60000) = 60000"},
            {1000, 250000, "Very large timeout (1000s): min(500000, 60000) = 60000, max(250000, 60000) = 250000"},
            {120, 60000, "Boundary: timeout_ms/2 equals default (120s)"},
            {240, 60000, "Boundary: timeout_ms/4 equals default (240s)"},
            {180, 60000, "Boundary: timeout_ms/4 between timeout_ms/2 and default (180s)"},
            {0, 0, "Zero timeout: timeout_ms/2 = 0, timeout_ms/4 = 0"},
            {500, 125000, "Lower bound dominates: min(250000, 60000) = 60000, max(125000, 60000) = 125000"},
            {2147484, 536871000, "No int32 overflow (64-bit): timeout_ms/4 = 536871000 dominates the default"},
            {9000000, 2147483647, "Huge timeout: timeout_ms/4 exceeds INT32_MAX, capped to INT32_MAX"},
    };

    for (const auto& tc : test_cases) {
        SCOPED_TRACE(tc.description);
        StreamLoadContext ctx(nullptr, nullptr);
        ctx.timeout_second = tc.timeout_second;

        int32_t original_timeout_second = ctx.timeout_second;
        int32_t result = ctx.calc_put_and_commit_rpc_timeout_ms();

        EXPECT_EQ(tc.expected_rpc_timeout_ms, result) << "Failed for timeout_second=" << tc.timeout_second;
        EXPECT_EQ(original_timeout_second, ctx.timeout_second) << "Method should not modify timeout_second";
    }
}

TEST(StreamLoadContextTest, destructor_removes_pipe_from_injected_load_stream_mgr) {
    LoadStreamMgr load_stream_mgr;
    UniqueId load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<StreamLoadPipe>();

    ASSERT_OK(load_stream_mgr.put(load_id, pipe));
    ASSERT_NE(nullptr, load_stream_mgr.get(load_id));

    { StreamLoadContext ctx(nullptr, load_id, &load_stream_mgr); }

    EXPECT_EQ(nullptr, load_stream_mgr.get(load_id));
}

TEST(StreamLoadContextTest, destructor_runs_rollback_callback_when_needed) {
    int rollback_count = 0;
    StreamLoadContext* expected_ctx = nullptr;

    {
        StreamLoadContext ctx(nullptr, nullptr);
        expected_ctx = &ctx;
        ctx.set_need_rollback([&](StreamLoadContext* callback_ctx) {
            EXPECT_EQ(expected_ctx, callback_ctx);
            ++rollback_count;
            return Status::OK();
        });

        EXPECT_TRUE(ctx.need_rollback());
    }

    EXPECT_EQ(1, rollback_count);
}

TEST(StreamLoadContextTest, clear_need_rollback_prevents_destructor_rollback_callback) {
    int rollback_count = 0;

    {
        StreamLoadContext ctx(nullptr, nullptr);
        ctx.set_need_rollback([&](StreamLoadContext* /*callback_ctx*/) {
            ++rollback_count;
            return Status::OK();
        });
        ctx.clear_need_rollback();

        EXPECT_FALSE(ctx.need_rollback());
    }

    EXPECT_EQ(0, rollback_count);
}

TEST(StreamLoadContextTest, set_need_rollback_rejects_empty_callback) {
    StreamLoadContext ctx(nullptr, nullptr);
    ASSERT_DEATH(ctx.set_need_rollback(nullptr), "callback");
}

} // namespace starrocks
