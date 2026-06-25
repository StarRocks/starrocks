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

#include <memory>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "compute_env/load/load_stream_mgr.h"
#include "compute_env/load/stream_load_pipe.h"
#include "runtime/exec_env.h"
#include "runtime/message_body_sink.h"
#include "runtime/stream_load/stream_context_mgr.h"
#include "runtime/stream_load/stream_load_context_handle.h"

namespace starrocks {

class CountingBodySink final : public MessageBodySink {
public:
    Status append(const char* /*data*/, size_t /*size*/) override { return Status::OK(); }
    Status append(ByteBufferPtr&& /*buf*/) override { return Status::OK(); }

    void cancel(const Status& status) override {
        ++cancel_count;
        last_status = status;
    }

    int cancel_count = 0;
    Status last_status;
};

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

    const int32_t default_timeout_ms = config::stream_load_thrift_rpc_timeout_ms; // 60000

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

            // Overflow guard: timeout_second * 1000 must be computed in 64-bit (int32 would overflow
            // once timeout_second > 2147483), and the result is capped at INT32_MAX.
            {2147484, 536871000, "No int32 overflow (64-bit): timeout_ms/4 = 536871000 dominates the default"},
            {9000000, 2147483647, "Huge timeout: timeout_ms/4 exceeds INT32_MAX, capped to INT32_MAX"},
    };

    for (const auto& tc : test_cases) {
        SCOPED_TRACE(tc.description);
        StreamLoadContext ctx(_exec_env, _exec_env->load_stream_mgr());
        ctx.timeout_second = tc.timeout_second;

        int32_t original_timeout_second = ctx.timeout_second;
        int32_t result = ctx.calc_put_and_commit_rpc_timeout_ms();

        EXPECT_EQ(tc.expected_rpc_timeout_ms, result) << "Failed for timeout_second=" << tc.timeout_second;
        EXPECT_EQ(original_timeout_second, ctx.timeout_second) << "Method should not modify timeout_second";
    }
}

TEST_F(StreamLoadContextTest, destructor_removes_pipe_from_injected_load_stream_mgr) {
    LoadStreamMgr load_stream_mgr;
    UniqueId load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<StreamLoadPipe>();

    ASSERT_OK(load_stream_mgr.put(load_id, pipe));
    ASSERT_NE(nullptr, load_stream_mgr.get(load_id));

    { StreamLoadContext ctx(_exec_env, load_id, &load_stream_mgr); }

    EXPECT_EQ(nullptr, load_stream_mgr.get(load_id));
}

TEST_F(StreamLoadContextTest, stream_load_context_handle_cancel_and_close_channel_context) {
    StreamContextMgr stream_context_mgr;
    StreamLoadContext* ctx = nullptr;
    const std::string label = "stream_load_context_handle_label";
    const std::string db = "db";
    const std::string table = "table";
    const int channel_id = 7;
    const TUniqueId load_id = UniqueId::gen_uid().to_thrift();

    ASSERT_OK(stream_context_mgr.create_channel_context(_exec_env, label, channel_id, db, table,
                                                        TFileFormatType::FORMAT_CSV_PLAIN, ctx, load_id, 123));
    DeferOp release_ctx([&] { StreamLoadContext::release(ctx); });

    auto sink = std::make_shared<CountingBodySink>();
    ctx->body_sink = sink;
    ASSERT_OK(stream_context_mgr.put_channel_context(label, table, channel_id, ctx));

    StreamLoadContextHandle handle(ctx, &stream_context_mgr);
    handle.cancel(Status::Cancelled("cancel only"));
    EXPECT_EQ(1, sink->cancel_count);

    auto* fetched = stream_context_mgr.get_channel_context(label, table, channel_id);
    ASSERT_NE(nullptr, fetched);
    StreamLoadContext::release(fetched);

    handle.close(Status::Cancelled("close"));
    EXPECT_EQ(2, sink->cancel_count);
    EXPECT_EQ(nullptr, stream_context_mgr.get_channel_context(label, table, channel_id));

    handle.close(Status::Cancelled("close again"));
    EXPECT_EQ(2, sink->cancel_count);
}

} // namespace starrocks
