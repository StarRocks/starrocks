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

#include "compute_env/load/stream_load_context_handle.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "compute_env/load/stream_context_mgr.h"
#include "compute_env/load/stream_load_context.h"
#include "runtime/exec_env.h"
#include "runtime/message_body_sink.h"

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

class StreamLoadContextHandleTest : public testing::Test {
public:
    StreamLoadContextHandleTest() = default;
    ~StreamLoadContextHandleTest() override = default;

    void SetUp() override { _exec_env = ExecEnv::GetInstance(); }

protected:
    ExecEnv* _exec_env;
};

TEST_F(StreamLoadContextHandleTest, cancel_and_close_channel_context) {
    StreamContextMgr stream_context_mgr(_exec_env->load_stream_mgr());
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

    int close_count = 0;
    StreamLoadContextHandle handle(ctx, [&](StreamLoadContext* context) {
        ++close_count;
        stream_context_mgr.remove_channel_context(context);
    });
    handle.cancel(Status::Cancelled("cancel only"));
    EXPECT_EQ(1, sink->cancel_count);
    EXPECT_EQ(0, close_count);

    auto* fetched = stream_context_mgr.get_channel_context(label, table, channel_id);
    ASSERT_NE(nullptr, fetched);
    StreamLoadContext::release(fetched);

    handle.close(Status::Cancelled("close"));
    EXPECT_EQ(2, sink->cancel_count);
    EXPECT_EQ(1, close_count);
    EXPECT_EQ(nullptr, stream_context_mgr.get_channel_context(label, table, channel_id));

    handle.close(Status::Cancelled("close again"));
    EXPECT_EQ(2, sink->cancel_count);
    EXPECT_EQ(1, close_count);
}

} // namespace starrocks
