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

#include "data_workflows/load/stream_load/stream_load_executor.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "compute_env/load/stream_load_context.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

// The attachment is sent with both the commit and the rollback, so a failed load reports
// what it consumed as well.
TEST(StreamLoadExecutorTest, collect_load_stat_carries_resource_usage_of_manual_load) {
    StreamLoadExecutor executor;
    StreamLoadContext ctx(nullptr);
    ctx.load_type = TLoadType::MANUAL_LOAD;
    ctx.load_src_type = TLoadSourceType::RAW;
    ctx.status = Status::MemoryLimitExceeded("Memory of query exceed limit");
    ctx.cpu_ns = 111;
    ctx.peak_mem_bytes = 222;

    TTxnCommitAttachment attachment;
    ASSERT_TRUE(executor.collect_load_stat(&ctx, &attachment));
    ASSERT_TRUE(attachment.__isset.manualLoadTxnCommitAttachment);
    const auto& manual_attachment = attachment.manualLoadTxnCommitAttachment;
    ASSERT_TRUE(manual_attachment.__isset.cpuNs);
    ASSERT_TRUE(manual_attachment.__isset.peakMemBytes);
    EXPECT_EQ(111, manual_attachment.cpuNs);
    EXPECT_EQ(222, manual_attachment.peakMemBytes);
}

TEST(StreamLoadExecutorTest, collect_load_stat_carries_resource_usage_of_routine_load) {
    StreamLoadExecutor executor;
    StreamLoadContext ctx(nullptr);
    ctx.load_type = TLoadType::ROUTINE_LOAD;
    ctx.load_src_type = TLoadSourceType::RAW;
    ctx.status = Status::MemoryLimitExceeded("Memory of query exceed limit");
    ctx.cpu_ns = 333;
    ctx.peak_mem_bytes = 444;

    TTxnCommitAttachment attachment;
    ASSERT_TRUE(executor.collect_load_stat(&ctx, &attachment));
    ASSERT_TRUE(attachment.__isset.rlTaskTxnCommitAttachment);
    const auto& rl_attachment = attachment.rlTaskTxnCommitAttachment;
    ASSERT_TRUE(rl_attachment.__isset.cpuNs);
    ASSERT_TRUE(rl_attachment.__isset.peakMemBytes);
    EXPECT_EQ(333, rl_attachment.cpuNs);
    EXPECT_EQ(444, rl_attachment.peakMemBytes);
}

// Zero means the backend does not know, and the fields stay unset so that the FE shows NULL.
TEST(StreamLoadExecutorTest, collect_load_stat_leaves_unknown_resource_usage_unset) {
    StreamLoadExecutor executor;
    StreamLoadContext ctx(nullptr);
    ctx.load_type = TLoadType::MANUAL_LOAD;
    ctx.load_src_type = TLoadSourceType::RAW;

    TTxnCommitAttachment attachment;
    ASSERT_TRUE(executor.collect_load_stat(&ctx, &attachment));
    ASSERT_TRUE(attachment.__isset.manualLoadTxnCommitAttachment);
    EXPECT_FALSE(attachment.manualLoadTxnCommitAttachment.__isset.cpuNs);
    EXPECT_FALSE(attachment.manualLoadTxnCommitAttachment.__isset.peakMemBytes);
}

} // namespace starrocks
