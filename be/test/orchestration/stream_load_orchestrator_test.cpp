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

#include "orchestration/stream_load_orchestrator.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "compute_env/load/stream_load_context.h"
#include "runtime/exec_env.h"

namespace starrocks::orchestration {

TEST(StreamLoadOrchestratorTest, execute_plan_fragment_preserves_be_test_sync_point) {
    ExecEnv exec_env;
    StreamLoadOrchestrator stream_load_orchestrator(&exec_env, nullptr);
    StreamLoadContext ctx(nullptr);

    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("StreamLoadExecutor::execute_plan_fragment:1",
                                          [](void* arg) { *((Status*)arg) = Status::InternalError("TestFail"); });
    DeferOp clear_sync_point([] {
        SyncPoint::GetInstance()->ClearCallBack("StreamLoadExecutor::execute_plan_fragment:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    ASSERT_OK(stream_load_orchestrator.execute_plan_fragment(&ctx));
    auto status = ctx.future.get();
    ASSERT_TRUE(status.is_internal_error());
    ASSERT_EQ("TestFail", status.message());
}

} // namespace starrocks::orchestration
