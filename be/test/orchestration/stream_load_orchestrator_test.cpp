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
#include "compute_env/query/query_runtime_state.h"
#include "exec/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

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

// A load that fails still tells the FE what it consumed; the numbers go out with the rollback.
TEST(StreamLoadOrchestratorTest, finish_stream_load_records_resource_usage_on_failure) {
    pipeline::QueryRuntimeState query_runtime_state;
    query_runtime_state.incr_cpu_cost(4321);
    auto query_mem_tracker = std::make_shared<MemTracker>(-1, "finish_stream_load_failure");
    query_mem_tracker->consume(1000);
    query_mem_tracker->release(1000);

    RuntimeState runtime_state;
    runtime_state.set_query_runtime_state(&query_runtime_state);
    runtime_state.set_query_mem_tracker(query_mem_tracker);
    // the load channel on this backend peaked at 200, the one on another backend at 5000
    runtime_state.update_local_load_channel_peak_mem_bytes(200);
    runtime_state.update_load_channel_peak_mem_bytes(5000);
    runtime_state.update_num_rows_load_sink(7);

    StreamLoadContext ctx(nullptr);
    ctx.load_type = TLoadType::MANUAL_LOAD;
    ctx.load_src_type = TLoadSourceType::RAW;

    finish_stream_load(&ctx, &runtime_state, Status::MemoryLimitExceeded("Memory of query exceed limit"));

    auto status = ctx.future.get();
    ASSERT_TRUE(status.is_mem_limit_exceeded());
    EXPECT_EQ(4321, ctx.cpu_ns);
    // max(query peak + local load channel peak, largest load channel peak)
    EXPECT_EQ(5000, ctx.peak_mem_bytes);
    // the row counters are only taken from a successful execution
    EXPECT_EQ(0, ctx.number_loaded_rows);
}

TEST(StreamLoadOrchestratorTest, finish_stream_load_adds_local_load_channel_to_query_peak) {
    auto query_mem_tracker = std::make_shared<MemTracker>(-1, "finish_stream_load_local_peak");
    query_mem_tracker->consume(1000);
    query_mem_tracker->release(1000);

    RuntimeState runtime_state;
    runtime_state.set_query_mem_tracker(query_mem_tracker);
    runtime_state.update_local_load_channel_peak_mem_bytes(600);
    runtime_state.update_load_channel_peak_mem_bytes(600);

    StreamLoadContext ctx(nullptr);
    ctx.load_type = TLoadType::MANUAL_LOAD;
    ctx.load_src_type = TLoadSourceType::RAW;

    finish_stream_load(&ctx, &runtime_state, Status::InternalError("artificial failure"));

    ASSERT_TRUE(ctx.future.get().is_internal_error());
    // neither a query runtime state nor non-pipeline threads accounted: the CPU time stays unknown
    EXPECT_EQ(0, ctx.cpu_ns);
    EXPECT_EQ(1600, ctx.peak_mem_bytes);
}

// The non-pipeline engine, the default for stream load and routine load, has no query runtime
// state. Its CPU time is what the fragment thread and the scanner threads added to the fragment.
TEST(StreamLoadOrchestratorTest, finish_stream_load_uses_non_pipeline_cpu_without_query_runtime_state) {
    RuntimeState runtime_state;
    runtime_state.add_non_pipeline_cpu_ns(3000); // fragment thread
    runtime_state.add_non_pipeline_cpu_ns(1500); // scanner thread
    runtime_state.add_non_pipeline_cpu_ns(-7);   // a clock that went backwards is ignored
    runtime_state.update_num_rows_load_sink(3);

    StreamLoadContext ctx(nullptr);
    ctx.load_type = TLoadType::ROUTINE_LOAD;
    ctx.load_src_type = TLoadSourceType::RAW;
    ctx.max_filter_ratio = 1.0;

    finish_stream_load(&ctx, &runtime_state, Status::OK());

    ASSERT_OK(ctx.future.get());
    EXPECT_EQ(4500, ctx.cpu_ns);
    EXPECT_EQ(3, ctx.number_loaded_rows);
}

// A pipeline fragment reports the query's CPU counter, not the non-pipeline one.
TEST(StreamLoadOrchestratorTest, finish_stream_load_prefers_query_runtime_state_cpu) {
    pipeline::QueryRuntimeState query_runtime_state;
    query_runtime_state.incr_cpu_cost(900);

    RuntimeState runtime_state;
    runtime_state.set_query_runtime_state(&query_runtime_state);
    runtime_state.add_non_pipeline_cpu_ns(50);

    StreamLoadContext ctx(nullptr);
    ctx.load_type = TLoadType::MANUAL_LOAD;
    ctx.load_src_type = TLoadSourceType::RAW;

    finish_stream_load(&ctx, &runtime_state, Status::InternalError("artificial failure"));

    ASSERT_TRUE(ctx.future.get().is_internal_error());
    EXPECT_EQ(900, ctx.cpu_ns);
}

TEST(StreamLoadOrchestratorTest, thread_cpu_time_grows_with_work_on_the_thread) {
    int64_t start_ns = RuntimeState::thread_cpu_time_ns();
    ASSERT_GT(start_ns, 0);
    volatile uint64_t sink = 0;
    for (uint64_t i = 0; i < 20000000; ++i) {
        sink = sink + i;
    }
    EXPECT_GT(RuntimeState::thread_cpu_time_ns(), start_ns);
}

} // namespace starrocks::orchestration
