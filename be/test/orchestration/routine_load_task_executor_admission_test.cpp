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

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "base/concurrency/await.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "common/config_exec_env_fwd.h"
#include "common/config_ingest_fwd.h"
#include "common/process_exit.h"
#include "common/system/cpu_info.h"
#include "data_workflows/load/stream_load/stream_load_executor.h"
#include "exec/exec_env.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/FrontendService_types.h"
#include "orchestration/routine_load_task_executor.h"
#include "orchestration/stream_load_orchestrator.h"

namespace starrocks {

extern std::atomic<bool> k_starrocks_exit;
extern std::atomic<int64_t> k_starrocks_exit_start_ms;

namespace orchestration {

class RoutineLoadTaskExecutorAdmissionTest : public testing::Test {
protected:
    static void SetUpTestSuite() { CpuInfo::init(); }

    void SetUp() override {
        _stream_load_executor = std::make_unique<StreamLoadExecutor>();
        _executor = std::make_unique<RoutineLoadTaskExecutor>(&_env, &_orchestrator, _stream_load_executor.get());
        ASSERT_OK(_executor->init());
    }

    void TearDown() override {
        k_starrocks_exit.store(false);
        k_starrocks_exit_start_ms.store(0);
        if (_executor) {
            _executor->stop();
        }
        _executor.reset();
        _stream_load_executor.reset();
    }

    TRoutineLoadTask make_task() {
        TRoutineLoadTask task;
        task.type = TLoadSourceType::KAFKA;
        task.job_id = 1;
        task.id = TUniqueId();
        task.txn_id = 4;
        task.auth_code = 5;
        task.__set_db("db1");
        task.__set_tbl("tbl1");
        task.__set_label("l1");
        TKafkaLoadInfo k_info;
        k_info.brokers = "127.0.0.1:9092";
        k_info.topic = "test";
        std::map<int32_t, int64_t> part_off;
        part_off[0] = 13;
        k_info.__set_partition_begin_offset(part_off);
        task.__set_kafka_load_info(k_info);
        return task;
    }

    ExecEnv _env;
    StreamLoadOrchestrator _orchestrator{&_env, nullptr};
    std::unique_ptr<StreamLoadExecutor> _stream_load_executor;
    std::unique_ptr<RoutineLoadTaskExecutor> _executor;
};

TEST_F(RoutineLoadTaskExecutorAdmissionTest, submit_rejects_after_cutoff) {
    ASSERT_TRUE(set_process_exit());
    k_starrocks_exit_start_ms.store(MonotonicMillis() - config::graceful_exit_reject_fallback_ms - 1);
    ASSERT_FALSE(should_accept_new_request());

    Status st = _executor->submit_task(make_task());
    ASSERT_TRUE(st.is_service_unavailable());
    ASSERT_EQ(0, shutdown_work_inflight());
}

TEST_F(RoutineLoadTaskExecutorAdmissionTest, submit_fail_zeros_inflight) {
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });

    Status st = _executor->submit_task(make_task());
    ASSERT_TRUE(st.is_internal_error());
    ASSERT_EQ(0, shutdown_work_inflight());
}

TEST_F(RoutineLoadTaskExecutorAdmissionTest, submit_unknown_source_zeros_inflight) {
    TRoutineLoadTask task = make_task();
    task.type = static_cast<TLoadSourceType::type>(99);
    Status st = _executor->submit_task(task);
    ASSERT_TRUE(st.is_internal_error());
    ASSERT_NE(st.message().find("unknown load source type"), std::string::npos);
    ASSERT_EQ(0, shutdown_work_inflight());
}

TEST_F(RoutineLoadTaskExecutorAdmissionTest, submit_holds_inflight_until_finish) {
    std::atomic<int> phase{0};
    DeferOp defer([&]() {
        phase.store(2);
        SyncPoint::GetInstance()->ClearCallBack("RoutineLoadTaskExecutor::submit_task:before_exec");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("RoutineLoadTaskExecutor::submit_task:before_exec", [&](void* arg) {
        *static_cast<bool*>(arg) = true;
        phase.store(1);
        while (phase.load() < 2) {
            std::this_thread::yield();
        }
    });

    ASSERT_OK(_executor->submit_task(make_task()));
    ASSERT_TRUE(Awaitility().timeout(60000).until([&] { return phase.load() == 1; }));
    ASSERT_EQ(1, shutdown_work_inflight());
    phase.store(2);
    ASSERT_TRUE(Awaitility().timeout(60000).until([&] { return shutdown_work_inflight() == 0; }));
}

} // namespace orchestration
} // namespace starrocks
