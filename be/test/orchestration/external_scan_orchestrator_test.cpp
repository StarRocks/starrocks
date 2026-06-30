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

#include "orchestration/external_scan_orchestrator.h"

#include <gtest/gtest.h>

#include <ctime>
#include <filesystem>
#include <memory>
#include <string>

#include "base/metrics.h"
#include "common/config_exec_env_fwd.h"
#include "common/config_runtime_fwd.h"
#include "common/status.h"
#include "common/system/cpu_info.h"
#include "compute_env/compute_env.h"
#include "exec/pipeline/driver_executor_factory.h"
#include "exec/pipeline/driver_queue_factory.h"
#include "exec/runtime/query_context_manager.h"
#include "gen_cpp/StatusCode_types.h"
#include "orchestration/external_scan_context_mgr.h"
#include "orchestration/fragment_mgr.h"
#include "runtime/env/global_env.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_env_test_util.h"

namespace starrocks::orchestration {

class ExternalScanOrchestratorTest : public testing::Test {
protected:
    static void SetUpTestSuite() {
        static auto* metrics = new MetricRegistry("external_scan_orchestrator_global_test");
        CpuInfo::init();
        auto* global_env = GlobalEnv::GetInstance();
        _previous_process_mem_tracker = global_env->_process_mem_tracker;
        _previous_query_pool_mem_tracker = global_env->_query_pool_mem_tracker;
        if (global_env->_process_mem_tracker == nullptr) {
            _process_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::PROCESS, -1, "process");
            global_env->_process_mem_tracker = _process_mem_tracker;
        }
        if (global_env->_query_pool_mem_tracker == nullptr) {
            _query_pool_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, -1, "query_pool",
                                                                   global_env->process_mem_tracker());
            global_env->_query_pool_mem_tracker = _query_pool_mem_tracker;
        }

        runtime_env_test::set_small_thread_pool_configs();
        config::fragment_pool_thread_num_min = 1;
        config::fragment_pool_thread_num_max = 1;
        config::fragment_pool_queue_size = 64;
        config::pipeline_exec_thread_pool_thread_num = 1;
        config::pipeline_scan_thread_pool_thread_num = 1;
        config::pipeline_connector_scan_thread_num_per_cpu = 1;
        config::pipeline_max_num_drivers_per_exec_thread = 4;
        const auto spill_path = std::filesystem::absolute("./ut_dir/orchestration_spill");
        config::spill_local_storage_dir = spill_path.string() + ",medium:ssd";
        std::error_code ec;
        std::filesystem::create_directories(spill_path, ec);
        ASSERT_FALSE(ec) << ec.message();
        ASSERT_TRUE(global_env->init_execution_thread_pools(metrics).ok());
    }

    static void TearDownTestSuite() {
        auto* global_env = GlobalEnv::GetInstance();
        global_env->_query_pool_mem_tracker = _previous_query_pool_mem_tracker;
        global_env->_process_mem_tracker = _previous_process_mem_tracker;
        _previous_query_pool_mem_tracker.reset();
        _previous_process_mem_tracker.reset();
        _query_pool_mem_tracker.reset();
        _process_mem_tracker.reset();
    }

    void SetUp() override {
        ComputeEnvOptions options;
        options.global_env = GlobalEnv::GetInstance();
        options.as_cn = true;
        options.query_cache_capacity = 4 * 1024 * 1024;
        options.driver_queue_factory = pipeline::create_query_shared_driver_queue;
        options.driver_executor_factory = pipeline::create_workgroup_driver_executor;
        ASSERT_TRUE(_compute_env.init(options).ok());
        _exec_env.set_compute_env(&_compute_env);
        _fragment_mgr = std::make_unique<FragmentMgr>(&_exec_env, nullptr);
        _exec_env._query_context_mgr = new pipeline::QueryContextManager(5);
        _exec_env._refresh_service_contexts();
    }

    void TearDown() override {
        _fragment_mgr.reset();
        delete _exec_env._query_context_mgr;
        _exec_env._query_context_mgr = nullptr;
        _exec_env.set_compute_env(nullptr);
        _compute_env.destroy();
    }

    ExecEnv _exec_env;
    ComputeEnv _compute_env;
    std::unique_ptr<FragmentMgr> _fragment_mgr;
    static std::shared_ptr<MemTracker> _previous_process_mem_tracker;
    static std::shared_ptr<MemTracker> _previous_query_pool_mem_tracker;
    static std::shared_ptr<MemTracker> _process_mem_tracker;
    static std::shared_ptr<MemTracker> _query_pool_mem_tracker;
};

std::shared_ptr<MemTracker> ExternalScanOrchestratorTest::_previous_process_mem_tracker;
std::shared_ptr<MemTracker> ExternalScanOrchestratorTest::_previous_query_pool_mem_tracker;
std::shared_ptr<MemTracker> ExternalScanOrchestratorTest::_process_mem_tracker;
std::shared_ptr<MemTracker> ExternalScanOrchestratorTest::_query_pool_mem_tracker;

TEST_F(ExternalScanOrchestratorTest, open_scanner_returns_context_for_invalid_batch_size) {
    ExternalScanContextMgr context_mgr(&_exec_env, nullptr, _fragment_mgr.get());
    ExternalScanOrchestrator orchestrator(&_exec_env, &context_mgr);

    TScanOpenParams params;
    TScanOpenResult result;
    orchestrator.open_scanner(result, params);

    EXPECT_EQ(TStatusCode::INVALID_ARGUMENT, result.status.status_code);
    EXPECT_TRUE(result.__isset.context_id);
    EXPECT_TRUE(result.__isset.selected_columns);

    std::shared_ptr<ScanContext> context;
    ASSERT_TRUE(context_mgr.get_scan_context(result.context_id, &context).ok());
    ASSERT_NE(nullptr, context);
}

TEST_F(ExternalScanOrchestratorTest, get_next_missing_context_returns_not_found) {
    ExternalScanContextMgr context_mgr(&_exec_env, nullptr, _fragment_mgr.get());
    ExternalScanOrchestrator orchestrator(&_exec_env, &context_mgr);

    TScanNextBatchParams params;
    params.__set_context_id("missing");
    params.__set_offset(0);
    TScanBatchResult result;
    orchestrator.get_next(result, params);

    EXPECT_EQ(TStatusCode::NOT_FOUND, result.status.status_code);
}

TEST_F(ExternalScanOrchestratorTest, get_next_offset_mismatch_returns_not_found_and_refreshes_access_time) {
    ExternalScanContextMgr context_mgr(&_exec_env, nullptr, _fragment_mgr.get());
    ExternalScanOrchestrator orchestrator(&_exec_env, &context_mgr);

    std::shared_ptr<ScanContext> context;
    ASSERT_TRUE(context_mgr.create_scan_context(&context).ok());
    ASSERT_NE(nullptr, context);
    context->offset = 7;
    context->last_access_time = 1;

    TScanNextBatchParams params;
    params.__set_context_id(context->context_id);
    params.__set_offset(3);
    TScanBatchResult result;
    orchestrator.get_next(result, params);

    EXPECT_EQ(TStatusCode::NOT_FOUND, result.status.status_code);
    EXPECT_GT(context->last_access_time, 1);
}

TEST_F(ExternalScanOrchestratorTest, close_scanner_clears_context) {
    ExternalScanContextMgr context_mgr(&_exec_env, nullptr, _fragment_mgr.get());
    ExternalScanOrchestrator orchestrator(&_exec_env, &context_mgr);

    std::shared_ptr<ScanContext> context;
    ASSERT_TRUE(context_mgr.create_scan_context(&context).ok());
    ASSERT_NE(nullptr, context);

    TScanCloseParams params;
    params.__set_context_id(context->context_id);
    TScanCloseResult result;
    orchestrator.close_scanner(result, params);

    EXPECT_EQ(TStatusCode::OK, result.status.status_code);

    std::shared_ptr<ScanContext> missing;
    EXPECT_FALSE(context_mgr.get_scan_context(context->context_id, &missing).ok());
}

} // namespace starrocks::orchestration
