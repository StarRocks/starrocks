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
#include <memory>
#include <string>

#include "common/config_runtime_fwd.h"
#include "common/status.h"
#include "common/system/cpu_info.h"
#include "compute_env/compute_env.h"
#include "exec/runtime/query_context_manager.h"
#include "gen_cpp/StatusCode_types.h"
#include "orchestration/external_scan_context_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"

namespace starrocks::orchestration {

class ExternalScanOrchestratorTest : public testing::Test {
protected:
    static void SetUpTestSuite() {
        CpuInfo::init();
        config::fragment_pool_thread_num_min = 1;
        config::fragment_pool_thread_num_max = 1;
        config::fragment_pool_queue_size = 64;
    }

    void SetUp() override {
        ComputeEnvOptions options;
        options.max_num_pipeline_drivers = 1;
        ASSERT_TRUE(_exec_env.compute_env()->init(options).ok());
        _exec_env._fragment_mgr = new FragmentMgr(&_exec_env, nullptr);
        _exec_env._query_context_mgr = new pipeline::QueryContextManager(5);
        _exec_env._refresh_service_contexts();
    }

    void TearDown() override {
        delete _exec_env._fragment_mgr;
        _exec_env._fragment_mgr = nullptr;
        delete _exec_env._query_context_mgr;
        _exec_env._query_context_mgr = nullptr;
        _exec_env.compute_env()->destroy();
    }

    ExecEnv _exec_env;
};

TEST_F(ExternalScanOrchestratorTest, open_scanner_returns_context_for_invalid_batch_size) {
    ExternalScanContextMgr context_mgr(&_exec_env, nullptr);
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
    ExternalScanContextMgr context_mgr(&_exec_env, nullptr);
    ExternalScanOrchestrator orchestrator(&_exec_env, &context_mgr);

    TScanNextBatchParams params;
    params.__set_context_id("missing");
    params.__set_offset(0);
    TScanBatchResult result;
    orchestrator.get_next(result, params);

    EXPECT_EQ(TStatusCode::NOT_FOUND, result.status.status_code);
}

TEST_F(ExternalScanOrchestratorTest, get_next_offset_mismatch_returns_not_found_and_refreshes_access_time) {
    ExternalScanContextMgr context_mgr(&_exec_env, nullptr);
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
    ExternalScanContextMgr context_mgr(&_exec_env, nullptr);
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
