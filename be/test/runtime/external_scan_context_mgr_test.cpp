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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/runtime/external_scan_context_mgr_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/external_scan_context_mgr.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "common/status.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/query_context.h"
#include "exec/workgroup/work_group.h"
#include "runtime/fragment_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

class ExternalScanContextMgrTest : public testing::Test {
public:
    ExternalScanContextMgrTest() {
        auto* fragment_mgr = new FragmentMgr(&_exec_env);
        auto* result_queue_mgr = new ResultQueueMgr();
        auto* query_ctx_mgr = new pipeline::QueryContextManager(5);
        _exec_env._fragment_mgr = fragment_mgr;
        _exec_env._result_queue_mgr = result_queue_mgr;
        _exec_env._query_context_mgr = query_ctx_mgr;
    }
    ~ExternalScanContextMgrTest() override {
        delete _exec_env._fragment_mgr;
        delete _exec_env._result_queue_mgr;
        delete _exec_env._query_context_mgr;
    }

protected:
    void SetUp() override {}

private:
    ExecEnv _exec_env;
};

TEST_F(ExternalScanContextMgrTest, create_normal) {
    std::shared_ptr<ScanContext> context;
    ExternalScanContextMgr context_mgr(&_exec_env);
    Status st = context_mgr.create_scan_context(&context);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(context != nullptr);
}

TEST_F(ExternalScanContextMgrTest, get_normal) {
    std::shared_ptr<ScanContext> context;
    ExternalScanContextMgr context_mgr(&_exec_env);
    Status st = context_mgr.create_scan_context(&context);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(context != nullptr);

    std::string context_id = context->context_id;
    std::shared_ptr<ScanContext> result;
    st = context_mgr.get_scan_context(context_id, &result);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(context != nullptr);
}

TEST_F(ExternalScanContextMgrTest, get_abnormal) {
    std::string context_id = "not_exist";
    std::shared_ptr<ScanContext> result;
    ExternalScanContextMgr context_mgr(&_exec_env);
    Status st = context_mgr.get_scan_context(context_id, &result);
    ASSERT_TRUE(!st.ok());
    ASSERT_TRUE(result == nullptr);
}

TEST_F(ExternalScanContextMgrTest, clear_context) {
    std::shared_ptr<ScanContext> context;
    ExternalScanContextMgr context_mgr(&_exec_env);
    Status st = context_mgr.create_scan_context(&context);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(context != nullptr);

    std::string context_id = context->context_id;
    st = context_mgr.clear_scan_context(context_id);
    ASSERT_TRUE(st.ok());

    std::shared_ptr<ScanContext> result;
    st = context_mgr.get_scan_context(context_id, &result);
    ASSERT_TRUE(!st.ok());
    ASSERT_TRUE(result == nullptr);
}

TEST_F(ExternalScanContextMgrTest, clear_scan_context_cancels_pipeline_fragment) {
    TUniqueId query_id;
    query_id.__set_hi(2026);
    query_id.__set_lo(715);
    TUniqueId fragment_id;
    fragment_id.__set_hi(2026);
    fragment_id.__set_lo(716);

    pipeline::QueryContext* query_ctx = nullptr;
    ASSIGN_OR_ASSERT_FAIL(query_ctx, _exec_env.query_context_mgr()->get_or_register(query_id));
    query_ctx->set_delivery_expire_seconds(60);
    query_ctx->set_query_expire_seconds(60);
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                GlobalEnv::GetInstance()->query_pool_mem_tracker());

    auto* fragment_ctx = query_ctx->fragment_mgr()->get_or_register(fragment_id);
    fragment_ctx->set_query_id(query_id);
    fragment_ctx->set_fragment_instance_id(fragment_id);
    fragment_ctx->set_workgroup(ExecEnv::GetInstance()->workgroup_manager()->get_default_workgroup());
    fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(query_id, fragment_id, TQueryOptions(), TQueryGlobals(), &_exec_env));

    ExternalScanContextMgr context_mgr(&_exec_env);
    std::shared_ptr<ScanContext> context;
    ASSERT_OK(context_mgr.create_scan_context(&context));
    context->query_id = query_id;
    context->fragment_instance_id = fragment_id;

    // clear_scan_context and the expired-context reaper share the same cancellation path
    // (_cancel_scan_context): the pipeline fragment must be cancelled, not the non-pipeline one.
    ASSERT_OK(context_mgr.clear_scan_context(context->context_id));

    ASSERT_TRUE(fragment_ctx->final_status().is_cancelled());
}

} // namespace starrocks
