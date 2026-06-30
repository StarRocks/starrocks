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
//   https://github.com/apache/incubator-doris/blob/master/be/test/runtime/fragment_mgr_test.cpp

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

#include "orchestration/fragment_mgr.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>

#include "base/time/monotime.h"
#include "common/config_runtime_fwd.h"
#include "exec/data_sink.h"
#include "exec/exec_env.h"
#include "gen_cpp/Types_types.h"
#include "orchestration/plan_fragment_executor.h"
#include "runtime/env/global_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks {

using orchestration::FragmentMgr;

// Mock used for this unittest
PlanFragmentExecutor::PlanFragmentExecutor(const QueryExecutionServices* query_execution_services,
                                           report_status_callback report_status_cb)
        : _query_execution_services(query_execution_services), _report_status_cb(std::move(report_status_cb)) {}

PlanFragmentExecutor::~PlanFragmentExecutor() = default;

Status PlanFragmentExecutor::prepare(const TExecPlanFragmentParams& request) {
    EXPECT_NE(runtime_state(), nullptr);
    EXPECT_NE(runtime_state()->runtime_filter_port(), nullptr);
    return Status::OK();
}

Status PlanFragmentExecutor::open() {
    SleepFor(MonoDelta::FromMilliseconds(50));
    return Status::OK();
}

void PlanFragmentExecutor::cancel() {}

void PlanFragmentExecutor::close() {}

void PlanFragmentExecutor::report_profile_once() {}

class FragmentMgrTest : public testing::Test {
public:
    FragmentMgrTest() = default;

    static void SetUpTestSuite() {
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

        _previous_global_env = ExecEnv::GetInstance()->_global_env;
        ExecEnv::GetInstance()->_global_env = global_env;
    }

    static void TearDownTestSuite() {
        auto* global_env = GlobalEnv::GetInstance();
        ExecEnv::GetInstance()->_global_env = _previous_global_env;
        _previous_global_env = nullptr;
        global_env->_query_pool_mem_tracker = _previous_query_pool_mem_tracker;
        global_env->_process_mem_tracker = _previous_process_mem_tracker;
        _previous_query_pool_mem_tracker.reset();
        _previous_process_mem_tracker.reset();
        _query_pool_mem_tracker.reset();
        _process_mem_tracker.reset();
    }

protected:
    void SetUp() override {
        config::fragment_pool_thread_num_min = 32;
        config::fragment_pool_thread_num_max = 32;
        config::fragment_pool_queue_size = 1024;
    }
    void TearDown() override {}

private:
    static GlobalEnv* _previous_global_env;
    static std::shared_ptr<MemTracker> _previous_process_mem_tracker;
    static std::shared_ptr<MemTracker> _previous_query_pool_mem_tracker;
    static std::shared_ptr<MemTracker> _process_mem_tracker;
    static std::shared_ptr<MemTracker> _query_pool_mem_tracker;
};

GlobalEnv* FragmentMgrTest::_previous_global_env = nullptr;
std::shared_ptr<MemTracker> FragmentMgrTest::_previous_process_mem_tracker;
std::shared_ptr<MemTracker> FragmentMgrTest::_previous_query_pool_mem_tracker;
std::shared_ptr<MemTracker> FragmentMgrTest::_process_mem_tracker;
std::shared_ptr<MemTracker> FragmentMgrTest::_query_pool_mem_tracker;

TEST_F(FragmentMgrTest, Normal) {
    FragmentMgr mgr(ExecEnv::GetInstance(), nullptr);
    TExecPlanFragmentParams params;
    params.params.fragment_instance_id = TUniqueId();
    params.params.fragment_instance_id.__set_hi(100);
    params.params.fragment_instance_id.__set_lo(200);
    ASSERT_TRUE(mgr.exec_plan_fragment(params).ok());
    // Duplicated
    ASSERT_TRUE(mgr.exec_plan_fragment(params).ok());
}

TEST_F(FragmentMgrTest, AddNormal) {
    FragmentMgr mgr(ExecEnv::GetInstance(), nullptr);
    for (int i = 0; i < 8; ++i) {
        TExecPlanFragmentParams params;
        params.params.fragment_instance_id = TUniqueId();
        params.params.fragment_instance_id.__set_hi(100 + i);
        params.params.fragment_instance_id.__set_lo(200);
        ASSERT_TRUE(mgr.exec_plan_fragment(params).ok());
    }
}

TEST_F(FragmentMgrTest, CancelNormal) {
    FragmentMgr mgr(ExecEnv::GetInstance(), nullptr);
    TExecPlanFragmentParams params;
    params.params.fragment_instance_id = TUniqueId();
    params.params.fragment_instance_id.__set_hi(100);
    params.params.fragment_instance_id.__set_lo(200);
    ASSERT_TRUE(mgr.exec_plan_fragment(params).ok());
    // Cancel after add
    ASSERT_TRUE(mgr.cancel(params.params.fragment_instance_id).ok());
}

TEST_F(FragmentMgrTest, CloseNornaml) {
    FragmentMgr mgr(ExecEnv::GetInstance(), nullptr);
    TExecPlanFragmentParams params;
    params.params.fragment_instance_id = TUniqueId();
    params.params.fragment_instance_id.__set_hi(100);
    params.params.fragment_instance_id.__set_lo(200);
    ASSERT_TRUE(mgr.exec_plan_fragment(params).ok());

    // Close after add, no dead lock
    mgr.close();

    // error when adding fragment after close()
    {
        TExecPlanFragmentParams params;
        params.params.fragment_instance_id = TUniqueId();
        params.params.fragment_instance_id.__set_hi(200);
        params.params.fragment_instance_id.__set_lo(300);
        auto st = mgr.exec_plan_fragment(params);
        EXPECT_FALSE(st.ok());
        EXPECT_TRUE(st.is_cancelled()) << "exec_plan_fragment() failed with error:" << st;
    }
}

TEST_F(FragmentMgrTest, CancelWithoutAdd) {
    FragmentMgr mgr(ExecEnv::GetInstance(), nullptr);
    TExecPlanFragmentParams params;
    params.params.fragment_instance_id = TUniqueId();
    params.params.fragment_instance_id.__set_hi(100);
    params.params.fragment_instance_id.__set_lo(200);
    ASSERT_TRUE(mgr.cancel(params.params.fragment_instance_id).ok());
}

TEST_F(FragmentMgrTest, RejectLegacyStreamPipeline) {
    FragmentMgr mgr(ExecEnv::GetInstance(), nullptr);
    TExecPlanFragmentParams params;
    params.params.fragment_instance_id = TUniqueId();
    params.params.fragment_instance_id.__set_hi(101);
    params.params.fragment_instance_id.__set_lo(201);
    params.__set_is_stream_pipeline(true);

    Status st = mgr.exec_plan_fragment(params);
    ASSERT_TRUE(st.is_not_supported()) << st;
    ASSERT_NE(st.message().find("Legacy incremental MV maintenance is no longer supported"), std::string::npos);
}

} // namespace starrocks
