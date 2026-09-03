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

#include "common/process_exit.h"

#include <gtest/gtest.h>

#include <atomic>

#include "base/time/time.h"
#include "common/config_exec_env_fwd.h"
#include "common/status.h"

namespace starrocks {

extern std::atomic<bool> k_starrocks_exit;
extern std::atomic<bool> k_starrocks_quick_exit;
extern std::atomic<bool> k_starrocks_force_reject;
extern std::atomic<int64_t> k_starrocks_exit_start_ms;
extern std::atomic<int64_t> k_starrocks_fe_aware_shutdown_ms;

using namespace ::testing;

class ProcessExitTest : public testing::Test {
    void SetUp() override {
        _old_reject_delay_ms = config::graceful_exit_reject_delay_ms;
        _old_reject_fallback_ms = config::graceful_exit_reject_fallback_ms;
        _old_wait_for_frontend_heartbeat = config::graceful_exit_wait_for_frontend_heartbeat;
        config::graceful_exit_wait_for_frontend_heartbeat = true;
        config::graceful_exit_reject_delay_ms = 5000;
        config::graceful_exit_reject_fallback_ms = 15000;
    }
    void TearDown() override {
        config::graceful_exit_wait_for_frontend_heartbeat = _old_wait_for_frontend_heartbeat;
        config::graceful_exit_reject_delay_ms = _old_reject_delay_ms;
        config::graceful_exit_reject_fallback_ms = _old_reject_fallback_ms;
        k_starrocks_exit.store(false);
        k_starrocks_quick_exit.store(false);
        k_starrocks_force_reject.store(false);
        k_starrocks_exit_start_ms.store(0);
        k_starrocks_fe_aware_shutdown_ms.store(0);
    }

private:
    int64_t _old_reject_fallback_ms;
    int64_t _old_reject_delay_ms;
    bool _old_wait_for_frontend_heartbeat;
};

TEST_F(ProcessExitTest, testExitFlag) {
    // no exit at all
    EXPECT_FALSE(process_exit_in_progress());
    EXPECT_FALSE(process_quick_exit_in_progress());

    // first time set exit, return true
    EXPECT_TRUE(set_process_exit());

    EXPECT_TRUE(process_exit_in_progress());
    EXPECT_FALSE(process_quick_exit_in_progress());

    // second time set exit, return false because it is already set to true
    EXPECT_FALSE(set_process_exit());

    // verify the exit status remain the same
    EXPECT_TRUE(process_exit_in_progress());
    EXPECT_FALSE(process_quick_exit_in_progress());
}

TEST_F(ProcessExitTest, testQuickExitFlag) {
    // no exit at all
    EXPECT_FALSE(process_exit_in_progress());
    EXPECT_FALSE(process_quick_exit_in_progress());

    // first time set exit, return true
    EXPECT_TRUE(set_process_quick_exit());

    EXPECT_TRUE(process_exit_in_progress());
    EXPECT_TRUE(process_quick_exit_in_progress());

    // second time set exit, return false because it is already set to true
    EXPECT_FALSE(set_process_quick_exit());

    // verify the exit status remain the same
    EXPECT_TRUE(process_exit_in_progress());
    EXPECT_TRUE(process_quick_exit_in_progress());
}

TEST_F(ProcessExitTest, testShouldAcceptNotExiting) {
    EXPECT_TRUE(should_accept_new_request());
}

TEST_F(ProcessExitTest, testShouldNotAcceptForceReject) {
    ASSERT_TRUE(set_process_exit());
    k_starrocks_force_reject.store(true);
    EXPECT_FALSE(should_accept_new_request());
}

TEST_F(ProcessExitTest, testForceRejectExecPlanFragment) {
    ASSERT_TRUE(set_process_exit());
    force_reject_exec_plan_fragment();
    EXPECT_TRUE(k_starrocks_force_reject.load());
}

TEST_F(ProcessExitTest, testShouldNotAcceptQuickExit) {
    ASSERT_TRUE(set_process_quick_exit());
    EXPECT_FALSE(should_accept_new_request());
}

TEST_F(ProcessExitTest, testShouldAcceptUntilFrontendAware) {
    ASSERT_TRUE(set_process_exit());
    EXPECT_TRUE(should_accept_new_request());
    set_frontend_aware_of_exit();
    EXPECT_TRUE(should_accept_new_request());
}

TEST_F(ProcessExitTest, testShouldNotAcceptAfterFrontendAwareDelay) {
    ASSERT_TRUE(set_process_exit());
    k_starrocks_fe_aware_shutdown_ms.store(MonotonicMillis() - config::graceful_exit_reject_delay_ms - 1);
    EXPECT_FALSE(should_accept_new_request());
}
TEST_F(ProcessExitTest, testShouldNotAcceptAfterFallbackWithoutFrontendAware) {
    ASSERT_TRUE(set_process_exit());
    k_starrocks_exit_start_ms.store(MonotonicMillis() - config::graceful_exit_reject_fallback_ms - 1);
    EXPECT_FALSE(should_accept_new_request());
}

TEST_F(ProcessExitTest, testShouldNotAcceptImmediatelyWhenHeartbeatWaitDisabled) {
    config::graceful_exit_wait_for_frontend_heartbeat = false;
    ASSERT_TRUE(set_process_exit());
    // New requests reject immediately when heartbeat waiting is disabled.
    EXPECT_FALSE(should_accept_new_request());
}

TEST_F(ProcessExitTest, testShouldAcceptDuringHeartbeatDelay) {
    ASSERT_TRUE(set_process_exit());
    set_frontend_aware_of_exit();
    EXPECT_TRUE(should_accept_new_request());

    k_starrocks_fe_aware_shutdown_ms.store(MonotonicMillis() - config::graceful_exit_reject_delay_ms - 1);
    EXPECT_FALSE(should_accept_new_request());
}
TEST_F(ProcessExitTest, testRequestAdmissionGuardClosesWithForceReject) {
    {
        RequestAdmissionGuard guard;
        EXPECT_TRUE(guard.accepted());
    }

    force_reject_exec_plan_fragment();

    RequestAdmissionGuard guard;
    EXPECT_FALSE(guard.accepted());
}
} // namespace starrocks
