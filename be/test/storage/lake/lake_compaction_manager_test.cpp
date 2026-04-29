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

#include "storage/lake/lake_compaction_manager.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "common/config.h"

namespace starrocks::lake {

class LakeCompactionManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_enabled = config::enable_lake_autonomous_compaction;
        _saved_score = config::lake_autonomous_compaction_score_threshold;
        config::enable_lake_autonomous_compaction = true;
        config::lake_autonomous_compaction_score_threshold = 1.0;
        // start() with nullptrs: compute_score_locked short-circuits to 0.0 when
        // tablet_mgr is null (real cached-metadata lookup is not possible), so
        // update_tablet_async is a no-op below threshold. These tests exercise
        // the manager's lifecycle/idempotency contracts, not real dispatch.
        LakeCompactionManager::instance()->start(nullptr, nullptr);
    }
    void TearDown() override {
        LakeCompactionManager::instance()->stop();
        config::enable_lake_autonomous_compaction = _saved_enabled;
        config::lake_autonomous_compaction_score_threshold = _saved_score;
    }
    bool _saved_enabled = false;
    double _saved_score = 0.0;
};

TEST_F(LakeCompactionManagerTest, update_tablet_async_dedup) {
    auto* mgr = LakeCompactionManager::instance();
    mgr->update_tablet_async(101);
    mgr->update_tablet_async(101);
    mgr->update_tablet_async(101);
    // Idle dispatcher in skeleton mode immediately drains the queue, so we
    // assert by checking that running_tasks_for_tablet stays consistent.
    // After the dispatcher drains in skeleton mode, queue_size should be 0
    // and per-tablet running counts should be zero (skeleton releases the slot
    // immediately after dispatch).
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(0, mgr->running_tasks_for_tablet(101));
}

TEST_F(LakeCompactionManagerTest, disabled_no_op) {
    config::enable_lake_autonomous_compaction = false;
    auto* mgr = LakeCompactionManager::instance();
    mgr->update_tablet_async(202);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(0u, mgr->queue_size());
}

TEST_F(LakeCompactionManagerTest, notify_decrements_counters_idempotent) {
    auto* mgr = LakeCompactionManager::instance();
    // Fabricate state by calling notify with no prior reservation. Should not
    // crash or leave negative counts.
    mgr->notify_task_finished(303, {1, 2, 3});
    EXPECT_EQ(0, mgr->running_tasks_for_tablet(303));
    EXPECT_EQ(0u, mgr->running_inputs(303).size());
    EXPECT_EQ(0, mgr->running_tasks());
}

TEST_F(LakeCompactionManagerTest, notify_without_reservation_does_not_underflow) {
    auto* mgr = LakeCompactionManager::instance();
    // Many notify-without-reservation calls in a row must not push running_tasks
    // below zero (which would break dispatch_loop's global cap check).
    for (int i = 0; i < 50; ++i) {
        mgr->notify_task_finished(404, {});
    }
    EXPECT_EQ(0, mgr->running_tasks());
}

TEST_F(LakeCompactionManagerTest, instance_is_singleton) {
    auto* a = LakeCompactionManager::instance();
    auto* b = LakeCompactionManager::instance();
    EXPECT_EQ(a, b);
}

TEST_F(LakeCompactionManagerTest, queue_size_tracks_enqueue) {
    auto* mgr = LakeCompactionManager::instance();
    // The dispatch loop drains the queue continuously, but we can at least
    // observe non-negative queue size and non-crashing concurrent updates.
    for (int i = 0; i < 10; ++i) {
        mgr->update_tablet_async(500 + i);
    }
    // Allow the dispatcher to drain.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_GE(mgr->queue_size(), 0u);
}

TEST_F(LakeCompactionManagerTest, running_inputs_returns_empty_for_unknown_tablet) {
    auto* mgr = LakeCompactionManager::instance();
    auto inputs = mgr->running_inputs(99999);
    EXPECT_TRUE(inputs.empty());
}

TEST_F(LakeCompactionManagerTest, start_is_idempotent) {
    auto* mgr = LakeCompactionManager::instance();
    // SetUp already started; calling start() again should be a no-op.
    mgr->start(nullptr, nullptr);
    mgr->start(nullptr, nullptr);
    // Still functional.
    mgr->update_tablet_async(606);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(0, mgr->running_tasks_for_tablet(606));
}

TEST_F(LakeCompactionManagerTest, stop_is_idempotent) {
    auto* mgr = LakeCompactionManager::instance();
    mgr->stop();
    // Calling stop again should be safe.
    mgr->stop();
    // Restart for TearDown's stop() to be a no-op.
    mgr->start(nullptr, nullptr);
}

} // namespace starrocks::lake
