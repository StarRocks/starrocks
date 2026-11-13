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

#include "cache/mem_space_monitor.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "base/testutil/sync_point.h"
#include "base/time/monotime.h"
#include "cache/datacache.h"
#include "cache/mem_cache/lrucache_engine.h"
#include "common/config_cache_fwd.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

// The monitor is constructed without a DataCache or a process MemTracker. The
// background loop bails out at the null tracker check on every iteration, so
// none of these tests dereference _datacache. They only exercise the thread
// lifecycle, which is what broke BE startup failures: destroying a started
// monitor without stop() used to destroy a joinable std::thread and terminate.

TEST(MemSpaceMonitorTest, destruct_without_stop) {
    {
        MemSpaceMonitor monitor(nullptr, nullptr);
        monitor.start();
        ASSERT_TRUE(monitor._adjust_datacache_thread.joinable());
        // Leave scope without calling stop(). Must not std::terminate.
    }
    SUCCEED();
}

TEST(MemSpaceMonitorTest, destruct_via_shared_ptr_without_stop) {
    // Mirrors how DataCache owns the monitor and how exit() tears it down.
    auto monitor = std::make_shared<MemSpaceMonitor>(nullptr, nullptr);
    monitor->start();
    monitor.reset();
    SUCCEED();
}

TEST(MemSpaceMonitorTest, stop_is_idempotent) {
    MemSpaceMonitor monitor(nullptr, nullptr);
    monitor.start();
    monitor.stop();
    ASSERT_FALSE(monitor._adjust_datacache_thread.joinable());
    monitor.stop();
    ASSERT_FALSE(monitor._adjust_datacache_thread.joinable());
}

TEST(MemSpaceMonitorTest, start_is_idempotent) {
    MemSpaceMonitor monitor(nullptr, nullptr);
    monitor.start();
    auto first_id = monitor._adjust_datacache_thread.get_id();
    ASSERT_TRUE(monitor._adjust_datacache_thread.joinable());
    // A second start() must not move-assign over the running thread.
    monitor.start();
    ASSERT_EQ(first_id, monitor._adjust_datacache_thread.get_id());
    monitor.stop();
    ASSERT_FALSE(monitor._adjust_datacache_thread.joinable());
}

TEST(MemSpaceMonitorTest, stop_without_start) {
    MemSpaceMonitor monitor(nullptr, nullptr);
    ASSERT_FALSE(monitor._adjust_datacache_thread.joinable());
    monitor.stop();
    ASSERT_FALSE(monitor._adjust_datacache_thread.joinable());
}

TEST(MemSpaceMonitorTest, restart_after_stop) {
    MemSpaceMonitor monitor(nullptr, nullptr);
    monitor.start();
    monitor.stop();
    monitor.start();
    ASSERT_TRUE(monitor._adjust_datacache_thread.joinable());
    monitor.stop();
    ASSERT_FALSE(monitor._adjust_datacache_thread.joinable());
}

class MemSpaceMonitorAdjustmentTest : public ::testing::Test {
protected:
    static constexpr int64_t kGB = 1024L * 1024 * 1024;
    static constexpr int64_t kCeiling = 20 * kGB;

    void SetUp() override {
        config::datacache_enable = true;
        config::enable_datacache_mem_auto_adjust = true;
        config::datacache_mem_adjust_period = 20;
        config::datacache_mem_adjust_interval_seconds = 10;
        config::datacache_mem_size = "20%";
        config::memory_high_level = 75;
        config::memory_urgent_level = 90;
    }

    void TearDown() override {
        auto* sp = SyncPoint::GetInstance();
        sp->DisableProcessing();
        sp->ClearAllCallBacks();
        config::datacache_enable = _enabled;
        config::enable_datacache_mem_auto_adjust = _auto_adjust;
        config::datacache_mem_adjust_period = _period;
        config::datacache_mem_adjust_interval_seconds = _interval;
        config::datacache_mem_size = _size;
        config::memory_high_level = _high;
        config::memory_urgent_level = _urgent;
    }

    // Run the actual callback with a real LRU and process tracker. Only the clock and wait are replaced, so a
    // 30-minute idle period takes no wall time. Return the cache capacity after each adjustment round.
    std::vector<int64_t> run_rounds(const std::vector<int64_t>& usages, int64_t initial_capacity) {
        DataCache cache;
        cache._process_mem_limit = 100 * kGB;
        auto lru = std::make_shared<LRUCacheEngine>();
        EXPECT_TRUE(lru->init(MemCacheOptions{.mem_space_size = static_cast<size_t>(initial_capacity)}).ok());
        cache._local_mem_cache = lru;
        MemTracker tracker(100 * kGB);
        MemSpaceMonitor monitor(&cache, &tracker);
        MonoTime now = MonoTime::Now();
        size_t round = 0;
        std::vector<int64_t> capacities;
        auto* sp = SyncPoint::GetInstance();
        sp->SetCallBack("MemSpaceMonitor::now", [&](void* arg) {
            now += MonoDelta::FromSeconds(10);
            *static_cast<MonoTime*>(arg) = now;
        });
        sp->SetCallBack("MemSpaceMonitor::wait", [&](void* arg) {
            *static_cast<int64_t*>(arg) = 0;
            if (round > 0) {
                capacities.push_back(cache.get_mem_capacity());
            }
            if (round == usages.size()) {
                monitor._stopped = true;
            } else {
                tracker.set(usages[round++]);
            }
        });
        sp->EnableProcessing();
        monitor._stopped = false;
        monitor._adjust_datacache_callback();
        sp->DisableProcessing();
        sp->ClearAllCallBacks();
        return capacities;
    }

private:
    bool _enabled = config::datacache_enable;
    bool _auto_adjust = config::enable_datacache_mem_auto_adjust;
    int64_t _period = config::datacache_mem_adjust_period;
    int64_t _interval = config::datacache_mem_adjust_interval_seconds;
    std::string _size = config::datacache_mem_size.value();
    int64_t _high = config::memory_high_level;
    int64_t _urgent = config::memory_urgent_level;
};

TEST_F(MemSpaceMonitorAdjustmentTest, second_pressure_episode_shrinks_gradually) {
    // The 10 GB excess disappears abruptly. At the ceiling, neither advisor has work for the next 30 minutes.
    std::vector<int64_t> usages{85 * kGB};
    usages.insert(usages.end(), 180, 75 * kGB);
    usages.insert(usages.end(), 21, 80 * kGB);
    auto capacities = run_rounds(usages, kCeiling);
    ASSERT_EQ(usages.size(), capacities.size());
    EXPECT_EQ(kCeiling, capacities[180]);
    EXPECT_EQ(kCeiling, capacities[181]);
    EXPECT_LT(capacities[182], capacities[181]);
    EXPECT_GT(capacities[183], kCeiling - 5 * kGB);
    EXPECT_LT(capacities.back(), capacities[183]);
}

TEST_F(MemSpaceMonitorAdjustmentTest, growth_resumes_gradually_after_pressure) {
    // Both the high and urgent branches must age the idle growth advisor. Its previous headroom was 10 GB;
    // the new one is only 5 GB, which used to be granted in full in the first round after pressure.
    for (int64_t pressure : {85 * kGB, 95 * kGB}) {
        SCOPED_TRACE(pressure);
        std::vector<int64_t> usages{65 * kGB};
        usages.insert(usages.end(), 180, pressure);
        usages.insert(usages.end(), 21, 70 * kGB);
        auto capacities = run_rounds(usages, 5 * kGB);
        ASSERT_EQ(usages.size(), capacities.size());
        int64_t shrunk = capacities[180];
        EXPECT_LT(shrunk, 5 * kGB);
        EXPECT_EQ(shrunk, capacities[181]);
        EXPECT_GT(capacities[182], capacities[181]);
        EXPECT_LT(capacities[183], shrunk + 5 * kGB);
        EXPECT_GT(capacities.back(), capacities[183]);
        EXPECT_LE(capacities.back(), kCeiling);
    }
}

TEST_F(MemSpaceMonitorAdjustmentTest, alternating_pressure_still_releases_memory) {
    // Reconstructing advisors on direction changes would release 0 forever in this case.
    std::vector<int64_t> usages;
    for (int i = 0; i < 30; ++i) {
        usages.push_back(80 * kGB);
        usages.push_back(75 * kGB);
    }
    auto capacities = run_rounds(usages, kCeiling);
    ASSERT_EQ(usages.size(), capacities.size());
    EXPECT_EQ(kCeiling, capacities.front());
    EXPECT_LT(capacities.back(), kCeiling);
}

} // namespace starrocks
