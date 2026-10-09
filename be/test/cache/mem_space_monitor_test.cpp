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

} // namespace starrocks
