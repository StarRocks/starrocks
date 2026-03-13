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

#include "runtime/current_thread.h"

namespace starrocks {

namespace {

bool g_env_initialized = false;
MemTracker* g_process_mem_tracker = nullptr;

bool is_env_initialized_for_test() {
    return g_env_initialized;
}

MemTracker* process_mem_tracker_for_test() {
    return g_process_mem_tracker;
}

class CurrentThreadCoreTest : public testing::Test {
protected:
    void SetUp() override { _reset(); }
    void TearDown() override { _reset(); }

private:
    static void _reset() {
        CurrentThread::set_mem_tracker_source(nullptr, nullptr);
        tls_mem_tracker = nullptr;
        g_env_initialized = false;
        g_process_mem_tracker = nullptr;
    }
};

TEST_F(CurrentThreadCoreTest, default_provider_returns_null) {
    EXPECT_EQ(CurrentThread::mem_tracker(), nullptr);
}

TEST_F(CurrentThreadCoreTest, custom_provider_with_uninitialized_env_returns_null) {
    MemTracker process_tracker;
    g_env_initialized = false;
    g_process_mem_tracker = &process_tracker;
    CurrentThread::set_mem_tracker_source(is_env_initialized_for_test, process_mem_tracker_for_test);

    EXPECT_EQ(CurrentThread::mem_tracker(), nullptr);
}

TEST_F(CurrentThreadCoreTest, custom_provider_with_initialized_env_returns_process_tracker) {
    MemTracker process_tracker;
    g_env_initialized = true;
    g_process_mem_tracker = &process_tracker;
    CurrentThread::set_mem_tracker_source(is_env_initialized_for_test, process_mem_tracker_for_test);

    EXPECT_EQ(CurrentThread::mem_tracker(), &process_tracker);
}

TEST_F(CurrentThreadCoreTest, mem_tracker_uses_tls_cache_until_reset) {
    MemTracker process_tracker1;
    MemTracker process_tracker2;
    g_env_initialized = true;
    g_process_mem_tracker = &process_tracker1;
    CurrentThread::set_mem_tracker_source(is_env_initialized_for_test, process_mem_tracker_for_test);

    EXPECT_EQ(CurrentThread::mem_tracker(), &process_tracker1);
    g_process_mem_tracker = &process_tracker2;
    EXPECT_EQ(CurrentThread::mem_tracker(), &process_tracker1);

    tls_mem_tracker = nullptr;
    EXPECT_EQ(CurrentThread::mem_tracker(), &process_tracker2);
}

TEST_F(CurrentThreadCoreTest, reset_provider_to_default_behaves_as_pre_init) {
    MemTracker process_tracker;
    g_env_initialized = true;
    g_process_mem_tracker = &process_tracker;
    CurrentThread::set_mem_tracker_source(is_env_initialized_for_test, process_mem_tracker_for_test);
    EXPECT_EQ(CurrentThread::mem_tracker(), &process_tracker);

    CurrentThread::set_mem_tracker_source(nullptr, nullptr);
    tls_mem_tracker = nullptr;
    EXPECT_EQ(CurrentThread::mem_tracker(), nullptr);
}

// Verify that CurrentThreadMemTrackerSetter restores the correct tracker even when TLS is
// polluted during the nullptr scope. This simulates what happens when:
// 1. LoadChannelMgr sets tls_mem_tracker to load_tracker (outer SCOPED_THREAD_LOCAL_MEM_SETTER)
// 2. Before bthread yield, SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr) resets TLS to nullptr
// 3. During yield, another bthread overwrites TLS with a different tracker (e.g., query_pool)
// 4. After yield, the RAII guard must restore load_tracker (saved on stack), not the polluted value
TEST_F(CurrentThreadCoreTest, mem_tracker_setter_restores_after_tls_pollution) {
    MemTracker process_tracker(-1, "process");
    MemTracker load_tracker(-1, "load", &process_tracker);
    MemTracker query_pool_tracker(-1, "query_pool", &process_tracker);

    // Simulate: outer scope sets load_tracker (LoadChannelMgr::add_chunk)
    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&load_tracker);
        EXPECT_EQ(tls_mem_tracker, &load_tracker);

        // Simulate: inner scope resets to nullptr before yield (tablets_channel yield point)
        {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
            EXPECT_EQ(tls_mem_tracker, nullptr);

            // Simulate: TLS pollution during bthread yield
            // (another bthread sets tls_mem_tracker to query_pool tracker)
            tls_mem_tracker = &query_pool_tracker;
        }
        // After inner guard destructor: load_tracker should be restored from stack,
        // NOT the polluted query_pool_tracker
        EXPECT_EQ(tls_mem_tracker, &load_tracker);
    }
    // After outer guard destructor: nullptr should be restored (original value)
    EXPECT_EQ(tls_mem_tracker, nullptr);
}

// Verify the full two-layer pattern used for the query_pool negative memory fix:
// Layer 1: LoadChannelMgr sets load_tracker for the whole handler scope
// Layer 2: tablets_channel resets to nullptr around yield, restores load_tracker after
// Memory allocated under load_tracker and freed under load_tracker → net zero on query_pool
TEST_F(CurrentThreadCoreTest, two_layer_tracker_pattern_prevents_negative_query_pool) {
    MemTracker process_tracker(-1, "process");
    MemTracker load_tracker(-1, "load", &process_tracker);
    MemTracker query_pool_tracker(-1, "query_pool", &process_tracker);

    // Initial state: no tracker (typical for brpc handler bthread)
    ASSERT_EQ(tls_mem_tracker, nullptr);

    // Layer 1: LoadChannelMgr::add_chunk sets load tracker
    {
        SCOPED_THREAD_LOCAL_MEM_SETTER(&load_tracker, false);
        EXPECT_EQ(tls_mem_tracker, &load_tracker);

        // Chunk deserialization happens here — tracked under load_tracker

        // Layer 2: before yield, reset to nullptr
        {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
            EXPECT_EQ(tls_mem_tracker, nullptr);

            // Simulate: bthread yield + TLS pollution by DataStreamRecvr
            tls_mem_tracker = &query_pool_tracker;

            // DataStreamRecvr::add_chunks would see prev_tracker == nullptr (safe for DCHECK)
            // since we set nullptr before the yield
        }

        // After yield: load_tracker restored (chunk will be freed under this tracker)
        EXPECT_EQ(tls_mem_tracker, &load_tracker);
    }

    // Verify: query_pool should NOT have gone negative
    // (In real scenario, alloc under load_tracker and free under load_tracker → net zero on query_pool)
    EXPECT_EQ(query_pool_tracker.consumption(), 0);
    EXPECT_EQ(tls_mem_tracker, nullptr);
}

} // namespace

} // namespace starrocks
