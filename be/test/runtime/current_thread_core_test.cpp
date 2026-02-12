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

} // namespace

} // namespace starrocks
