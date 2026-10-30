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

TEST_F(CurrentThreadCoreTest, exported_offsets_match_tls_layout) {
    const auto* base = reinterpret_cast<const uint8_t*>(&tls_thread_status);
    const auto* query_id = reinterpret_cast<const uint8_t*>(&tls_thread_status._query_id);
    const auto* module_type = reinterpret_cast<const uint8_t*>(&tls_thread_status._module_type);

    EXPECT_EQ(CurrentThread::query_id_offset(), static_cast<size_t>(query_id - base));
    EXPECT_EQ(CurrentThread::module_type_offset(), static_cast<size_t>(module_type - base));
}

TEST_F(CurrentThreadCoreTest, transfer_keeps_common_ancestor_unchanged) {
    constexpr int64_t kib = 1024;
    MemTracker process_tracker(2176 * kib, "process");
    MemTracker sender(-1, "sender", &process_tracker);
    MemTracker receiver(-1, "receiver", &process_tracker);
    g_env_initialized = true;
    g_process_mem_tracker = &process_tracker;
    CurrentThread::set_mem_tracker_source(is_env_initialized_for_test, process_mem_tracker_for_test);
    CurrentThreadMemTrackerSetter sender_scope(&sender);
    auto& thread = CurrentThread::current();
    const int64_t consumed_before = thread.get_consumed_bytes();
    thread.mem_consume(1984 * kib);
    DeferOp cleanup([&] {
        thread.mem_release(1984 * kib);
        receiver.release(receiver.consumption());
        thread.mem_tracker_ctx_shift();
    });
    ASSERT_TRUE(thread.try_mem_consume(128 * kib));
    EXPECT_EQ(2112 * kib, process_tracker.consumption());

    thread.mem_transfer_to(&receiver, 128 * kib);
    EXPECT_EQ(consumed_before + 1984 * kib, thread.get_consumed_bytes());
    EXPECT_EQ(1984 * kib, sender.consumption());
    EXPECT_EQ(128 * kib, receiver.consumption());
    EXPECT_EQ(2112 * kib, process_tracker.consumption());
    EXPECT_EQ(2112 * kib, process_tracker.peak_consumption());
    EXPECT_EQ(nullptr, sender.find_limit_exceeded_tracker());
    thread.mem_tracker_ctx_shift();
    EXPECT_EQ(2112 * kib, process_tracker.consumption());
    EXPECT_EQ(2112 * kib, process_tracker.peak_consumption());
}

TEST_F(CurrentThreadCoreTest, transfer_from_does_not_spend_reservation) {
    constexpr int64_t mib = 1024 * 1024;
    MemTracker process(-1, "process");
    MemTracker buffer(-1, "buffer", &process);
    MemTracker receiver(-1, "receiver", &process);
    g_env_initialized = true;
    g_process_mem_tracker = &process;
    CurrentThread::set_mem_tracker_source(is_env_initialized_for_test, process_mem_tracker_for_test);
    CurrentThreadMemTrackerSetter scope(&receiver);
    auto& thread = CurrentThread::current();
    buffer.consume(128);
    ASSERT_TRUE(thread.try_mem_reserve(2 * mib, 0));
    const int64_t before = thread.get_consumed_bytes();
    thread.mem_transfer_from(&buffer, 128);
    EXPECT_EQ(before + 128, thread.get_consumed_bytes());
    EXPECT_EQ(2 * mib + 128, receiver.consumption());
    EXPECT_EQ(2 * mib + 128, process.consumption());
    EXPECT_EQ(0, buffer.consumption());
    thread.release_reserved();
    thread.mem_tracker_ctx_shift();
    EXPECT_EQ(128, receiver.consumption());
    EXPECT_EQ(128, process.consumption());
    thread.mem_release(128);
    thread.mem_tracker_ctx_shift();
    EXPECT_EQ(0, process.consumption());
}

TEST_F(CurrentThreadCoreTest, transfer_does_not_replenish_exhausted_reservation) {
    constexpr int64_t kib = 1024;
    constexpr int64_t reserved_bytes = 2048 * kib;
    MemTracker process_tracker(-1, "process");
    MemTracker sender(-1, "sender", &process_tracker);
    MemTracker receiver(-1, "receiver", &process_tracker);
    g_env_initialized = true;
    g_process_mem_tracker = &process_tracker;
    CurrentThread::set_mem_tracker_source(is_env_initialized_for_test, process_mem_tracker_for_test);
    CurrentThreadMemTrackerSetter sender_scope(&sender);
    auto& thread = CurrentThread::current();
    ASSERT_TRUE(thread.try_mem_reserve(reserved_bytes, 0));
    thread.mem_consume(reserved_bytes + 1984 * kib);
    DeferOp cleanup([&] {
        thread.release_reserved();
        thread.mem_release(reserved_bytes + 1984 * kib);
        receiver.release(receiver.consumption());
        thread.mem_tracker_ctx_shift();
    });
    const int64_t before_clone = thread.get_consumed_bytes();
    ASSERT_TRUE(thread.try_mem_consume(128 * kib));
    thread.mem_transfer_to(&receiver, thread.get_consumed_bytes() - before_clone);
    EXPECT_EQ(before_clone, thread.get_consumed_bytes());
    EXPECT_EQ(reserved_bytes + 1984 * kib, sender.consumption());
    EXPECT_EQ(128 * kib, receiver.consumption());
    EXPECT_EQ(reserved_bytes + 2112 * kib, process_tracker.consumption());
    EXPECT_EQ(reserved_bytes + 2112 * kib, process_tracker.peak_consumption());

    // A transfer must not make the next allocation consume a recycled reservation.
    thread.mem_consume(64 * kib);
    EXPECT_EQ(before_clone + 64 * kib, thread.get_consumed_bytes());
    thread.mem_release(64 * kib);
    thread.release_reserved();
    thread.mem_tracker_ctx_shift();
    EXPECT_EQ(reserved_bytes + 2112 * kib, process_tracker.consumption());
}

} // namespace

} // namespace starrocks
