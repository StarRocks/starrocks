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

#include "runtime/current_thread.h"

#include <atomic>

#include "common/logging.h"

namespace starrocks {

namespace {

// CurrentThread is not a standard-layout type under newer toolchains, so
// offsetof(CurrentThread, ...) triggers -Winvalid-offsetof. Compute offsets
// from the live TLS object instead.
template <typename Member>
size_t tls_member_offset(Member CurrentThread::*member) {
    const auto& current_thread = CurrentThread::current();
    const auto* base = reinterpret_cast<const uint8_t*>(&current_thread);
    const auto* field = reinterpret_cast<const uint8_t*>(&(current_thread.*member));
    return static_cast<size_t>(field - base);
}

} // namespace

// The TP-relative offset of tls_thread_status, written once at startup by
// init_tls_thread_status_offset().  External profilers (e.g. query_cpu_profile.py)
// can read this global from /proc/PID/mem to obtain the exact TLS offset without
// needing ELF arithmetic or DTV walking.
// Value is 0 until init_tls_thread_status_offset() is called.
volatile int64_t g_tls_thread_status_tpoff = 0;

void init_tls_thread_status_offset() {
    // __builtin_thread_pointer() returns the thread-pointer register value:
    //   x86-64 : FS base  (TLS Variant II, negative offsets)
    //   aarch64: TPIDR_EL0 (TLS Variant I,  positive offsets)
    // Supported by GCC >= 4.8 and Clang >= 3.5 on both architectures.
    auto tp = reinterpret_cast<uintptr_t>(__builtin_thread_pointer());
    if (tp != 0) {
        g_tls_thread_status_tpoff =
                static_cast<int64_t>(reinterpret_cast<uintptr_t>(&tls_thread_status)) - static_cast<int64_t>(tp);
    }

    LOG(INFO) << "[eBPF] tls_thread_status tpoff=" << g_tls_thread_status_tpoff
              << " query_id_offset=" << CurrentThread::query_id_offset()
              << " module_type_offset=" << CurrentThread::module_type_offset();
}

namespace {

bool default_is_env_initialized() {
    return false;
}

MemTracker* default_process_mem_tracker() {
    return nullptr;
}

std::atomic<CurrentThread::IsEnvInitializedFn> s_is_env_initialized{default_is_env_initialized};
std::atomic<CurrentThread::ProcessMemTrackerFn> s_process_mem_tracker{default_process_mem_tracker};

} // namespace

void CurrentThread::set_mem_tracker_source(IsEnvInitializedFn is_env_initialized,
                                           ProcessMemTrackerFn process_mem_tracker) {
    s_is_env_initialized.store(is_env_initialized == nullptr ? default_is_env_initialized : is_env_initialized,
                               std::memory_order_release);
    s_process_mem_tracker.store(process_mem_tracker == nullptr ? default_process_mem_tracker : process_mem_tracker,
                                std::memory_order_release);
}

size_t CurrentThread::query_id_offset() {
    return tls_member_offset(&CurrentThread::_query_id);
}

size_t CurrentThread::module_type_offset() {
    return tls_member_offset(&CurrentThread::_module_type);
}

CurrentThread::~CurrentThread() {
    auto is_env_initialized = s_is_env_initialized.load(std::memory_order_acquire);
    if (!is_env_initialized()) {
        tls_is_thread_status_init = false;
        return;
    }
    mem_tracker_ctx_shift();
    tls_is_thread_status_init = false;
}

starrocks::MemTracker* CurrentThread::mem_tracker() {
    auto is_env_initialized = s_is_env_initialized.load(std::memory_order_acquire);
    if (LIKELY(is_env_initialized())) {
        if (UNLIKELY(tls_mem_tracker == nullptr)) {
            auto process_mem_tracker = s_process_mem_tracker.load(std::memory_order_acquire);
            tls_mem_tracker = process_mem_tracker();
        }
        return tls_mem_tracker;
    } else {
        return nullptr;
    }
}

starrocks::MemTracker* CurrentThread::singleton_check_mem_tracker() {
    return tls_singleton_check_mem_tracker;
}

CurrentThread& CurrentThread::current() {
    return tls_thread_status;
}

} // namespace starrocks
