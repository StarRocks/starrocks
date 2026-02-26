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

namespace starrocks {

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
