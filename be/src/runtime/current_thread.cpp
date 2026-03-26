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

#include "runtime/exec_env.h"
#include "storage/storage_engine.h"

namespace starrocks {

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
}

CurrentThread::~CurrentThread() {
    if (!GlobalEnv::is_init()) {
        tls_is_thread_status_init = false;
        return;
    }
    mem_tracker_ctx_shift();
    tls_is_thread_status_init = false;
}

starrocks::MemTracker* CurrentThread::mem_tracker() {
    if (LIKELY(GlobalEnv::is_init())) {
        if (UNLIKELY(tls_mem_tracker == nullptr)) {
            tls_mem_tracker = GlobalEnv::GetInstance()->process_mem_tracker();
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
