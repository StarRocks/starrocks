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

#include <thread>

#include "runtime/exec_env.h"

namespace starrocks {

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

starrocks::MemTracker* CurrentThread::operator_mem_tracker() {
    return tls_operator_mem_tracker;
}

starrocks::MemTracker* CurrentThread::singleton_check_mem_tracker() {
    return tls_singleton_check_mem_tracker;
}

CurrentThread& CurrentThread::current() {
    return tls_thread_status;
}

void CurrentThread::set_scan_seq(int32_t operator_id, int32_t scan_seq) {
    _operator_id = operator_id;
    _scan_seq = scan_seq;
}

int32_t CurrentThread::get_unique_execution_id() const {
    if (_driver_id > 0 || _scan_seq > 0) {
        // Assumption:
        // scan_seq at most 512
        // operators at most 131072
        return (_driver_id << 16) + (_operator_id << 8) + _scan_seq;
    }
    return std::hash<std::thread::id>()(std::this_thread::get_id());
}

} // namespace starrocks
