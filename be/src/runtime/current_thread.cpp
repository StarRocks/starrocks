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
#include "runtime/mem_tracker.h"

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

std::unique_ptr<ThreadMemoryMigrator> ThreadMemoryMigrator::migrate_to(MemTracker* tracker) {
    DCHECK(CurrentThread::mem_tracker() != tracker) << "should not transfer to current tracker";

    return std::make_unique<ThreadMemoryMigrator>(tracker);
}

void ThreadMemoryMigrator::consume(int64_t bytes) {
    _bytes = bytes;
    // record the memory consumption in both PROCESS & TARGET:
    // 1. The real memory release would happen at PROCESS, so consume it first
    // 2. The target tracker also needs to be aware of that memory consumption
    CurrentThread::current().mem_release(bytes);
    GlobalEnv::GetInstance()->process_mem_tracker()->consume(bytes);
    _target_tracker->consume_without_root(bytes);
}

ThreadMemoryMigrator::~ThreadMemoryMigrator() {
    // TODO: consider support using an individual tracker
    DCHECK(CurrentThread::mem_tracker()->type() == MemTracker::PROCESS) << "must release memory in PROCESS_MEM_TRACKER";

    _target_tracker->release_without_root(_bytes);
}

} // namespace starrocks
