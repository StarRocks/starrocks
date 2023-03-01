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

CurrentThread::~CurrentThread() {
    StorageEngine* storage_engine = StorageEngine::instance();
    if (UNLIKELY(storage_engine != nullptr && storage_engine->bg_worker_stopped())) {
        tls_is_thread_status_init = false;
        return;
    }
    mem_tracker_ctx_shift();
    tls_is_thread_status_init = false;
}

starrocks::MemTracker* CurrentThread::mem_tracker() {
    if (UNLIKELY(tls_mem_tracker == nullptr)) {
        if (ExecEnv::is_init()) {
            tls_mem_tracker = ExecEnv::GetInstance()->process_mem_tracker();
        }
    }
    return tls_mem_tracker;
}

starrocks::MemTracker* CurrentThread::operator_mem_tracker() {
    return tls_operator_mem_tracker;
}

CurrentThread& CurrentThread::current() {
    return tls_thread_status;
}

} // namespace starrocks
