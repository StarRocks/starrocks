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

CurrentThread::CurrentThread(): _mem_cache_manager(this, std::bind(&CurrentThread::get_mem_tracker,this)),
    _operator_mem_cache_manager(this, std::bind(&CurrentThread::get_operator_mem_tracker, this)) {
    // if (!bthread_self()) {
    //     tls_is_thread_status_init = true;
    // }
    if (LIKELY(GlobalEnv::is_init())) {
        _mem_tracker = GlobalEnv::GetInstance()->process_mem_tracker();
        _is_init = true;
    }
}

CurrentThread::~CurrentThread() {
    if (!GlobalEnv::is_init()) {
        if (!bthread_self()) {
            tls_is_thread_status_init = false;
        }
        tls_is_thread_status_init = false;
        _is_init = false;
        return;
    }
    mem_tracker_ctx_shift();
    _is_init = false;
    if (!bthread_self()) {
        tls_is_thread_status_init = false;
    }
}
void CurrentThread::init() {
    if (_is_init) {
        return;
    }
    if (LIKELY(GlobalEnv::is_init())) {
        _mem_tracker = GlobalEnv::GetInstance()->process_mem_tracker();
        _is_init = true;
    }
}

// starrocks::MemTracker* CurrentThread::mem_tracker() {
//     if (LIKELY(GlobalEnv::is_init())) {
//         return CurrentThread::current()._mem_tracker;
//         // if (UNLIKELY(tls_mem_tracker == nullptr)) {
//         //     tls_mem_tracker = GlobalEnv::GetInstance()->process_mem_tracker();
//         // }
//         // return tls_mem_tracker;
//     } else {
//         return nullptr;
//     }
// }

// starrocks::MemTracker* CurrentThread::operator_mem_tracker() {
//     return CurrentThread::current()._operator_mem_tracker;
//     // return tls_operator_mem_tracker;
// }

MemTracker* CurrentThread::mem_tracker() {
    if (LIKELY(GlobalEnv::is_init())) {
        if (auto mem_tracker = CurrentThread::current().get_mem_tracker(); mem_tracker != nullptr) {
            return mem_tracker;
        }
        return GlobalEnv::GetInstance()->process_mem_tracker();
    }
    return nullptr;
}

MemTracker* CurrentThread::operator_mem_tracker() {
    // @TODO will be null?
    return CurrentThread::current().get_operator_mem_tracker();
}

CurrentThread& CurrentThread::current() {
    if (bthread_self()) {
        if (bthread_equal(bthread_self(), bthread_tls_id) == 0) {
            // bthread recorded in bthread_tls_id is not the current bthread, we should get it via bthread_get_specific
            bthread_tls_id = bthread_self();
            bthread_tls_thread_status = static_cast<CurrentThread*>(bthread_getspecific(bthread_tls_key));
            if (bthread_tls_thread_status == nullptr) {
                // LOG(INFO) << "no bthread local status for bthread: " << bthread_self();
                // if we didn't create manually, just use pthread local
                // no bthread local thread status, use pthread
                // @TODO is it safe?

                // @TODO if bthread destruct and it hold tls_thread_status, maybe not safe
                bthread_tls_thread_status = &tls_thread_status;
            }
        }
        // LOG(INFO) << "get bthread tls";
        // DCHECK(bthread_tls_thread_status != nullptr);
        // return bthread_tls_thread_status;
        // @TODO remove it
        // @TODO in which case, bthread_tls_thread_status can be null
        if (bthread_tls_thread_status == nullptr) {
            bthread_tls_thread_status = &tls_thread_status;
        }
        return *bthread_tls_thread_status;
        // return tls_thread_status;
    }
    // @TODO set tls mem tracker
    // @TODO we need init mem tracker some where
    return tls_thread_status;
}

CurrentThread* CurrentThread::current_ptr() {
    if (bthread_self()) {
        if (bthread_equal(bthread_self(), bthread_tls_id) == 0) {
            // bthread recorded in bthread_tls_id is not the current bthread, we should get it via bthread_get_specific
            bthread_tls_id = bthread_self();
            bthread_tls_thread_status = static_cast<CurrentThread*>(bthread_getspecific(bthread_tls_key));
            if (bthread_tls_thread_status == nullptr) {
                bthread_tls_thread_status = &tls_thread_status;
            }
        }
        // LOG(INFO) << "get bthread tls";
        // DCHECK(bthread_tls_thread_status != nullptr);
        // return bthread_tls_thread_status;
        // @TODO remove it
        // @TODO in which case, bthread_tls_thread_status can be null
        if (bthread_tls_thread_status == nullptr) {
            bthread_tls_thread_status = &tls_thread_status;
        }
        return bthread_tls_thread_status;
    }
    return &tls_thread_status;
}
// MemTracker* CurrentThread::current_mem_tracker() {
//     if (LIKELY(GlobalEnv::is_init())) {
//         if (auto mem_tracker = CurrentThread::current().mem_tracker(); mem_tracker != nullptr) {
//             return mem_tracker;
//         } else {
//             // @TODO log
//         }
//         return GlobalEnv::GetInstance()->process_mem_tracker();
//     }
//     return nullptr;
// }

} // namespace starrocks
