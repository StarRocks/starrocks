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

#include "storage/index/inverted/tantivy/tantivy_ffi_pool_bridge.h"

#include <tantivy_binding.h>

#include "common/logging.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "util/countdown_latch.h"
#include "util/threadpool.h"

namespace starrocks {

namespace tb = ::starrocks::tantivy_binding;

namespace {

// Shared pool the tantivy tasks run on. Owned by ExecEnv; borrowed here.
ThreadPool* g_tantivy_index_pool = nullptr;

// Join handle backing a joinable submit. A single-count latch flipped when the
// task finishes; `sr_tantivy_pool_join` waits on it and frees the handle.
struct PoolJoinHandle {
    CountDownLatch latch{1};
};

// Run a boxed Rust task with `tracker` installed as the current thread's mem
// tracker. This is the crux of the fix: the task (allocated on the submitting
// thread, e.g. a compaction/load thread) is run here on a pool thread with the
// SAME tracker, so allocations and their matching frees both charge to it —
// `consume` and `release` no longer land on different trackers.
void run_with_tracker(void* task, MemTracker* tracker) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(tracker);
    tb::tantivy_binding_run_pool_task(task);
}

// Submit a joinable task. Matches `tantivy_binding::TantivyPoolSubmitFn`.
void* sr_tantivy_pool_submit(void* task) {
    MemTracker* tracker = CurrentThread::mem_tracker();
    auto* handle = new PoolJoinHandle();
    if (g_tantivy_index_pool != nullptr) {
        auto st = g_tantivy_index_pool->submit_func([task, tracker, handle]() {
            run_with_tracker(task, tracker);
            handle->latch.count_down();
        });
        if (st.ok()) {
            return handle;
        }
        LOG(WARNING) << "tantivy: submit joinable task to pool failed: " << st.to_string()
                     << ", running inline";
    }
    // Fallback (pool absent or submit rejected): run inline on the caller thread
    // so tantivy keeps making progress. The caller's tracker is already active.
    tb::tantivy_binding_run_pool_task(task);
    handle->latch.count_down();
    return handle;
}

// Submit a fire-and-forget task. Matches `TantivyPoolSubmitDetachedFn`.
void sr_tantivy_pool_submit_detached(void* task) {
    MemTracker* tracker = CurrentThread::mem_tracker();
    if (g_tantivy_index_pool != nullptr) {
        auto st = g_tantivy_index_pool->submit_func([task, tracker]() { run_with_tracker(task, tracker); });
        if (st.ok()) {
            return;
        }
        LOG(WARNING) << "tantivy: submit detached task to pool failed: " << st.to_string()
                     << ", running inline";
    }
    tb::tantivy_binding_run_pool_task(task);
}

// Wait for a joinable task to finish and release the handle. Matches
// `TantivyPoolJoinFn`.
void sr_tantivy_pool_join(void* handle) {
    if (handle == nullptr) {
        return;
    }
    auto* h = static_cast<PoolJoinHandle*>(handle);
    h->latch.wait();
    delete h;
}

} // namespace

void register_tantivy_index_thread_pool(ThreadPool* pool) {
    g_tantivy_index_pool = pool;
    tb::tantivy_binding_init_thread_pool(&sr_tantivy_pool_submit, &sr_tantivy_pool_submit_detached,
                                         &sr_tantivy_pool_join);
}

} // namespace starrocks
