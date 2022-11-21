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

#pragma once

#include <utility>

#include "common/compiler_util.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {
struct EmptyMemGuard {
    void scoped_begin() const {}
    void scoped_end() const {}
};

struct MemTrackerGuard {
    MemTrackerGuard(MemTracker* scope_tracker_) : scope_tracker(scope_tracker_) {}
    void scoped_begin() const { old_tracker = tls_thread_status.set_mem_tracker(scope_tracker); }
    void scoped_end() const { tls_thread_status.set_mem_tracker(old_tracker); }
    MemTracker* scope_tracker;
    mutable MemTracker* old_tracker = nullptr;
};

struct IOTaskExecutor {
    IOTaskExecutor(PriorityThreadPool* pool_) : pool(pool_) {}
    PriorityThreadPool* pool;
    template <class Func>
    Status submit(Func&& func) {
        PriorityThreadPool::WorkFunction wf = std::move(func);
        if (pool->offer(wf)) {
            return Status::OK();
        } else {
            return Status::InternalError("offer task failed");
        }
    }
};

struct SyncTaskExecutor {
    template <class Func>
    Status submit(Func&& func) {
        std::forward<Func>(func)();
        return Status::OK();
    }
};

} // namespace starrocks