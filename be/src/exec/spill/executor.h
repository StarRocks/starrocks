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

#include <memory>
#include <tuple>
#include <utility>

#include "common/compiler_util.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/query_context.h"
#include "exec/workgroup/scan_executor.h"
#include "gen_cpp/Types_types.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::spill {
struct TraceInfo {
    TraceInfo(RuntimeState* state) : query_id(state->query_id()), fragment_id(state->fragment_instance_id()) {}
    TUniqueId query_id;
    TUniqueId fragment_id;
};

struct EmptyMemGuard {
    bool scoped_begin() const { return true; }
    void scoped_end() const {}
};

struct MemTrackerGuard {
    MemTrackerGuard(MemTracker* scope_tracker_) : scope_tracker(scope_tracker_) {}
    bool scoped_begin() const {
        old_tracker = tls_thread_status.set_mem_tracker(scope_tracker);
        return true;
    }
    void scoped_end() const { tls_thread_status.set_mem_tracker(old_tracker); }
    MemTracker* scope_tracker;
    mutable MemTracker* old_tracker = nullptr;
};

template <class... WeakPtrs>
struct ResourceMemTrackerGuard {
    ResourceMemTrackerGuard(MemTracker* scope_tracker_, WeakPtrs&&... args)
            : scope_tracker(scope_tracker_), resources(std::make_tuple(args...)) {}

    bool scoped_begin() const {
        auto res = capture(resources);
        if (!res.has_value()) {
            return false;
        }
        captured = std::move(res.value());
        old_tracker = tls_thread_status.set_mem_tracker(scope_tracker);
        return true;
    }

    void scoped_end() const {
        tls_thread_status.set_mem_tracker(old_tracker);
        captured = {};
    }

private:
    auto capture(const std::tuple<WeakPtrs...>& weak_tup) const
            -> std::optional<std::tuple<std::shared_ptr<typename WeakPtrs::element_type>...>> {
        auto shared_ptrs = std::make_tuple(std::get<WeakPtrs>(weak_tup).lock()...);
        bool all_locked = ((std::get<WeakPtrs>(weak_tup).lock() != nullptr) && ...);
        if (all_locked) {
            return shared_ptrs;
        } else {
            return std::nullopt;
        }
    }

    MemTracker* scope_tracker;
    std::tuple<WeakPtrs...> resources;

    mutable std::tuple<std::shared_ptr<typename WeakPtrs::element_type>...> captured;
    mutable MemTracker* old_tracker = nullptr;
};

struct IOTaskExecutor {
    workgroup::ScanExecutor* pool;
    workgroup::WorkGroupPtr wg;

    IOTaskExecutor(workgroup::ScanExecutor* pool_, workgroup::WorkGroupPtr wg_) : pool(pool_), wg(std::move(wg_)) {}

    template <class Func>
    Status submit(Func&& func) {
        workgroup::ScanTask task(wg.get(), func);
        if (pool->submit(std::move(task))) {
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

#define RESOURCE_TLS_MEMTRACER_GUARD(state, ...) \
    spill::ResourceMemTrackerGuard(tls_mem_tracker, state->query_ctx()->weak_from_this(), ##__VA_ARGS__)

#define TRACKER_WITH_SPILLER_GUARD(state, spiller) RESOURCE_TLS_MEMTRACER_GUARD(state, spiller->weak_from_this())

} // namespace starrocks::spill