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
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "base/utility/defer_op.h"
#include "compute_env/spill/runtime_context.h"
#include "gutil/macros.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"

namespace starrocks::spill {

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
            : scope_tracker(scope_tracker_), resources(std::forward<WeakPtrs>(args)...) {}

    bool scoped_begin() const {
        auto res = capture();
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
    using ResourceTuple = std::tuple<std::decay_t<WeakPtrs>...>;
    using CapturedTuple = std::tuple<std::shared_ptr<typename std::decay_t<WeakPtrs>::element_type>...>;

    auto capture() const -> std::optional<CapturedTuple> {
        return capture_impl(std::index_sequence_for<WeakPtrs...>{});
    }

    template <size_t... Is>
    auto capture_impl(std::index_sequence<Is...>) const -> std::optional<CapturedTuple> {
        CapturedTuple shared_ptrs(std::get<Is>(resources).lock()...);
        bool all_locked = ((std::get<Is>(shared_ptrs) != nullptr) && ...);
        if (all_locked) {
            return shared_ptrs;
        } else {
            return std::nullopt;
        }
    }

    MemTracker* scope_tracker;
    ResourceTuple resources;

    mutable CapturedTuple captured;
    mutable MemTracker* old_tracker = nullptr;
};

} // namespace starrocks::spill

#define DEFER_GUARD_END(guard) auto VARNAME_LINENUM(defer) = DeferOp([&]() { guard.scoped_end(); });

#define RESOURCE_TLS_MEMTRACER_GUARD(state, ...) \
    spill::ResourceMemTrackerGuard(tls_mem_tracker, spill::spill_query_ctx_lifetime(state), ##__VA_ARGS__)

#define TRACKER_WITH_SPILLER_GUARD(state, spiller) RESOURCE_TLS_MEMTRACER_GUARD(state, spiller->weak_from_this())

#define TRACKER_WITH_SPILLER_RES_GUARD(state, spiller, ...) \
    RESOURCE_TLS_MEMTRACER_GUARD(state, spiller->weak_from_this(), ##__VA_ARGS__)

#define TRACKER_WITH_SPILLER_READER_GUARD(state, spiller) \
    RESOURCE_TLS_MEMTRACER_GUARD(state, spiller->weak_from_this(), std::weak_ptr((spiller)->reader()))
