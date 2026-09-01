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

#include "exec/pipeline/ai/ai_query_memory_account.h"

#include <limits>
#include <memory>
#include <utility>

#include "base/logging.h"
#include "base/testutil/sync_point.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "compute_env/workgroup/work_group.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

template <typename T>
class AIProcessScopeAllocator {
public:
    using value_type = T;

    explicit AIProcessScopeAllocator(MemTracker* process_tracker) noexcept : _process_tracker(process_tracker) {}

    template <typename U>
    AIProcessScopeAllocator(const AIProcessScopeAllocator<U>& other) noexcept
            : _process_tracker(other.process_tracker()) {}

    T* allocate(size_t count) {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_process_tracker);
        return std::allocator<T>{}.allocate(count);
    }

    void deallocate(T* pointer, size_t count) noexcept {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_process_tracker);
        TEST_SYNC_POINT("AIQueryMemoryAccount::control_block_deallocate:in_physical_scope");
        std::allocator<T>{}.deallocate(pointer, count);
    }

    MemTracker* process_tracker() const noexcept { return _process_tracker; }

    template <typename U>
    bool operator==(const AIProcessScopeAllocator<U>& other) const noexcept {
        return _process_tracker == other.process_tracker();
    }

private:
    MemTracker* _process_tracker;
};

struct AIProcessScopeDeleter {
    MemTracker* process_tracker;

    void operator()(AIQueryMemoryAccount* account) const noexcept {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(process_tracker);
        TEST_SYNC_POINT("AIQueryMemoryAccount::destroy:in_physical_scope");
        delete account;
    }
};

class AIMemoryContextOwner {
public:
    AIMemoryContextOwner(std::shared_ptr<AIQueryMemoryAccount> account, MemTracker* process_tracker)
            : _account(std::move(account)), _process_tracker(process_tracker) {}

    static AIMemoryContext create(std::shared_ptr<AIQueryMemoryAccount> account) {
        MemTracker* process_tracker = account->_process_tracker;
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(process_tracker);
        auto* owner = new AIMemoryContextOwner(std::move(account), process_tracker);
        return AIMemoryContext::create(owner, &AIMemoryContextOwner::reserve, &AIMemoryContextOwner::release_bytes,
                                       &AIMemoryContextOwner::run, &AIMemoryContextOwner::retain,
                                       &AIMemoryContextOwner::release_owner);
    }

    static bool reserve(void* opaque, size_t bytes) noexcept {
        return static_cast<AIMemoryContextOwner*>(opaque)->_account->_reserve(bytes);
    }

    static void release_bytes(void* opaque, size_t bytes) noexcept {
        static_cast<AIMemoryContextOwner*>(opaque)->_account->_release(bytes);
    }

    static void run(void* opaque, AIMemoryContext::Action action, void* action_context) {
        auto* owner = static_cast<AIMemoryContextOwner*>(opaque);
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(owner->_process_tracker);
        action(action_context);
    }

    static void retain(void* opaque) noexcept {
        static_cast<AIMemoryContextOwner*>(opaque)->_references.fetch_add(1, std::memory_order_relaxed);
    }

    static void release_owner(void* opaque) noexcept {
        auto* owner = static_cast<AIMemoryContextOwner*>(opaque);
        if (owner->_references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(owner->_process_tracker);
            TEST_SYNC_POINT("AIQueryMemoryAccount::context_owner_destroy:in_physical_scope");
            delete owner;
        }
    }

private:
    std::atomic<size_t> _references{0};
    std::shared_ptr<AIQueryMemoryAccount> _account;
    MemTracker* _process_tracker;
};

StatusOr<AIQueryMemoryAccount::Ptr> AIQueryMemoryAccount::create(const RuntimeState& state) {
    QueryContextLifetimeWeakPtr query_lifetime = state.query_ctx_lifetime();
    auto query_lifetime_pin = query_lifetime.lock();
    if (query_lifetime_pin == nullptr) {
        return Status::InvalidArgument("AI query memory account requires a live QueryContext");
    }

    const FragmentRuntimeState* fragment_state = state.fragment_runtime_state();
    if (fragment_state == nullptr) {
        return Status::InvalidArgument("AI query memory account requires a FragmentRuntimeState");
    }

    workgroup::WorkGroupPtr workgroup_owner = fragment_state->workgroup();
    if (workgroup_owner == nullptr) {
        return Status::InvalidArgument("AI query memory account requires a WorkGroup");
    }
    if (workgroup_owner->mem_tracker() == nullptr) {
        return Status::InvalidArgument("AI query memory account requires an initialized WorkGroup memory tracker");
    }

    std::shared_ptr<MemTracker> query_tracker = state.query_mem_tracker_ptr();
    if (query_tracker == nullptr) {
        return Status::InvalidArgument("AI query memory account requires a query memory tracker");
    }
    if (query_tracker->parent() != workgroup_owner->mem_tracker()) {
        return Status::InvalidArgument("AI query memory tracker must be a direct child of the WorkGroup tracker");
    }
    MemTracker* root_tracker = query_tracker.get();
    while (root_tracker->parent() != nullptr) {
        root_tracker = root_tracker->parent();
    }
    if (root_tracker->type() != MemTrackerType::PROCESS) {
        return Status::InvalidArgument("AI query memory tracker hierarchy must terminate at the process tracker");
    }

    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(root_tracker);
        auto* account = new AIQueryMemoryAccount(std::move(query_lifetime), std::move(workgroup_owner),
                                                 std::move(query_tracker), root_tracker);
        return Ptr(account, AIProcessScopeDeleter{root_tracker},
                   AIProcessScopeAllocator<AIQueryMemoryAccount>{root_tracker});
    }
}

AIQueryMemoryAccount::AIQueryMemoryAccount(QueryContextLifetimeWeakPtr query_lifetime,
                                           workgroup::WorkGroupPtr workgroup_owner,
                                           std::shared_ptr<MemTracker> query_tracker, MemTracker* process_tracker)
        : _query_lifetime(std::move(query_lifetime)),
          _workgroup_owner(std::move(workgroup_owner)),
          _query_tracker(std::move(query_tracker)),
          _process_tracker(process_tracker) {}

AIQueryMemoryAccount::~AIQueryMemoryAccount() noexcept {
    _drain_residual();
    DCHECK_EQ(0, reserved_bytes());
}

void AIQueryMemoryAccount::_drain_residual() noexcept {
    try {
        std::lock_guard lock(_mutex);
        const int64_t residual = _reserved_bytes.load(std::memory_order_relaxed);
        if (residual <= 0) {
            return;
        }
        _query_tracker->release_without_root(residual);
        _reserved_bytes.store(0, std::memory_order_relaxed);
        LOG(WARNING) << "Drained residual AI query memory reservation during account destruction";
    } catch (...) {
        LOG(WARNING) << "Failed to drain residual AI query memory reservation during account destruction";
    }
}

AIMemoryContext AIQueryMemoryAccount::memory_context() {
    return AIMemoryContextOwner::create(shared_from_this());
}

bool AIQueryMemoryAccount::_reserve(size_t bytes) noexcept {
    if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        return false;
    }

    try {
        // Keep QueryContext alive only for this admission decision and tracker update. The account retains no strong
        // QueryContext reference after this method returns.
        auto query_lifetime_pin = _query_lifetime.lock();
        if (query_lifetime_pin == nullptr) {
            return false;
        }

        const int64_t signed_bytes = static_cast<int64_t>(bytes);
        std::lock_guard lock(_mutex);
        const int64_t current = _reserved_bytes.load(std::memory_order_relaxed);
        if (signed_bytes > std::numeric_limits<int64_t>::max() - current) {
            return false;
        }
        if (_query_tracker->try_consume_without_root(signed_bytes) != nullptr) {
            return false;
        }
        _reserved_bytes.store(current + signed_bytes, std::memory_order_relaxed);
        return true;
    } catch (...) {
        // Memory-accounting hooks cross asynchronous/native boundaries and must fail closed instead of propagating.
        return false;
    }
}

void AIQueryMemoryAccount::_release(size_t bytes) noexcept {
    if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        return;
    }

    try {
        const int64_t signed_bytes = static_cast<int64_t>(bytes);
        std::lock_guard lock(_mutex);
        const int64_t current = _reserved_bytes.load(std::memory_order_relaxed);
        if (signed_bytes > current) {
            return;
        }
        _query_tracker->release_without_root(signed_bytes);
        _reserved_bytes.store(current - signed_bytes, std::memory_order_relaxed);
    } catch (...) {
        // Keep the reservation visible if a future MemTracker implementation can throw during release.
    }
}

} // namespace starrocks::pipeline
