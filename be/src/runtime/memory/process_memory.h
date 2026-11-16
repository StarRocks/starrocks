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
#include <utility>

#include "runtime/current_thread.h"
#include "runtime/runtime_env.h"

namespace starrocks {

// Shared cache metadata has a process lifetime, not the lifetime of the task that
// happened to create or evict it. Do not use the manually maintained metadata
// trackers here: they are not children of the process tracker.
inline MemTracker* process_memory_tracker() {
    return RuntimeEnv::is_init() ? RuntimeEnv::GetInstance()->process_mem_tracker() : nullptr;
}

// Accounts storage only. In particular, construct/destroy are intentionally NOT
// scoped: the object's referenced payload may have a different owner. Callers
// must separately arrange the allocation and destruction of owned members.
template <class T>
class ProcessMemoryAllocator {
public:
    using value_type = T;
    using is_always_equal = std::true_type;

    ProcessMemoryAllocator() = default;
    template <class U>
    ProcessMemoryAllocator(const ProcessMemoryAllocator<U>&) noexcept {}

    T* allocate(size_t n) {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(process_memory_tracker());
        return std::allocator<T>().allocate(n);
    }

    void deallocate(T* ptr, size_t n) noexcept {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(process_memory_tracker());
        std::allocator<T>().deallocate(ptr, n);
    }

    template <class U>
    bool operator==(const ProcessMemoryAllocator<U>&) const noexcept {
        return true;
    }

    template <class U>
    bool operator!=(const ProcessMemoryAllocator<U>&) const noexcept {
        return false;
    }
};

// A failed operation can allocate its returned Status under the owner. Copy the
// error to the caller's tracker and destroy the original under the owner. Merely
// returning it across the scope boundary creates another cross-task imbalance.
// No allocation limits or exception-catching flags are disabled by this helper.
template <class Callable>
Status with_memory_tracker(MemTracker* owner, Callable&& operation) {
    auto* caller = CurrentThread::mem_tracker();
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(owner);
    auto status = std::forward<Callable>(operation)();
    if (status.ok()) {
        return status;
    }
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(caller);
    return Status(status);
}

template <class Callable>
Status with_process_memory(bool enabled, Callable&& operation) {
    if (!enabled) {
        return std::forward<Callable>(operation)();
    }
    return with_memory_tracker(process_memory_tracker(), std::forward<Callable>(operation));
}

} // namespace starrocks
