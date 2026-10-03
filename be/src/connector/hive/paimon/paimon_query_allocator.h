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
#include <new>
#include <utility>

#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

// Binds allocate_shared's control block, object construction/destruction, and
// final deallocation to the query tracker. The allocator itself retains the
// tracker until a possibly cross-thread final release completes.
template <typename T>
class PaimonQueryAllocator {
public:
    using value_type = T;

    explicit PaimonQueryAllocator(std::shared_ptr<MemTracker> tracker) : _tracker(std::move(tracker)) {}

    template <typename U>
    PaimonQueryAllocator(const PaimonQueryAllocator<U>& other) : _tracker(other._tracker) {}

    T* allocate(size_t count) {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker.get());
        return std::allocator<T>().allocate(count);
    }

    void deallocate(T* ptr, size_t count) {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker.get());
        std::allocator<T>().deallocate(ptr, count);
    }

    template <typename U, typename... Args>
    void construct(U* ptr, Args&&... args) {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker.get());
        ::new (static_cast<void*>(ptr)) U(std::forward<Args>(args)...);
    }

    template <typename U>
    void destroy(U* ptr) {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker.get());
        ptr->~U();
    }

    template <typename U>
    bool operator==(const PaimonQueryAllocator<U>& other) const {
        return _tracker == other._tracker;
    }

    template <typename U>
    bool operator!=(const PaimonQueryAllocator<U>& other) const {
        return !(*this == other);
    }

private:
    template <typename U>
    friend class PaimonQueryAllocator;

    std::shared_ptr<MemTracker> _tracker;
};

} // namespace starrocks
