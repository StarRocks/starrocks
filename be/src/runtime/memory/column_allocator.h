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

#include <glog/logging.h>

#include "runtime/memory/mem_hook_allocator.h"

namespace starrocks {

extern MemHookAllocator kDefaultColumnAllocator;
inline thread_local Allocator* tls_column_allocator = &kDefaultColumnAllocator;

// Implement the std::allocator: https://en.cppreference.com/w/cpp/memory/allocator
template <class T>
class ColumnAllocator {
public:
    typedef T value_type;
    typedef size_t size_type;
    using propagate_on_container_copy_assignment = std::true_type; // for consistency
    using propagate_on_container_move_assignment = std::true_type; // to avoid the pessimization
    using propagate_on_container_swap = std::true_type;            // to avoid the undefined behavior

    template <typename U>
    struct rebind {
        using other = ColumnAllocator<U>;
    };
    ColumnAllocator() = default;
    template <class U>
    ColumnAllocator(const ColumnAllocator<U>& other) {}

    ~ColumnAllocator() = default;

    // Allocator n elements, throw std::bad_malloc if allocate failed
    T* allocate(size_t n) {
        DCHECK(tls_column_allocator != nullptr);
        return static_cast<T*>(tls_column_allocator->checked_alloc(n * sizeof(T)));
    }

    void deallocate(T* ptr, size_t n) {
        DCHECK(tls_column_allocator != nullptr);
        tls_column_allocator->free(ptr);
    }

    ColumnAllocator& operator=(const ColumnAllocator& rhs) = default;

    template <class U>
    ColumnAllocator& operator=(const ColumnAllocator<U>& rhs) {
        return *this;
    }

    bool operator==(const ColumnAllocator& rhs) const { return true; }

    bool operator!=(const ColumnAllocator& rhs) const { return false; }

    void swap(ColumnAllocator& rhs) {}
};

class ThreadLocalColumnAllocatorSetter {
public:
    ThreadLocalColumnAllocatorSetter(Allocator* allocator) {
        _prev = tls_column_allocator;
        tls_column_allocator = allocator;
    }
    ~ThreadLocalColumnAllocatorSetter() { tls_column_allocator = _prev; }

private:
    Allocator* _prev = nullptr;
};
} // namespace starrocks