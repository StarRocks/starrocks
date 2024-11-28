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

#include <type_traits>

#include "runtime/memory/allocator.h"
#include "runtime/memory/roaring_hook.h"

namespace starrocks {

inline thread_local Allocator* tls_scope_counting_allocator = nullptr;

template <class T>
class ScopeCountingAllocator {
public:
    typedef T value_type;
    typedef size_t size_type;
    using propagate_on_container_copy_assignment = std::true_type; // for consistency
    using propagate_on_container_move_assignment = std::true_type; // to avoid the pessimization
    using propagate_on_container_swap = std::true_type;            // to avoid the undefined behavior

    template <typename U>
    struct rebind {
        using other = ScopeCountingAllocator<U>;
    };
    ScopeCountingAllocator() = default;
    template <class U>
    ScopeCountingAllocator(const ScopeCountingAllocator<U>& other) {}

    ~ScopeCountingAllocator() = default;

    T* allocate(size_t n) {
        DCHECK(tls_scope_counting_allocator != nullptr);
        return static_cast<T*>(tls_scope_counting_allocator->checked_alloc(n * sizeof(T)));
    }

    void deallocate(T* ptr, size_t n) {
        DCHECK(tls_scope_counting_allocator != nullptr);
        tls_scope_counting_allocator->free(ptr);
    }

    ScopeCountingAllocator& operator=(const ScopeCountingAllocator& rhs) = default;

    template <class U>
    ScopeCountingAllocator& operator=(const ScopeCountingAllocator<U>& rhs) {
        return *this;
    }

    bool operator==(const ScopeCountingAllocator& rhs) const { return true; }

    bool operator!=(const ScopeCountingAllocator& rhs) const { return false; }

    void swap(ScopeCountingAllocator& rhs) {}
};
template <class T>
void swap(ScopeCountingAllocator<T>& lhs, ScopeCountingAllocator<T>& rhs) {
    lhs.swap(rhs);
}

class ThreadLocalScopeCountingAllocatorSetter {
public:
    ThreadLocalScopeCountingAllocatorSetter(Allocator* allocator) {
        _prev = tls_scope_counting_allocator;
        tls_scope_counting_allocator = allocator;
    }
    ~ThreadLocalScopeCountingAllocatorSetter() { tls_scope_counting_allocator = _prev; }

private:
    Allocator* _prev = nullptr;
};
#define SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(allocator) \
    auto VARNAME_LINENUM(alloc_setter) = ThreadLocalScopeCountingAllocatorSetter(allocator)

// Thread local aggregate state allocator setter with roaring allocator
class ThreadLocalStateAllocatorSetter {
public:
    ThreadLocalStateAllocatorSetter(Allocator* allocator)
            : _scope_counting_allocator_setter(allocator), _roaring_allocator_setter(allocator) {}
    ~ThreadLocalStateAllocatorSetter() = default;

private:
    ThreadLocalScopeCountingAllocatorSetter _scope_counting_allocator_setter;
    ThreadLocalRoaringAllocatorSetter _roaring_allocator_setter;
};

#define SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(allocator) \
    auto VARNAME_LINENUM(alloc_setter) = ThreadLocalStateAllocatorSetter(allocator)

} // namespace starrocks
