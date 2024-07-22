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

#include <malloc.h>
#include <boost/mpl/always.hpp>
#include <iostream>
#include <memory>

#include "exprs/expr_context.h"
#include "runtime/memory/allocator.h"
#include "runtime/memory/mem_hook_allocator.h"

namespace starrocks {

template<class Base>
class CountingAllocatorT final : public Base {
public:
    ALWAYS_INLINE void* alloc(size_t size) {
        LOG(INFO) << "CountingAllocatorT::alloc";
        void* result = Base::alloc(size);
        _memory_usage += size;
        return result;
    }
    ALWAYS_INLINE void free(void* ptr) {
        Base::free(ptr);
        _memory_usage -= malloc_usable_size(ptr);
    }

    ALWAYS_INLINE void* realloc(void* ptr, size_t size) {
        int64_t old_size = malloc_usable_size(ptr);
        void* result = Base::realloc(ptr, size);
        // @TODO
        if (result != nullptr && result != ptr) {
            _memory_usage += size - old_size;
        }
        return result;
    }

    ALWAYS_INLINE void* calloc(size_t n, size_t size) {
        void* result = Base::calloc(n, size);
        _memory_usage += n * size;
        return result;
    }

    ALWAYS_INLINE void cfree(void* ptr) {
        ::free(ptr);
        _memory_usage -= malloc_usable_size(ptr);
    }

    ALWAYS_INLINE void* memalign(size_t align, size_t size) {
        void* result = Base::memalign(align, size);
        _memory_usage += malloc_usable_size(result);
        return result;
    }
    ALWAYS_INLINE void* aligned_alloc(size_t align, size_t size) {
        void* result = Base::aligned_alloc(align, size);
        _memory_usage += malloc_usable_size(result);
        return result;
    }

    ALWAYS_INLINE void* valloc(size_t size) {
        void* result = Base::valloc(size);
        _memory_usage += malloc_usable_size(result);
        return result;
    }

    ALWAYS_INLINE void* pvalloc(size_t size) {
        void* result = Base::pvalloc(size);
        // @TODO how to avoid invoke malloc_usable_size twice
        _memory_usage += malloc_usable_size(result);
        return result;
    }

    ALWAYS_INLINE int posix_memalign(void** ptr, size_t align, size_t size) {
        int result = Base::posix_memalign(ptr, align, size);
        if (LIKELY(result == 0)) {
            _memory_usage += malloc_usable_size(*ptr);
        }
        return result;
    }

    int64_t memory_usage() const {
        return _memory_usage;
    }

    int64_t _memory_usage = 0;
};

using CountingAllocatorWithHook = CountingAllocatorT<MemHookAllocator>;

// inline thread_local CountingAllocatorT* _counting_allocator = nullptr;
inline thread_local CountingAllocatorWithHook* tls_counting_allocator = nullptr;


// @TODO this is stl compitable allocator
template <class T>
class CountingAllocator {
public:
    typedef T value_type;
    typedef size_t size_type;
    using propagate_on_container_copy_assignment = std::true_type; // for consistency
    using propagate_on_container_move_assignment = std::true_type; // to avoid the pessimization
    using propagate_on_container_swap = std::true_type;            // to avoid the undefined behavior

    template <typename U>
    struct rebind {
        using other = CountingAllocator<U>;
    };
    CountingAllocator() = default;
    explicit CountingAllocator(int64_t* counter) : _counter(counter) {}
    explicit CountingAllocator(const CountingAllocator& rhs) : _counter(rhs._counter) {}
    template <class U>
    CountingAllocator(const CountingAllocator<U>& other) : _counter(other._counter) {}

    ~CountingAllocator() = default;

    CountingAllocator(CountingAllocator&& rhs) noexcept { std::swap(_counter, rhs._counter); }

    CountingAllocator& operator=(CountingAllocator&& rhs) noexcept {
        if (this != &rhs) {
            std::swap(_counter, rhs._counter);
        }
        return *this;
    }

    T* allocate(size_t n) {
        DCHECK(_counter != nullptr);
        T* result = static_cast<T*>(malloc(n * sizeof(T)));
        *_counter += (result != nullptr) ? n * sizeof(T) : 0;
        return result;
    }

    void deallocate(T* ptr, size_t n) {
        DCHECK(_counter != nullptr);
        free(ptr);
        *_counter -= n * sizeof(T);
    }

    CountingAllocator& operator=(const CountingAllocator& rhs) {
        _counter = rhs._counter;
        return *this;
    }

    template <class U>
    CountingAllocator& operator=(const CountingAllocator<U>& rhs) {
        _counter = rhs._counter;
        return *this;
    }

    bool operator==(const CountingAllocator& rhs) const { return _counter == rhs._counter; }

    bool operator!=(const CountingAllocator& rhs) const { return !(*this == rhs); }

    // CountingAllocator doesn't support swap
    void swap(CountingAllocator& rhs) {}

    int64_t* _counter = nullptr;
};
template <class T>
void swap(CountingAllocator<T>& lhs, CountingAllocator<T>& rhs) {
    lhs.swap(rhs);
}

} // namespace starrocks