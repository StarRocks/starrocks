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
class CountingAllocator final : public AllocatorFactory<Base, CountingAllocator<Base>> {
public:
    void* alloc(size_t size) override {
        void* result = static_cast<Base*>(this)->alloc(size);
        _memory_usage += size;
        return result;
    }
    void free(void* ptr) override {
        static_cast<Base*>(this)->free(ptr);
        _memory_usage -= malloc_usable_size(ptr);
    }

    void* realloc(void* ptr, size_t size) override {
        int64_t old_size = malloc_usable_size(ptr);
        void* result = static_cast<Base*>(this)->realloc(ptr, size);
        if (LIKELY(result != nullptr)) {
            _memory_usage += malloc_usable_size(ptr) - old_size;
        }
        return result;
    }

    void* calloc(size_t n, size_t size) override {
        void* result = static_cast<Base*>(this)->calloc(n, size);
        _memory_usage += n * size;
        return result;
    }

    void cfree(void* ptr) override {
        static_cast<Base*>(this)->cfree(ptr);
        _memory_usage -= malloc_usable_size(ptr);
    }

    void* memalign(size_t align, size_t size) override {
        void* result = static_cast<Base*>(this)->memalign(align, size);
        _memory_usage += malloc_usable_size(result);
        return result;
    }
    void* aligned_alloc(size_t align, size_t size) override {
        void* result = static_cast<Base*>(this)->aligned_alloc(align, size);
        _memory_usage += malloc_usable_size(result);
        return result;
    }

    void* valloc(size_t size) override {
        void* result = static_cast<Base*>(this)->valloc(size);
        _memory_usage += malloc_usable_size(result);
        return result;
    }

    void* pvalloc(size_t size) override {
        void* result = static_cast<Base*>(this)->pvalloc(size);
        _memory_usage += malloc_usable_size(result);
        return result;
    }

    int posix_memalign(void** ptr, size_t align, size_t size) override {
        int result = static_cast<Base*>(this)->posix_memalign(ptr, align, size);
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

using CountingAllocatorWithHook = CountingAllocator<MemHookAllocator>;

inline thread_local Allocator* tls_counting_allocator = nullptr;

// based on tls counting allocator
template <class T>
class STLCountingAllocator {
public:
    typedef T value_type;
    typedef size_t size_type;
    using propagate_on_container_copy_assignment = std::true_type; // for consistency
    using propagate_on_container_move_assignment = std::true_type; // to avoid the pessimization
    using propagate_on_container_swap = std::true_type;            // to avoid the undefined behavior

    template <typename U>
    struct rebind {
        using other = STLCountingAllocator<U>;
    };
    STLCountingAllocator() = default;
    explicit STLCountingAllocator(int64_t* counter) : _counter(counter) {}
    explicit STLCountingAllocator(const STLCountingAllocator& rhs) : _counter(rhs._counter) {}
    template <class U>
    STLCountingAllocator(const STLCountingAllocator<U>& other) : _counter(other._counter) {}

    ~STLCountingAllocator() = default;

    STLCountingAllocator(STLCountingAllocator&& rhs) noexcept { std::swap(_counter, rhs._counter); }

    STLCountingAllocator& operator=(STLCountingAllocator&& rhs) noexcept {
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

    STLCountingAllocator& operator=(const STLCountingAllocator& rhs) {
        _counter = rhs._counter;
        return *this;
    }

    template <class U>
    STLCountingAllocator& operator=(const STLCountingAllocator<U>& rhs) {
        _counter = rhs._counter;
        return *this;
    }

    bool operator==(const STLCountingAllocator& rhs) const { return _counter == rhs._counter; }

    bool operator!=(const STLCountingAllocator& rhs) const { return !(*this == rhs); }

    // CountingAllocator doesn't support swap
    void swap(STLCountingAllocator& rhs) {}

    int64_t* _counter = nullptr;
};
template <class T>
void swap(STLCountingAllocator<T>& lhs, STLCountingAllocator<T>& rhs) {
    lhs.swap(rhs);
}

} // namespace starrocks