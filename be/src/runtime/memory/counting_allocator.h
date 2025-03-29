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

#include <iostream>
#include <memory>

#include "common/compiler_util.h"
#include "exprs/expr_context.h"
#include "runtime/memory/allocator.h"
#include "runtime/memory/mem_hook_allocator.h"

namespace starrocks {

// used to record the memory changes after each operation, for example,
// if malloc allocates 4 bytes  its value will be 4, and if free releases 4 bytes, its value will be -4
inline thread_local int64_t tls_delta_memory = 0;

template <class Base>
class CountingAllocator final : public AllocatorFactory<Base, CountingAllocator<Base>> {
public:
    void* alloc(size_t size) override {
        void* result = Base::alloc(size);
        _memory_usage += tls_delta_memory;
        return result;
    }
    void free(void* ptr) override {
        Base::free(ptr);
        _memory_usage += tls_delta_memory;
    }

    void* realloc(void* ptr, size_t size) override {
        void* result = Base::realloc(ptr, size);
        _memory_usage += tls_delta_memory;
        return result;
    }

    void* calloc(size_t n, size_t size) override {
        void* result = Base::calloc(n, size);
        _memory_usage += tls_delta_memory;
        return result;
    }

    void cfree(void* ptr) override {
        Base::cfree(ptr);
        _memory_usage += tls_delta_memory;
    }

    void* memalign(size_t align, size_t size) override {
        void* result = Base::memalign(align, size);
        _memory_usage += tls_delta_memory;
        return result;
    }
    void* aligned_alloc(size_t align, size_t size) override {
        void* result = Base::aligned_alloc(align, size);
        _memory_usage += tls_delta_memory;
        return result;
    }

    void* valloc(size_t size) override {
        void* result = Base::valloc(size);
        _memory_usage += tls_delta_memory;
        return result;
    }

    void* pvalloc(size_t size) override {
        void* result = Base::pvalloc(size);
        _memory_usage += tls_delta_memory;
        return result;
    }

    int posix_memalign(void** ptr, size_t align, size_t size) override {
        int result = Base::posix_memalign(ptr, align, size);
        _memory_usage += tls_delta_memory;
        return result;
    }

    int64_t memory_usage() const { return _memory_usage; }

    int64_t _memory_usage = 0;
};

using CountingAllocatorWithHook = CountingAllocator<MemHookAllocator>;

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
        // in ut mode, mem_hook won't take effect and tls_delta_memory will always be 0,
        // we use logical size for counting,
        // otherwise, we use the actual size allocated by memory allocator.
#ifndef BE_TEST
        *_counter += tls_delta_memory;
#else
        *_counter += (result != nullptr) ? n * sizeof(T) : 0;
#endif
        if (UNLIKELY(result == nullptr)) {
            throw std::bad_alloc();
        }

        return result;
    }

    void deallocate(T* ptr, size_t n) {
        DCHECK(_counter != nullptr);
        free(ptr);
#ifndef BE_TEST
        *_counter += tls_delta_memory;
#else
        *_counter -= n * sizeof(T);
#endif
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