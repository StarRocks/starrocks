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

#include <atomic>
#include <concepts>
#include <new>

#include "base/compiler_util.h"
#include "base/memory/memory_allocator.h"

namespace starrocks::memory {

template <typename C>
concept Counter = requires(C counter, int64_t delta) {
    { counter.add(delta) }
    ->std::same_as<void>;
    { counter.value() }
    ->std::convertible_to<int64_t>;
};

struct IntCounter {
    int64_t v{0};
    ALWAYS_INLINE void add(int64_t delta) { v += delta; }
    ALWAYS_INLINE int64_t value() const { return v; }
};

struct AtomicIntCounter {
    std::atomic<int64_t> v{0};
    ALWAYS_INLINE void add(int64_t delta) { v.fetch_add(delta, std::memory_order_relaxed); }
    ALWAYS_INLINE int64_t value() const { return v.load(std::memory_order_relaxed); }
};

template <class BaseAllocator, class Counter>
class CountingAllocator : public BaseAllocator {
public:
    CountingAllocator() = default;
    ~CountingAllocator() override = default;

    void* alloc(size_t size, size_t alignment = 0) override;
    void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0) override;
    void free(void* ptr, size_t size) override;
    int64_t nallox(size_t size, int flags = 0) const override;

    Allocator::MemoryKind memory_kind() const override { return BaseAllocator::memory_kind(); }

    static constexpr bool throw_bad_alloc_on_failure() { return BaseAllocator::throw_bad_alloc_on_failure(); }

    int64_t memory_usage() const { return _counter.value(); }

private:
    Counter _counter;
};

// Implementation (inline for template instantiation)
template <class BaseAllocator, class Counter>
void* CountingAllocator<BaseAllocator, Counter>::alloc(size_t size, size_t alignment) {
    void* ptr = nullptr;
    if constexpr (BaseAllocator::throw_bad_alloc_on_failure()) {
        try {
            ptr = BaseAllocator::alloc(size, alignment);
        } catch (const std::bad_alloc& e) {
            throw;
        }
    } else {
        ptr = BaseAllocator::alloc(size, alignment);
        if (UNLIKELY(ptr == nullptr)) {
            return nullptr;
        }
    }

    _counter.add(size);
    return ptr;
}

template <class BaseAllocator, class Counter>
void* CountingAllocator<BaseAllocator, Counter>::realloc(void* ptr, size_t old_size, size_t new_size,
                                                         size_t alignment) {
    void* ret = nullptr;
    if constexpr (BaseAllocator::throw_bad_alloc_on_failure()) {
        try {
            ret = BaseAllocator::realloc(ptr, old_size, new_size, alignment);
        } catch (const std::bad_alloc& e) {
            throw;
        }
    } else {
        ret = BaseAllocator::realloc(ptr, old_size, new_size, alignment);
        if (UNLIKELY(ret == nullptr)) {
            return nullptr;
        }
    }

    _counter.add(static_cast<int64_t>(new_size) - static_cast<int64_t>(old_size));
    return ret;
}

template <class BaseAllocator, class Counter>
void CountingAllocator<BaseAllocator, Counter>::free(void* ptr, size_t size) {
    BaseAllocator::free(ptr, size);
    _counter.add(-static_cast<int64_t>(size));
}

template <class BaseAllocator, class Counter>
int64_t CountingAllocator<BaseAllocator, Counter>::nallox(size_t size, int flags) const {
    return BaseAllocator::nallox(size, flags);
}

template <class BaseAllocator>
using ThreadSafeCountingAllocator = CountingAllocator<BaseAllocator, AtomicIntCounter>;

template <class BaseAllocator>
using NonThreadSafeCountingAllocator = CountingAllocator<BaseAllocator, IntCounter>;

} // namespace starrocks::memory
