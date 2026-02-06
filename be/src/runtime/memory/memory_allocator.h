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
#include <cstdlib>

#include "base/compiler_util.h"

namespace starrocks::memory {

static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;

class Allocator {
public:
    enum class MemoryKind {
        kJemalloc,
        kMalloc,
    };
    virtual ~Allocator() = default;
    virtual void* alloc(size_t size, size_t alignment = 0) = 0;
    virtual void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0) = 0;
    virtual void free(void* ptr, size_t size) = 0;
    virtual int64_t nallox(size_t size, int flags = 0) const = 0;

    virtual bool transfer_to(Allocator* target, void* ptr, size_t size) { return false; }
    virtual MemoryKind memory_kind() const = 0;
};

template <bool clear_memory>
class JemallocAllocator : public Allocator {
public:
    JemallocAllocator() = default;
    ~JemallocAllocator() override = default;
    void* alloc(size_t size, size_t alignment = 0) override;
    void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0) override;
    void free(void* ptr, size_t size) override;
    int64_t nallox(size_t size, int flags = 0) const override;
    Allocator::MemoryKind memory_kind() const override { return Allocator::MemoryKind::kJemalloc; }

    static constexpr bool throw_bad_alloc_on_failure() { return false; }
};

template <bool clear_memory>
class MallocAllocator : public Allocator {
public:
    MallocAllocator() = default;
    ~MallocAllocator() override = default;
    void* alloc(size_t size, size_t alignment = 0) override;
    void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0) override;
    void free(void* ptr, size_t size) override;
    int64_t nallox(size_t size, int flags = 0) const override;
    Allocator::MemoryKind memory_kind() const override { return Allocator::MemoryKind::kMalloc; }

    static constexpr bool throw_bad_alloc_on_failure() { return false; }
};

template <class BaseAllocator>
class TrackedAllocator : public BaseAllocator {
public:
    TrackedAllocator() = default;
    ~TrackedAllocator() override = default;
    void* alloc(size_t size, size_t alignment = 0) override;
    void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0) override;
    void free(void* ptr, size_t size) override;
    int64_t nallox(size_t size, int flags = 0) const override;
    Allocator::MemoryKind memory_kind() const override { return BaseAllocator::memory_kind(); }
    static constexpr bool throw_bad_alloc_on_failure() { return true; }
};

template <class Alloc>
class AllocHolder : private Alloc {
public:
    AllocHolder() = default;
    AllocHolder(Alloc* alloc) : Alloc(*alloc) {}
    ~AllocHolder() override = default;
    Alloc* get_allocator() { return this; }
    const Alloc* get_allocator() const { return this; }
};

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

template <class BaseAllocator>
using ThreadSafeCountingAllocator = CountingAllocator<BaseAllocator, AtomicIntCounter>;
template <class BaseAllocator>
using NonThreadSafeCountingAllocator = CountingAllocator<BaseAllocator, IntCounter>;

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
extern TrackedAllocator<JemallocAllocator<false>> kDefaultAllocator;
#else
extern TrackedAllocator<MallocAllocator<false>> kDefaultAllocator;
#endif

Allocator* get_default_allocator();

} // namespace starrocks::memory
