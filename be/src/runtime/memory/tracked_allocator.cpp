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

#include "runtime/memory/tracked_allocator.h"

#include <new>

#include "base/compiler_util.h"
#include "base/memory/counting_allocator.h"
#include "base/memory/jemalloc_allocator.h"
#include "base/memory/malloc_allocator.h"
#include "runtime/current_thread.h"

namespace starrocks::memory {

namespace {

void try_consume_memory(int64_t size) {
    if (LIKELY(starrocks::tls_is_thread_status_init)) {
        if (UNLIKELY(!starrocks::tls_thread_status.try_mem_consume(size))) {
            throw std::bad_alloc();
        }
    } else {
        if (UNLIKELY(!starrocks::CurrentThread::try_mem_consume_without_cache(size))) {
            throw std::bad_alloc();
        }
    }
}

void release_memory(int64_t size) {
    if (LIKELY(starrocks::tls_is_thread_status_init)) {
        starrocks::tls_thread_status.mem_release(size);
    } else {
        starrocks::CurrentThread::mem_release_without_cache(size);
    }
}

} // namespace

template <class BaseAllocator>
void* TrackedAllocator<BaseAllocator>::alloc(size_t size, size_t alignment) {
    int64_t alloc_size = BaseAllocator::nallox(size, 0);

    try_consume_memory(alloc_size);

    void* ptr = nullptr;
    if constexpr (BaseAllocator::throw_bad_alloc_on_failure()) {
        try {
            ptr = BaseAllocator::alloc(size, alignment);
        } catch (const std::bad_alloc& e) {
            release_memory(alloc_size);
            throw;
        }
    } else {
        ptr = BaseAllocator::alloc(size, alignment);
        if (UNLIKELY(ptr == nullptr)) {
            release_memory(alloc_size);
            throw std::bad_alloc();
        }
    }

    return ptr;
}

template <class BaseAllocator>
void* TrackedAllocator<BaseAllocator>::realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment) {
    int64_t old_alloc_size = BaseAllocator::nallox(old_size, 0);
    int64_t new_alloc_size = BaseAllocator::nallox(new_size, 0);
    int64_t delta = new_alloc_size - old_alloc_size;
    if (delta > 0) {
        try_consume_memory(delta);
    }

    void* ret = nullptr;
    if constexpr (BaseAllocator::throw_bad_alloc_on_failure()) {
        try {
            ret = BaseAllocator::realloc(ptr, old_size, new_size, alignment);
        } catch (const std::bad_alloc& e) {
            if (delta > 0) {
                release_memory(delta);
            }
            throw;
        }
    } else {
        ret = BaseAllocator::realloc(ptr, old_size, new_size, alignment);
        if (UNLIKELY(ret == nullptr)) {
            if (delta > 0) {
                release_memory(delta);
            }
            throw std::bad_alloc();
        }
    }

    if (delta < 0) {
        release_memory(-delta);
    }
    return ret;
}

template <class BaseAllocator>
void TrackedAllocator<BaseAllocator>::free(void* ptr, size_t size) {
    int64_t alloc_size = BaseAllocator::nallox(size, 0);
    BaseAllocator::free(ptr, size);
    release_memory(alloc_size);
}

template <class BaseAllocator>
int64_t TrackedAllocator<BaseAllocator>::nallox(size_t size, int flags) const {
    return BaseAllocator::nallox(size, flags);
}

template class TrackedAllocator<JemallocAllocator<false>>;
template class TrackedAllocator<JemallocAllocator<true>>;
template class TrackedAllocator<MallocAllocator<false>>;
template class TrackedAllocator<MallocAllocator<true>>;
template class CountingAllocator<TrackedAllocator<JemallocAllocator<false>>, IntCounter>;
template class CountingAllocator<TrackedAllocator<JemallocAllocator<false>>, AtomicIntCounter>;

} // namespace starrocks::memory
