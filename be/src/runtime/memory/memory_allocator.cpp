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

#include "runtime/memory/memory_allocator.h"

#include <jemalloc/jemalloc.h>

#include <cstdlib>
#include <cstring>

#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"

namespace starrocks::memory {

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

template <bool clear_memory>
void* JemallocAllocator<clear_memory>::alloc(size_t size, size_t alignment) {
    void* ret = nullptr;
    if (alignment <= MALLOC_MIN_ALIGNMENT) {
        if constexpr (clear_memory) {
            ret = je_calloc(size, 1);
        } else {
            ret = je_malloc(size);
        }
    } else {
        int res = je_posix_memalign(&ret, alignment, size);
        if (UNLIKELY(res != 0)) {
            return nullptr;
        }
        if constexpr (clear_memory) {
            memset(ret, 0, size);
        }
    }
    return ret;
}

template <bool clear_memory>
void* JemallocAllocator<clear_memory>::realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment) {
    if (old_size == new_size) {
        return ptr;
    }
    void* ret = nullptr;
    if (alignment <= MALLOC_MIN_ALIGNMENT) {
        if (je_xallocx(ptr, new_size, 0, 0) >= new_size) {
            if constexpr (clear_memory) {
                if (new_size > old_size) {
                    memset(static_cast<char*>(ptr) + old_size, 0, new_size - old_size);
                }
            }
            return ptr;
        } else {
            ret = JemallocAllocator<clear_memory>::alloc(new_size, alignment);
            if (UNLIKELY(ret == nullptr)) {
                return nullptr;
            }
            strings::memcpy_inlined(ret, ptr, old_size);
            je_free(ptr);
            return ret;
        }
    } else {
        ret = JemallocAllocator<clear_memory>::alloc(new_size, alignment);
        if (UNLIKELY(ret == nullptr)) {
            return nullptr;
        }
        strings::memcpy_inlined(ret, ptr, old_size);
        je_free(ptr);
    }

    return ret;
}

template <bool clear_memory>
void JemallocAllocator<clear_memory>::free(void* ptr, size_t size) {
    if (UNLIKELY(ptr == nullptr)) {
        return;
    }
    je_free(ptr);
}

template <bool clear_memory>
ALWAYS_INLINE int64_t JemallocAllocator<clear_memory>::nallox(size_t size, int flags) const {
    return je_nallocx(size, flags);
}

template <bool clear_memory>
void* MallocAllocator<clear_memory>::alloc(size_t size, size_t alignment) {
    void* ret = nullptr;
    if (alignment <= MALLOC_MIN_ALIGNMENT) {
        if constexpr (clear_memory) {
            ret = std::calloc(size, 1);
        } else {
            ret = std::malloc(size);
        }

    } else {
        int res = posix_memalign(&ret, alignment, size);
        if (UNLIKELY(res != 0)) {
            return nullptr;
        }
        if constexpr (clear_memory) {
            std::memset(ret, 0, size);
        }
    }
    return ret;
}

template <bool clear_memory>
void* MallocAllocator<clear_memory>::realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment) {
    if (old_size == new_size) {
        return ptr;
    }
    void* ret = nullptr;
    if (alignment <= MALLOC_MIN_ALIGNMENT) {
        ret = std::realloc(ptr, new_size);
        if (UNLIKELY(ret == nullptr)) {
            return nullptr;
        }

        if constexpr (clear_memory) {
            if (new_size > old_size) {
                std::memset(static_cast<char*>(ret) + old_size, 0, new_size - old_size);
            }
        }
    } else {
        ret = alloc(new_size, alignment);
        if (UNLIKELY(ret == nullptr)) {
            return nullptr;
        }
        strings::memcpy_inlined(ret, ptr, old_size);
        std::free(ptr);
    }
    return ret;
}

template <bool clear_memory>
void MallocAllocator<clear_memory>::free(void* ptr, size_t size) {
    if (UNLIKELY(ptr == nullptr)) {
        return;
    }
    std::free(ptr);
}

template <bool clear_memory>
ALWAYS_INLINE int64_t MallocAllocator<clear_memory>::nallox(size_t size, int flags) const {
    (void)flags;
    return static_cast<int64_t>(size);
}

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

template class JemallocAllocator<false>;
template class JemallocAllocator<true>;
template class MallocAllocator<false>;
template class MallocAllocator<true>;
template class TrackedAllocator<JemallocAllocator<false>>;
template class TrackedAllocator<JemallocAllocator<true>>;
template class TrackedAllocator<MallocAllocator<false>>;
template class TrackedAllocator<MallocAllocator<true>>;
template class CountingAllocator<JemallocAllocator<false>, IntCounter>;
template class CountingAllocator<JemallocAllocator<false>, AtomicIntCounter>;
template class CountingAllocator<TrackedAllocator<JemallocAllocator<false>>, IntCounter>;
template class CountingAllocator<TrackedAllocator<JemallocAllocator<false>>, AtomicIntCounter>;

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
TrackedAllocator<JemallocAllocator<false>> kDefaultAllocator;
#else
TrackedAllocator<MallocAllocator<false>> kDefaultAllocator;
#endif

Allocator* get_default_allocator() {
    return &kDefaultAllocator;
}

} // namespace starrocks::memory
