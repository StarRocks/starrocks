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

#include "common/compiler_util.h"
#include "jemalloc/jemalloc.h"
#include "runtime/current_thread.h"
#include "util/failpoint/fail_point.h"
#include "util/stack_util.h"

#ifndef BE_TEST
#include "runtime/exec_env.h"
#endif

#define ALIAS(my_fn) __attribute__((alias(#my_fn), used))

#define STARROCKS_MALLOC_SIZE(ptr) je_malloc_usable_size(ptr)
#define STARROCKS_NALLOX(size, flags) je_nallocx(size, flags)
#define STARROCKS_MALLOC(size) je_malloc(size)
#define STARROCKS_FREE(ptr) je_free(ptr)
#define STARROCKS_REALLOC(ptr, size) je_realloc(ptr, size)
#define STARROCKS_CALLOC(number, size) je_calloc(number, size)
#define STARROCKS_ALIGNED_ALLOC(align, size) je_aligned_alloc(align, size)
#define STARROCKS_POSIX_MEMALIGN(ptr, align, size) je_posix_memalign(ptr, align, size)
#define STARROCKS_CFREE(ptr) je_free(ptr)
#define STARROCKS_VALLOC(size) je_valloc(size)

#define MEMORY_CONSUME_SIZE(size)                                      \
    do {                                                               \
        if (LIKELY(starrocks::tls_is_thread_status_init)) {            \
            starrocks::tls_thread_status.mem_consume(size);            \
        } else {                                                       \
            starrocks::CurrentThread::mem_consume_without_cache(size); \
        }                                                              \
    } while (0)
#define MEMORY_RELEASE_SIZE(size)                                      \
    do {                                                               \
        if (LIKELY(starrocks::tls_is_thread_status_init)) {            \
            starrocks::tls_thread_status.mem_release(size);            \
        } else {                                                       \
            starrocks::CurrentThread::mem_release_without_cache(size); \
        }                                                              \
    } while (0)
#define MEMORY_CONSUME_PTR(ptr) MEMORY_CONSUME_SIZE(STARROCKS_MALLOC_SIZE(ptr))
#define MEMORY_RELEASE_PTR(ptr) MEMORY_RELEASE_SIZE(STARROCKS_MALLOC_SIZE(ptr))

extern "C" {
// malloc
void* my_malloc(size_t size) __THROW {
    void* ptr = STARROCKS_MALLOC(size);
    // NOTE: do NOT call `tc_malloc_size` here, it may call the new operator, which in turn will
    // call the `my_malloc`, and result in a deadloop.
    if (LIKELY(ptr != nullptr)) {
        MEMORY_CONSUME_SIZE(STARROCKS_NALLOX(size, 0));
    }
    return ptr;
}

// free
void my_free(void* p) __THROW {
    if (UNLIKELY(p == nullptr)) {
        return;
    }
    MEMORY_RELEASE_PTR(p);
    STARROCKS_FREE(p);
}

// realloc
void* my_realloc(void* p, size_t size) __THROW {
    // If new_size is zero, the behavior is implementation defined
    // (null pointer may be returned (in which case the old memory block may or may not be freed),
    // or some non-null pointer may be returned that may not be used to access storage)
    if (UNLIKELY(size == 0)) {
        return nullptr;
    }
    int64_t old_size = STARROCKS_MALLOC_SIZE(p);

    void* ptr = STARROCKS_REALLOC(p, size);
    if (ptr != nullptr) {
        MEMORY_CONSUME_SIZE(STARROCKS_MALLOC_SIZE(ptr) - old_size);
    } else {
        // nothing to do.
        // If tc_realloc() fails the original block is left untouched; it is not freed or moved
    }
    return ptr;
}

// calloc
void* my_calloc(size_t n, size_t size) __THROW {
    // If size is zero, the behavior is implementation defined (null pointer may be returned
    // or some non-null pointer may be returned that may not be used to access storage)
    if (UNLIKELY(size == 0)) {
        return nullptr;
    }

    void* ptr = STARROCKS_CALLOC(n, size);
    MEMORY_CONSUME_PTR(ptr);
    return ptr;
}

void my_cfree(void* ptr) __THROW {
    if (UNLIKELY(ptr == nullptr)) {
        return;
    }
    MEMORY_RELEASE_PTR(ptr);
    STARROCKS_CFREE(ptr);
}

// memalign
void* my_memalign(size_t align, size_t size) __THROW {
    void* ptr = STARROCKS_ALIGNED_ALLOC(align, size);
    MEMORY_CONSUME_PTR(ptr);
    return ptr;
}

// aligned_alloc
void* my_aligned_alloc(size_t align, size_t size) __THROW {
    void* ptr = STARROCKS_ALIGNED_ALLOC(align, size);
    MEMORY_CONSUME_PTR(ptr);
    return ptr;
}

// valloc
void* my_valloc(size_t size) __THROW {
    void* ptr = STARROCKS_VALLOC(size);
    MEMORY_CONSUME_PTR(ptr);
    return ptr;
}

// pvalloc
void* my_pvalloc(size_t size) __THROW {
    void* ptr = STARROCKS_VALLOC(size);
    MEMORY_CONSUME_PTR(ptr);
    return ptr;
}

// posix_memalign
int my_posix_memalign(void** r, size_t align, size_t size) __THROW {
    int ret = STARROCKS_POSIX_MEMALIGN(r, align, size);
    if (ret == 0) {
        MEMORY_CONSUME_PTR(*r);
    }
    return ret;
}

size_t my_malloc_usebale_size(void* ptr) __THROW {
    size_t ret = STARROCKS_MALLOC_SIZE(ptr);
    return ret;
}

void* malloc(size_t size) __THROW ALIAS(my_malloc);
void free(void* p) __THROW ALIAS(my_free);
void* realloc(void* p, size_t size) __THROW ALIAS(my_realloc);
void* calloc(size_t n, size_t size) __THROW ALIAS(my_calloc);
void cfree(void* ptr) __THROW ALIAS(my_cfree);
void* memalign(size_t align, size_t size) __THROW ALIAS(my_memalign);
void* aligned_alloc(size_t align, size_t size) __THROW ALIAS(my_aligned_alloc);
void* valloc(size_t size) __THROW ALIAS(my_valloc);
void* pvalloc(size_t size) __THROW ALIAS(my_pvalloc);
int posix_memalign(void** r, size_t a, size_t s) __THROW ALIAS(my_posix_memalign);
size_t malloc_usable_size(void* ptr) __THROW ALIAS(my_malloc_usebale_size);

// This is the bug of glibc: https://sourceware.org/bugzilla/show_bug.cgi?id=17730,
// some version of glibc will alloc thread local storage using __libc_memalign
// If we use jemalloc, the tls memory will be allocated by __libc_memalign and
// then released by memalign(hooked by je_aligned_alloc), so it will crash.
// so we will hook the __libc_memalign to avoid this BUG.
void* __libc_memalign(size_t alignment, size_t size) {
    return memalign(alignment, size);
}
}
