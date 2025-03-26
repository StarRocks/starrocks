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

#include "mem_hook.h"

#include <iostream>

#include "common/compiler_util.h"
#include "common/config.h"
#include "glog/logging.h"
#include "jemalloc/jemalloc.h"
#include "runtime/current_thread.h"
#include "runtime/memory/counting_allocator.h"
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

#define SET_DELTA_MEMORY(value)              \
    do {                                     \
        starrocks::tls_delta_memory = value; \
    } while (0)
#define RESET_DELTA_MEMORY() SET_DELTA_MEMORY(0)

#ifndef BE_TEST
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
#define TRY_MEM_CONSUME(size, err_ret)                                                      \
    do {                                                                                    \
        if (LIKELY(starrocks::tls_is_thread_status_init)) {                                 \
            if (UNLIKELY(!starrocks::tls_thread_status.try_mem_consume(size))) {            \
                RESET_DELTA_MEMORY();                                                       \
                return err_ret;                                                             \
            }                                                                               \
        } else {                                                                            \
            if (UNLIKELY(!starrocks::CurrentThread::try_mem_consume_without_cache(size))) { \
                RESET_DELTA_MEMORY();                                                       \
                return err_ret;                                                             \
            }                                                                               \
        }                                                                                   \
    } while (0)
#define SET_EXCEED_MEM_TRACKER() \
    starrocks::tls_exceed_mem_tracker = starrocks::GlobalEnv::GetInstance()->process_mem_tracker()
#define IS_BAD_ALLOC_CATCHED() starrocks::tls_is_catched
#else
std::atomic<int64_t> g_mem_usage(0);
#define MEMORY_CONSUME_SIZE(size) g_mem_usage.fetch_add(size)
#define MEMORY_RELEASE_SIZE(size) g_mem_usage.fetch_sub(size)
#define MEMORY_CONSUME_PTR(ptr) g_mem_usage.fetch_add(STARROCKS_MALLOC_SIZE(ptr))
#define MEMORY_RELEASE_PTR(ptr) g_mem_usage.fetch_sub(STARROCKS_MALLOC_SIZE(ptr))
#define TRY_MEM_CONSUME(size, err_ret) g_mem_usage.fetch_add(size)
#define SET_EXCEED_MEM_TRACKER() (void)0
#define IS_BAD_ALLOC_CATCHED() false
#endif

static int64_t g_large_memory_alloc_failure_threshold = 0;

namespace starrocks {
// thread-safety is not a concern here since this is a really rare op
int64_t set_large_memory_alloc_failure_threshold(int64_t val) {
    int64_t old_val = g_large_memory_alloc_failure_threshold;
    g_large_memory_alloc_failure_threshold = val;
    return old_val;
}
} // namespace starrocks

const size_t large_memory_alloc_report_threshold = 1073741824;
inline thread_local bool skip_report = false;
inline void report_large_memory_alloc(size_t size) {
    if (size > large_memory_alloc_report_threshold && !skip_report) {
        skip_report = true; // to avoid recursive output log
        try {
            auto qid = starrocks::CurrentThread::current().query_id();
            auto fid = starrocks::CurrentThread::current().fragment_instance_id();
            LOG(WARNING) << "large memory alloc, query_id:" << print_id(qid) << " instance: " << print_id(fid)
                         << " acquire:" << size << " bytes, is_bad_alloc_caught: " << IS_BAD_ALLOC_CATCHED()
                         << ", stack:\n"
                         << starrocks::get_stack_trace();
        } catch (...) {
            // do nothing
        }
        skip_report = false;
    }
}
#define STARROCKS_REPORT_LARGE_MEM_ALLOC(size) report_large_memory_alloc(size)

inline bool block_large_memory_alloc(size_t size) {
    if (UNLIKELY(g_large_memory_alloc_failure_threshold > 0 && size > g_large_memory_alloc_failure_threshold)) {
        // DON'T try to allocate the memory at all
        if (starrocks::config::abort_on_large_memory_allocation) {
            std::abort();
        } else {
            return true;
        }
    }
    return false;
}

DEFINE_SCOPED_FAIL_POINT(mem_alloc_error);

#ifdef FIU_ENABLE
#define FAIL_POINT_INJECT_MEM_ALLOC_ERROR(retVal)                                       \
    FAIL_POINT_TRIGGER_EXECUTE(mem_alloc_error, {                                       \
        LOG(INFO) << "inject mem alloc error, stack: " << starrocks::get_stack_trace(); \
        return retVal;                                                                  \
    });
#else
#define FAIL_POINT_INJECT_MEM_ALLOC_ERROR(retVal) (void)0
#endif

extern "C" {
// malloc
void* my_malloc(size_t size) __THROW {
    STARROCKS_REPORT_LARGE_MEM_ALLOC(size);
    int64_t alloc_size = STARROCKS_NALLOX(size, 0);
    SET_DELTA_MEMORY(alloc_size);
    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(nullptr);
        // NOTE: do NOT call `tc_malloc_size` here, it may call the new operator, which in turn will
        // call the `my_malloc`, and result in a deadloop.
        TRY_MEM_CONSUME(alloc_size, nullptr);
        void* ptr = STARROCKS_MALLOC(size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(alloc_size);
            RESET_DELTA_MEMORY();
        }
        return ptr;
    } else {
        if (UNLIKELY(block_large_memory_alloc(size))) {
            return nullptr;
        }

        void* ptr = STARROCKS_MALLOC(size);
        // NOTE: do NOT call `tc_malloc_size` here, it may call the new operator, which in turn will
        // call the `my_malloc`, and result in a deadloop.
        if (LIKELY(ptr != nullptr)) {
            MEMORY_CONSUME_SIZE(alloc_size);
        } else {
            RESET_DELTA_MEMORY();
        }
        return ptr;
    }
}

// free
void my_free(void* p) __THROW {
    if (UNLIKELY(p == nullptr)) {
        RESET_DELTA_MEMORY();
        return;
    }
    int64_t malloc_size = STARROCKS_MALLOC_SIZE(p);
    MEMORY_RELEASE_SIZE(malloc_size);
    SET_DELTA_MEMORY(-malloc_size);
    STARROCKS_FREE(p);
}

// realloc
void* my_realloc(void* p, size_t size) __THROW {
    STARROCKS_REPORT_LARGE_MEM_ALLOC(size);
    // If new_size is zero, the behavior is implementation defined
    // (null pointer may be returned (in which case the old memory block may or may not be freed),
    // or some non-null pointer may be returned that may not be used to access storage)
    if (UNLIKELY(size == 0)) {
        RESET_DELTA_MEMORY();
        return nullptr;
    }
    int64_t old_size = STARROCKS_MALLOC_SIZE(p);
    int64_t new_size = STARROCKS_NALLOX(size, 0);
    SET_DELTA_MEMORY(new_size - old_size);

    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(nullptr);
        TRY_MEM_CONSUME(new_size - old_size, nullptr);
        void* ptr = STARROCKS_REALLOC(p, size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(new_size - old_size);
            RESET_DELTA_MEMORY();
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_REALLOC(p, size);
        if (ptr != nullptr) {
            MEMORY_CONSUME_SIZE(new_size - old_size);
        } else {
            // nothing to do.
            // If tc_realloc() fails the original block is left untouched; it is not freed or moved
            RESET_DELTA_MEMORY();
        }
        return ptr;
    }
}

// calloc
void* my_calloc(size_t n, size_t size) __THROW {
    STARROCKS_REPORT_LARGE_MEM_ALLOC(n * size);
    // If size is zero, the behavior is implementation defined (null pointer may be returned
    // or some non-null pointer may be returned that may not be used to access storage)
    if (UNLIKELY(size == 0)) {
        RESET_DELTA_MEMORY();
        return nullptr;
    }

    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(nullptr);
        TRY_MEM_CONSUME(n * size, nullptr);
        void* ptr = STARROCKS_CALLOC(n, size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(n * size);
            RESET_DELTA_MEMORY();
        } else {
            int64_t alloc_size = STARROCKS_MALLOC_SIZE(ptr);
            MEMORY_CONSUME_SIZE(alloc_size - n * size);
            SET_DELTA_MEMORY(alloc_size);
        }
        return ptr;
    } else {
        if (UNLIKELY(block_large_memory_alloc(n * size))) {
            return nullptr;
        }
        void* ptr = STARROCKS_CALLOC(n, size);
        int64_t alloc_size = STARROCKS_MALLOC_SIZE(ptr);
        MEMORY_CONSUME_SIZE(alloc_size);
        SET_DELTA_MEMORY(alloc_size);
        return ptr;
    }
}

void my_cfree(void* ptr) __THROW {
    if (UNLIKELY(ptr == nullptr)) {
        RESET_DELTA_MEMORY();
        return;
    }
    int64_t alloc_size = STARROCKS_MALLOC_SIZE(ptr);
    MEMORY_RELEASE_SIZE(alloc_size);
    SET_DELTA_MEMORY(-alloc_size);
    STARROCKS_CFREE(ptr);
}

// memalign
void* my_memalign(size_t align, size_t size) __THROW {
    STARROCKS_REPORT_LARGE_MEM_ALLOC(size);
    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(nullptr);
        TRY_MEM_CONSUME(size, nullptr);
        void* ptr = STARROCKS_ALIGNED_ALLOC(align, size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(size);
            RESET_DELTA_MEMORY();
        } else {
            int64_t alloc_size = STARROCKS_MALLOC_SIZE(ptr);
            MEMORY_CONSUME_SIZE(alloc_size - size);
            SET_DELTA_MEMORY(alloc_size);
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_ALIGNED_ALLOC(align, size);
        int64_t alloc_size = STARROCKS_MALLOC_SIZE(ptr);
        MEMORY_CONSUME_SIZE(alloc_size);
        SET_DELTA_MEMORY(alloc_size);
        return ptr;
    }
}

// aligned_alloc
void* my_aligned_alloc(size_t align, size_t size) __THROW {
    STARROCKS_REPORT_LARGE_MEM_ALLOC(size);
    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(nullptr);
        TRY_MEM_CONSUME(size, nullptr);
        void* ptr = STARROCKS_ALIGNED_ALLOC(align, size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(size);
            RESET_DELTA_MEMORY();
        } else {
            int64_t alloc_size = STARROCKS_MALLOC_SIZE(ptr);
            MEMORY_CONSUME_SIZE(alloc_size - size);
            SET_DELTA_MEMORY(alloc_size);
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_ALIGNED_ALLOC(align, size);
        int64_t alloc_size = STARROCKS_MALLOC_SIZE(ptr);
        MEMORY_CONSUME_SIZE(alloc_size);
        SET_DELTA_MEMORY(alloc_size);
        return ptr;
    }
}

// valloc
void* my_valloc(size_t size) __THROW {
    STARROCKS_REPORT_LARGE_MEM_ALLOC(size);
    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(nullptr);
        TRY_MEM_CONSUME(size, nullptr);
        void* ptr = STARROCKS_VALLOC(size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(size);
            RESET_DELTA_MEMORY();
        } else {
            int64_t alloc_size = STARROCKS_MALLOC_SIZE(ptr);
            MEMORY_CONSUME_SIZE(alloc_size - size);
            SET_DELTA_MEMORY(alloc_size);
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_VALLOC(size);
        int64_t alloc_size = STARROCKS_MALLOC_SIZE(ptr);
        MEMORY_CONSUME_SIZE(alloc_size);
        SET_DELTA_MEMORY(alloc_size);
        return ptr;
    }
}

// pvalloc
void* my_pvalloc(size_t size) __THROW {
    STARROCKS_REPORT_LARGE_MEM_ALLOC(size);
    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(nullptr);
        TRY_MEM_CONSUME(size, nullptr);
        void* ptr = STARROCKS_VALLOC(size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(size);
            RESET_DELTA_MEMORY();
        } else {
            int64_t alloc_size = STARROCKS_MALLOC_SIZE(ptr);
            MEMORY_CONSUME_SIZE(alloc_size - size);
            SET_DELTA_MEMORY(alloc_size);
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_VALLOC(size);
        int64_t alloc_size = STARROCKS_MALLOC_SIZE(ptr);
        MEMORY_CONSUME_SIZE(alloc_size);
        SET_DELTA_MEMORY(alloc_size);
        return ptr;
    }
}

// posix_memalign
int my_posix_memalign(void** r, size_t align, size_t size) __THROW {
    STARROCKS_REPORT_LARGE_MEM_ALLOC(size);
    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(-1);
        TRY_MEM_CONSUME(size, ENOMEM);
        int ret = STARROCKS_POSIX_MEMALIGN(r, align, size);
        if (UNLIKELY(ret != 0)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(size);
            SET_DELTA_MEMORY(0);
        } else {
            int64_t alloc_size = STARROCKS_MALLOC_SIZE(*r);
            MEMORY_CONSUME_SIZE(alloc_size - size);
            SET_DELTA_MEMORY(alloc_size);
        }
        return ret;
    } else {
        int ret = STARROCKS_POSIX_MEMALIGN(r, align, size);
        if (ret == 0) {
            int64_t alloc_size = STARROCKS_MALLOC_SIZE(*r);
            MEMORY_CONSUME_SIZE(alloc_size);
            SET_DELTA_MEMORY(alloc_size);
        } else {
            RESET_DELTA_MEMORY();
        }
        return ret;
    }
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
