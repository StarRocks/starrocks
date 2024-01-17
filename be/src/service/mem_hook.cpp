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

#include <atomic>
#include <iostream>

#include "common/compiler_util.h"
#include "common/config.h"
#include "glog/logging.h"
#include "jemalloc/jemalloc.h"
#include "runtime/current_thread.h"
#include "util/failpoint/fail_point.h"
#include "util/stack_util.h"

#ifndef BE_TEST
#include "runtime/exec_env.h"
#endif

#define ALIAS(my_fn) __attribute__((alias(#my_fn), used))

/*
//// void* ptr = new AAA();
void* operator new(size_t size) {
    void* ptr = tc_new(size);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}

// (<=C++11) delete ptr
void operator delete(void* p) noexcept {
    size_t actual_size = tc_malloc_size(p);
    starrocks::tls_thread_status.mem_release(actual_size);

    tc_delete(p);
}

// AAA* ptr = new AAA[5];
void* operator new[](size_t size) {
    void* ptr = tc_newarray(size);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}

// delete[] ptr;
void operator delete[](void* p) noexcept {
    size_t actual_size = tc_malloc_size(p);
    starrocks::tls_thread_status.mem_release(actual_size);

    tc_deletearray(p);
}

// void* c1 = new (std::nothrow) AAA();
void* operator new(size_t size, const std::nothrow_t& nt) noexcept {
    void* ptr = tc_new_nothrow(size, nt);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}

// AAA* c1 = new (std::nothrow) AAA[5];
void* operator new[](size_t size, const std::nothrow_t& nt) noexcept {
    void* ptr = tc_newarray_nothrow(size, nt);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}

void operator delete(void* p, const std::nothrow_t& nt) noexcept {
    size_t actual_size = tc_malloc_size(p);
    starrocks::tls_thread_status.mem_release(actual_size);

    tc_delete(p);
}

void operator delete[](void* p, const std::nothrow_t& nt) noexcept {
    size_t actual_size = tc_malloc_size(p);
    starrocks::tls_thread_status.mem_release(actual_size);

    tc_deletearray(p);
}

// (>C++11) delete ptr
void operator delete(void* p, size_t size) noexcept {
    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_release(actual_size);

    tc_delete(p);
}

void operator delete[](void* p, size_t size) noexcept {
    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_release(actual_size);

    tc_deletearray(p);
}

void* operator new(size_t size, std::align_val_t al) {
    void* ptr = tc_new_aligned(size, al);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}

void operator delete(void* p, std::align_val_t al) noexcept {
    size_t actual_size = tc_malloc_size(p);
    starrocks::tls_thread_status.mem_release(actual_size);

    return tc_delete_aligned(p, al);
}
void* operator new[](size_t size, std::align_val_t al) {
    void* ptr = tc_newarray_aligned(size, al);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}
void operator delete[](void* p, std::align_val_t al) noexcept {
    size_t actual_size = tc_malloc_size(p);
    starrocks::tls_thread_status.mem_release(actual_size);

    return tc_deletearray_aligned(p, al);
}
void* operator new(size_t size, std::align_val_t al, const std::nothrow_t& nt) noexcept {
    void* ptr = tc_new_aligned_nothrow(size, al, nt);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}
void* operator new[](size_t size, std::align_val_t al, const std::nothrow_t& nt) noexcept {
    void* ptr = tc_newarray_aligned_nothrow(size, al, nt);
    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}
void operator delete(void* p, std::align_val_t al, const std::nothrow_t& nt) noexcept {
    size_t actual_size = tc_malloc_size(p);
    starrocks::tls_thread_status.mem_release(actual_size);

    return tc_delete_aligned_nothrow(p, al, nt);
}
void operator delete[](void* p, std::align_val_t al, const std::nothrow_t& nt) noexcept {
    size_t actual_size = tc_malloc_size(p);
    starrocks::tls_thread_status.mem_release(actual_size);

    return tc_deletearray_aligned_nothrow(p, al, nt);
}

void operator delete(void* p, size_t size, std::align_val_t al) noexcept {
    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_release(actual_size);

    return tc_delete(p);
}
void operator delete[](void* p, size_t size, std::align_val_t al) noexcept {
    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_release(actual_size);

    return tc_deletearray(p);
}
*/

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
#define TRY_MEM_CONSUME(size, err_ret)                                                                   \
    do {                                                                                                 \
        if (LIKELY(starrocks::tls_is_thread_status_init)) {                                              \
            RETURN_IF_UNLIKELY(!starrocks::tls_thread_status.try_mem_consume(size), err_ret);            \
        } else {                                                                                         \
            RETURN_IF_UNLIKELY(!starrocks::CurrentThread::try_mem_consume_without_cache(size), err_ret); \
        }                                                                                                \
    } while (0)
#define SET_EXCEED_MEM_TRACKER() \
    starrocks::tls_exceed_mem_tracker = starrocks::GlobalEnv::GetInstance()->process_mem_tracker()
#define IS_BAD_ALLOC_CATCHED() starrocks::tls_thread_status.is_catched()
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

const size_t large_memory_alloc_report_threshold = 1073741824;
inline thread_local bool skip_report = false;
inline void report_large_memory_alloc(size_t size) {
    if (size > large_memory_alloc_report_threshold && !skip_report) {
        skip_report = true; // to avoid recursive output log
        try {
            auto qid = starrocks::CurrentThread::current().query_id();
            auto fid = starrocks::CurrentThread::current().fragment_instance_id();
            LOG(WARNING) << "large memory alloc, query_id:" << print_id(qid) << " instance: " << print_id(fid)
                         << " acquire:" << size << " bytes, stack:\n"
                         << starrocks::get_stack_trace();
        } catch (...) {
            // do nothing
        }
        skip_report = false;
    }
}
#define STARROCKS_REPORT_LARGE_MEM_ALLOC(size) report_large_memory_alloc(size)

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
    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(nullptr);
        // NOTE: do NOT call `tc_malloc_size` here, it may call the new operator, which in turn will
        // call the `my_malloc`, and result in a deadloop.
        TRY_MEM_CONSUME(STARROCKS_NALLOX(size, 0), nullptr);
        void* ptr = STARROCKS_MALLOC(size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(STARROCKS_NALLOX(size, 0));
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_MALLOC(size);
        // NOTE: do NOT call `tc_malloc_size` here, it may call the new operator, which in turn will
        // call the `my_malloc`, and result in a deadloop.
        if (LIKELY(ptr != nullptr)) {
            MEMORY_CONSUME_SIZE(STARROCKS_NALLOX(size, 0));
        }
        return ptr;
    }
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
    STARROCKS_REPORT_LARGE_MEM_ALLOC(size);
    // If new_size is zero, the behavior is implementation defined
    // (null pointer may be returned (in which case the old memory block may or may not be freed),
    // or some non-null pointer may be returned that may not be used to access storage)
    if (UNLIKELY(size == 0)) {
        return nullptr;
    }
    int64_t old_size = STARROCKS_MALLOC_SIZE(p);

    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(nullptr);
        TRY_MEM_CONSUME(STARROCKS_NALLOX(size, 0) - old_size, nullptr);
        void* ptr = STARROCKS_REALLOC(p, size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(STARROCKS_NALLOX(size, 0) - old_size);
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_REALLOC(p, size);
        if (ptr != nullptr) {
            MEMORY_CONSUME_SIZE(STARROCKS_MALLOC_SIZE(ptr) - old_size);
        } else {
            // nothing to do.
            // If tc_realloc() fails the original block is left untouched; it is not freed or moved
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
        return nullptr;
    }

    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(nullptr);
        TRY_MEM_CONSUME(n * size, nullptr);
        void* ptr = STARROCKS_CALLOC(n, size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(n * size);
        } else {
            MEMORY_CONSUME_SIZE(STARROCKS_MALLOC_SIZE(ptr) - n * size);
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_CALLOC(n, size);
        MEMORY_CONSUME_PTR(ptr);
        return ptr;
    }
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
    STARROCKS_REPORT_LARGE_MEM_ALLOC(size);
    if (IS_BAD_ALLOC_CATCHED()) {
        FAIL_POINT_INJECT_MEM_ALLOC_ERROR(nullptr);
        TRY_MEM_CONSUME(size, nullptr);
        void* ptr = STARROCKS_ALIGNED_ALLOC(align, size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(size);
        } else {
            MEMORY_CONSUME_SIZE(STARROCKS_MALLOC_SIZE(ptr) - size);
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_ALIGNED_ALLOC(align, size);
        MEMORY_CONSUME_PTR(ptr);
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
        } else {
            MEMORY_CONSUME_SIZE(STARROCKS_MALLOC_SIZE(ptr) - size);
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_ALIGNED_ALLOC(align, size);
        MEMORY_CONSUME_PTR(ptr);
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
        } else {
            MEMORY_CONSUME_SIZE(STARROCKS_MALLOC_SIZE(ptr) - size);
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_VALLOC(size);
        MEMORY_CONSUME_PTR(ptr);
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
        } else {
            MEMORY_CONSUME_SIZE(STARROCKS_MALLOC_SIZE(ptr) - size);
        }
        return ptr;
    } else {
        void* ptr = STARROCKS_VALLOC(size);
        MEMORY_CONSUME_PTR(ptr);
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
        } else {
            MEMORY_CONSUME_SIZE(STARROCKS_MALLOC_SIZE(*r) - size);
        }
        return ret;
    } else {
        int ret = STARROCKS_POSIX_MEMALIGN(r, align, size);
        if (ret == 0) {
            MEMORY_CONSUME_PTR(*r);
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

void * __libc_memalign(size_t alignment, size_t size) {
    return memalign(alignment, size);
}
}
