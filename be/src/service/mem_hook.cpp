// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <gperftools/nallocx.h>
#include <gperftools/tcmalloc.h>

#include <atomic>
#include <iostream>
#include <new>

#include "common/compiler_util.h"

#ifndef BE_TEST
#include "runtime/current_thread.h"
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

#ifndef BE_TEST
#define TC_MALLOC_SIZE(ptr) tc_malloc_size(ptr)
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
#define MEMORY_CONSUME_PTR(ptr) MEMORY_CONSUME_SIZE(TC_MALLOC_SIZE(ptr))
#define MEMORY_RELEASE_PTR(ptr) MEMORY_RELEASE_SIZE(TC_MALLOC_SIZE(ptr))
#define TRY_MEM_CONSUME(size, err_ret)                                                                   \
    do {                                                                                                 \
        if (LIKELY(starrocks::tls_is_thread_status_init)) {                                              \
            RETURN_IF_UNLIKELY(!starrocks::tls_thread_status.try_mem_consume(size), err_ret);            \
        } else {                                                                                         \
            RETURN_IF_UNLIKELY(!starrocks::CurrentThread::try_mem_consume_without_cache(size), err_ret); \
        }                                                                                                \
    } while (0)
#define SET_EXCEED_MEM_TRACKER() \
    starrocks::tls_exceed_mem_tracker = starrocks::ExecEnv::GetInstance()->process_mem_tracker()
#define IS_BAD_ALLOC_CATCHED() starrocks::tls_thread_status.is_catched()
#else
std::atomic<int64_t> g_mem_usage(0);
#define TC_MALLOC_SIZE(ptr) tc_malloc_size(ptr)
#define MEMORY_CONSUME_SIZE(size) g_mem_usage.fetch_add(size)
#define MEMORY_RELEASE_SIZE(size) g_mem_usage.fetch_sub(size)
#define MEMORY_CONSUME_PTR(ptr) g_mem_usage.fetch_add(tc_malloc_size(ptr))
#define MEMORY_RELEASE_PTR(ptr) g_mem_usage.fetch_sub(tc_malloc_size(ptr))
#define TRY_MEM_CONSUME(size, err_ret) g_mem_usage.fetch_add(size)
#define SET_EXCEED_MEM_TRACKER() (void)0
#define IS_BAD_ALLOC_CATCHED() false
#endif

extern "C" {
// malloc
void* my_malloc(size_t size) __THROW {
    if (IS_BAD_ALLOC_CATCHED()) {
        // NOTE: do NOT call `tc_malloc_size` here, it may call the new operator, which in turn will
        // call the `my_malloc`, and result in a deadloop.
        TRY_MEM_CONSUME(tc_nallocx(size, 0), nullptr);
        void* ptr = tc_malloc(size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(tc_nallocx(size, 0));
        }
        return ptr;
    } else {
        void* ptr = tc_malloc(size);
        // NOTE: do NOT call `tc_malloc_size` here, it may call the new operator, which in turn will
        // call the `my_malloc`, and result in a deadloop.
        if (LIKELY(ptr != nullptr)) {
            MEMORY_CONSUME_SIZE(tc_nallocx(size, 0));
        }
        return ptr;
    }
}

// free
void my_free(void* p) __THROW {
    MEMORY_RELEASE_PTR(p);
    tc_free(p);
}

// realloc
void* my_realloc(void* p, size_t size) __THROW {
    // If new_size is zero, the behavior is implementation defined
    // (null pointer may be returned (in which case the old memory block may or may not be freed),
    // or some non-null pointer may be returned that may not be used to access storage)
    if (UNLIKELY(size == 0)) {
        return nullptr;
    }
    int64_t old_size = TC_MALLOC_SIZE(p);

    if (IS_BAD_ALLOC_CATCHED()) {
        TRY_MEM_CONSUME(tc_nallocx(size, 0) - old_size, nullptr);
        void* ptr = tc_realloc(p, size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(tc_nallocx(size, 0) - old_size);
        }
        return ptr;
    } else {
        void* ptr = tc_realloc(p, size);
        if (ptr != nullptr) {
            MEMORY_CONSUME_SIZE(TC_MALLOC_SIZE(ptr) - old_size);
        } else {
            // nothing to do.
            // If tc_realloc() fails the original block is left untouched; it is not freed or moved
        }
        return ptr;
    }
}

// calloc
void* my_calloc(size_t n, size_t size) __THROW {
    // If size is zero, the behavior is implementation defined (null pointer may be returned
    // or some non-null pointer may be returned that may not be used to access storage)
    if (UNLIKELY(size == 0)) {
        return nullptr;
    }

    if (IS_BAD_ALLOC_CATCHED()) {
        TRY_MEM_CONSUME(n * size, nullptr);
        void* ptr = tc_calloc(n, size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(n * size);
        } else {
            MEMORY_CONSUME_SIZE(TC_MALLOC_SIZE(ptr) - n * size);
        }
        return ptr;
    } else {
        void* ptr = tc_calloc(n, size);
        MEMORY_CONSUME_PTR(ptr);
        return ptr;
    }
}

void my_cfree(void* ptr) __THROW {
    MEMORY_RELEASE_PTR(ptr);
    tc_cfree(ptr);
}

// memalign
void* my_memalign(size_t align, size_t size) __THROW {
    if (IS_BAD_ALLOC_CATCHED()) {
        TRY_MEM_CONSUME(size, nullptr);
        void* ptr = tc_memalign(align, size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(size);
        } else {
            MEMORY_CONSUME_SIZE(TC_MALLOC_SIZE(ptr) - size);
        }
        return ptr;
    } else {
        void* ptr = tc_memalign(align, size);
        MEMORY_CONSUME_PTR(ptr);
        return ptr;
    }
}

// aligned_alloc
void* my_aligned_alloc(size_t align, size_t size) __THROW {
    if (IS_BAD_ALLOC_CATCHED()) {
        TRY_MEM_CONSUME(size, nullptr);
        void* ptr = tc_memalign(align, size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(size);
        } else {
            MEMORY_CONSUME_SIZE(TC_MALLOC_SIZE(ptr) - size);
        }
        return ptr;
    } else {
        void* ptr = tc_memalign(align, size);
        MEMORY_CONSUME_PTR(ptr);
        return ptr;
    }
}

// valloc
void* my_valloc(size_t size) __THROW {
    if (IS_BAD_ALLOC_CATCHED()) {
        TRY_MEM_CONSUME(size, nullptr);
        void* ptr = tc_valloc(size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(size);
        } else {
            MEMORY_CONSUME_SIZE(TC_MALLOC_SIZE(ptr) - size);
        }
        return ptr;
    } else {
        void* ptr = tc_valloc(size);
        MEMORY_CONSUME_PTR(ptr);
        return ptr;
    }
}

// pvalloc
void* my_pvalloc(size_t size) __THROW {
    if (IS_BAD_ALLOC_CATCHED()) {
        TRY_MEM_CONSUME(size, nullptr);
        void* ptr = tc_valloc(size);
        if (UNLIKELY(ptr == nullptr)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(size);
        } else {
            MEMORY_CONSUME_SIZE(TC_MALLOC_SIZE(ptr) - size);
        }
        return ptr;
    } else {
        void* ptr = tc_pvalloc(size);
        MEMORY_CONSUME_PTR(ptr);
        return ptr;
    }
}

// posix_memalign
int my_posix_memalign(void** r, size_t align, size_t size) __THROW {
    if (IS_BAD_ALLOC_CATCHED()) {
        TRY_MEM_CONSUME(size, ENOMEM);
        int ret = tc_posix_memalign(r, align, size);
        if (UNLIKELY(ret != 0)) {
            SET_EXCEED_MEM_TRACKER();
            MEMORY_RELEASE_SIZE(size);
        } else {
            MEMORY_CONSUME_SIZE(TC_MALLOC_SIZE(*r) - size);
        }
        return ret;
    } else {
        int ret = tc_posix_memalign(r, align, size);
        if (ret == 0) {
            MEMORY_CONSUME_PTR(*r);
        }
        return ret;
    }
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
}
