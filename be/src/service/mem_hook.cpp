// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include <gperftools/nallocx.h>
#include <gperftools/tcmalloc.h>

#include <iostream>
#include <new>

#include "runtime/current_thread.h"

#define ALIAS(my_fn) __attribute__((alias(#my_fn), used))

// void* ptr = new AAA();
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

    tc_delete_nothrow(p, nt);
}

void operator delete[](void* p, const std::nothrow_t& nt) noexcept {
    size_t actual_size = tc_malloc_size(p);
    starrocks::tls_thread_status.mem_release(actual_size);

    tc_deletearray_nothrow(p, nt);
}

// (>C++11) delete ptr
void operator delete(void* p, size_t size) noexcept {
    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_release(actual_size);

    tc_delete_sized(p, size);
}

void operator delete[](void* p, size_t size) noexcept {
    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_release(actual_size);

    tc_deletearray_sized(p, size);
}

extern "C" {
// malloc
void* my_alloc(size_t size) __THROW {
    void* ptr = tc_malloc(size);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}

// free
void my_free(void* p) __THROW {
    size_t size = tc_malloc_size(p);
    starrocks::tls_thread_status.mem_release(size);

    tc_free(p);
}

// realloc
void* my_realloc(void* p, size_t size) __THROW {
    int64_t old_size = 0;
    if (p != 0) {
        old_size = tc_malloc_size(p);
    }
    int64_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size - old_size);

    return tc_realloc(p, size);
}

// calloc
void* my_calloc(size_t n, size_t size) __THROW {
    void* ptr = tc_calloc(n, size);

    size_t actual_size = tc_nallocx(n * size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}

// memalign
void* my_memalign(size_t align, size_t size) __THROW {
    void* ptr = tc_memalign(align, size);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}

// aligned_alloc
void* my_aligned_alloc(size_t align, size_t size) __THROW {
    void* ptr = tc_memalign(align, size);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}

// valloc
void* my_valloc(size_t size) __THROW {
    void* ptr = tc_valloc(size);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}

// pvalloc
void* my_pvalloc(size_t size) __THROW {
    void* ptr = tc_pvalloc(size);

    size_t actual_size = tc_nallocx(size, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ptr;
}

// posix_memalign
int my_posix_memalign(void** r, size_t a, size_t s) __THROW {
    int ret = tc_posix_memalign(r, a, s);

    size_t actual_size = tc_nallocx(s, 0);
    starrocks::tls_thread_status.mem_consume(actual_size);

    return ret;
}

void* malloc(size_t size) __THROW ALIAS(my_alloc);
void free(void* p) __THROW ALIAS(my_free);
void* realloc(void* p, size_t size) __THROW ALIAS(my_realloc);
void* calloc(size_t n, size_t size) __THROW ALIAS(my_calloc);
void* memalign(size_t align, size_t size) __THROW ALIAS(my_memalign);
void* aligned_alloc(size_t align, size_t size) __THROW ALIAS(my_aligned_alloc);
void* valloc(size_t size) __THROW ALIAS(my_valloc);
void* pvalloc(size_t size) __THROW ALIAS(my_pvalloc);
int posix_memalign(void** r, size_t a, size_t s) __THROW ALIAS(my_posix_memalign);
}