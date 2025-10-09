// macOS shim for missing <malloc.h>
// Keep StarRocks source unchanged by providing a trampoline header.
#ifndef STARROCKS_MAC_MALLOC_SHIM_H
#define STARROCKS_MAC_MALLOC_SHIM_H

#ifdef __APPLE__
#include <malloc/malloc.h>
#include <stdlib.h>
#include <unistd.h>

// Provide GNU extensions expected by some codepaths on Linux
// Implement memalign using posix_memalign on macOS
static inline void* memalign(size_t alignment, size_t size) {
    void* p = nullptr;
    if (posix_memalign(&p, alignment, size) != 0) return nullptr;
    return p;
}

// Provide pvalloc: allocate page-aligned memory rounded up to page size
static inline void* pvalloc(size_t size) {
    size_t page = (size_t)getpagesize();
    if (page == 0) page = 4096;
    size_t rounded = (size + page - 1) / page * page;
    // valloc is deprecated but still available; alternatives are posix_memalign
    // but valloc matches pvalloc semantics best here.
    return valloc(rounded);
}
#else
#include <malloc.h>
#endif

#endif // STARROCKS_MAC_MALLOC_SHIM_H
