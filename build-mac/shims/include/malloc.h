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

// macOS shim for missing <malloc.h>
// Keep StarRocks source unchanged by providing a trampoline header.
#ifndef STARROCKS_MAC_MALLOC_SHIM_H
#define STARROCKS_MAC_MALLOC_SHIM_H

#ifdef __APPLE__
#include <malloc/malloc.h>
#include <stdlib.h>
#include <unistd.h>

// Provide malloc_usable_size for macOS with proper handling of jemalloc
#ifndef MALLOC_USABLE_SIZE_DEFINED
#define MALLOC_USABLE_SIZE_DEFINED

// Check if jemalloc is available
#if __has_include(<jemalloc/jemalloc.h>)
#include <jemalloc/jemalloc.h>
#endif

// Check if malloc_usable_size is already defined by jemalloc
#ifndef malloc_usable_size
static inline size_t malloc_usable_size(void* ptr) {
    if (ptr == nullptr) return 0;
    return malloc_size(ptr);
}
#endif

#endif // MALLOC_USABLE_SIZE_DEFINED

// Provide GNU extensions expected by some codepaths on Linux
// Implement memalign using posix_memalign on macOS
static inline void* memalign(size_t alignment, size_t size) {
    void* p = nullptr;
    if (posix_memalign(&p, alignment, size) != 0) return nullptr;
    return p;
}

// Provide pvalloc: allocate page-aligned memory rounded up to page size
static inline void* pvalloc(size_t size) {
    long pagesize = sysconf(_SC_PAGESIZE);
    if (pagesize <= 0) pagesize = 4096;
    size_t n = ((size + (size_t)pagesize - 1) / (size_t)pagesize) * (size_t)pagesize;
    return valloc(n);
}

// Note: TCMalloc/gperftools stub functions are provided separately
// in gperftools_stubs.cpp to avoid conflicts with system headers

#else
#include <malloc.h>
#endif

#endif // STARROCKS_MAC_MALLOC_SHIM_H
