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

#include "common/compiler_util.h"
#include "runtime/memory/allocator.h"

#include <cstdlib>
#include "malloc.h"

namespace starrocks {

class MemHookAllocator: public AllocatorFactory<Allocator, MemHookAllocator> {
public:
    ALWAYS_INLINE void* alloc(size_t size) override {
        return ::malloc(size);
    }

    ALWAYS_INLINE void free(void* ptr) override {
        ::free(ptr);
    }

    ALWAYS_INLINE void* realloc(void* ptr, size_t size) override {
        return ::realloc(ptr, size);
    }

    ALWAYS_INLINE void* calloc(size_t n, size_t size) override {
        return ::calloc(n, size);
    }

    ALWAYS_INLINE void cfree(void* ptr) override {
        ::free(ptr);
    }

    ALWAYS_INLINE void* memalign(size_t align, size_t size) override {
        return ::memalign(align, size);
    }
    ALWAYS_INLINE void* aligned_alloc(size_t align, size_t size) override {
        return ::aligned_alloc(align, size);
    }

    ALWAYS_INLINE void* valloc(size_t size) override {
        return ::valloc(size);
    }

    ALWAYS_INLINE void* pvalloc(size_t size) override {
        return ::pvalloc(size);
    }

    ALWAYS_INLINE int posix_memalign(void** ptr, size_t align, size_t size) override {
        return ::posix_memalign(ptr, align, size);
    }
};


class NoInlineMemHookAllocator: public AllocatorFactory<Allocator, NoInlineMemHookAllocator> {
public:
    void* alloc(size_t size) override {
        return ::malloc(size);
    }

    void free(void* ptr) override {
        ::free(ptr);
    }

    void* realloc(void* ptr, size_t size) override {
        return ::realloc(ptr, size);
    }

    void* calloc(size_t n, size_t size) override {
        return ::calloc(n, size);
    }

    void cfree(void* ptr) override {
        ::free(ptr);
    }

    void* memalign(size_t align, size_t size) override {
        return ::memalign(align, size);
    }
    void* aligned_alloc(size_t align, size_t size) override {
        return ::aligned_alloc(align, size);
    }

    void* valloc(size_t size) override {
        return ::valloc(size);
    }

    void* pvalloc(size_t size) override {
        return ::pvalloc(size);
    }

    int posix_memalign(void** ptr, size_t align, size_t size) override {
        return ::posix_memalign(ptr, align, size);
    }
};
}