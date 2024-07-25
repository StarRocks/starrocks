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
#include "runtime/memory/mem_hook_allocator.h"
#include "util/phmap/btree.h"
#include "util/phmap/phmap.h"

#include <cstdlib>
#include "malloc.h"

namespace starrocks {

class DebugAllocator: public AllocatorFactory<MemHookAllocator, DebugAllocator> {
public:
    ALWAYS_INLINE void update_counter(phmap::btree_map<size_t, size_t> &m, size_t key) {
        auto iter = m.find(key);
        if (iter == m.end()) {
            m.insert({key, 1});
        } else {
            iter->second += 1;
        }
    }

    ALWAYS_INLINE void* alloc(size_t size) override {
        void* result = MemHookAllocator::alloc(size);
        size_t key = malloc_usable_size(result);
        update_counter(_malloc_counter, key);
        return result;
    }
    ALWAYS_INLINE void free(void* ptr) override {
        size_t key = malloc_usable_size(ptr);
        MemHookAllocator::free(ptr);
        update_counter(_free_counter, key);
    }

    void* realloc(void* ptr, size_t size) override {
        void* result = MemHookAllocator::realloc(ptr, size);
        return result;
    }

    void* calloc(size_t n, size_t size) override {
        void* result = MemHookAllocator::calloc(n, size);
        return result;
    }

    void cfree(void* ptr) override {
        MemHookAllocator::cfree(ptr);
    }

    void* memalign(size_t align, size_t size) override {
        void* result = MemHookAllocator::memalign(align, size);
        return result;
    }
    void* aligned_alloc(size_t align, size_t size) override {
        void* result = MemHookAllocator::aligned_alloc(align, size);
        return result;
    }

    void* valloc(size_t size) override {
        void* result = MemHookAllocator::valloc(size);
        return result;
    }

    void* pvalloc(size_t size) override {
        void* result = MemHookAllocator::pvalloc(size);
        return result;
    }

    int posix_memalign(void** ptr, size_t align, size_t size) override {
        int result = MemHookAllocator::posix_memalign(ptr, align, size);
        return result;
    }

public:
    phmap::btree_map<size_t, size_t> _malloc_counter;
    phmap::btree_map<size_t, size_t> _free_counter;
};

}