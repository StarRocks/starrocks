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

#include "runtime/memory/roaring_hook.h"

#include <malloc.h>

#include <cstdlib>

#include "roaring/memory.h"
#include "runtime/memory/mem_hook_allocator.h"

namespace starrocks {

static MemHookAllocator kDefaultRoaringAllocator = MemHookAllocator{};
inline thread_local Allocator* tls_roaring_allocator = &kDefaultRoaringAllocator;

ThreadLocalRoaringAllocatorSetter::ThreadLocalRoaringAllocatorSetter(Allocator* allocator) {
    _prev = tls_roaring_allocator;
    tls_roaring_allocator = allocator;
}

ThreadLocalRoaringAllocatorSetter::~ThreadLocalRoaringAllocatorSetter() {
    tls_roaring_allocator = _prev;
}

void* my_roaring_malloc(size_t bytes) {
    return tls_roaring_allocator->alloc(bytes);
}

void* my_roaring_realloc(void* ptr, size_t size) {
    return tls_roaring_allocator->realloc(ptr, size);
}

void* my_roaring_calloc(size_t n, size_t size) {
    return tls_roaring_allocator->calloc(n, size);
}

void my_roaring_free(void* ptr) {
    return tls_roaring_allocator->free(ptr);
}

void* my_roaring_aligned_malloc(size_t align, size_t size) {
    return tls_roaring_allocator->aligned_alloc(align, size);
}

void my_roaring_aligned_free(void* ptr) {
    return tls_roaring_allocator->free(ptr);
}

void init_roaring_hook() {
    roaring_memory_t global_memory_hook = {
            .malloc = my_roaring_malloc,
            .realloc = my_roaring_realloc,
            .calloc = my_roaring_calloc,
            .free = my_roaring_free,
            .aligned_malloc = my_roaring_aligned_malloc,
            .aligned_free = my_roaring_aligned_free,
    };
    roaring_init_memory_hook(global_memory_hook);
}
} // namespace starrocks