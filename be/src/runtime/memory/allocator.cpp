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

#include "runtime/memory/allocator.h"

#include <cstdlib>
#include "malloc.h"

namespace starrocks {

void* Allocator::alloc(size_t size) {
    return ::malloc(size);
}

void Allocator::free(void* ptr) {
    ::free(ptr);
}

void* Allocator::realloc(void* ptr, size_t size) {
    return ::realloc(ptr, size);
}

void* Allocator::calloc(size_t n, size_t size) {
    return ::calloc(n, size);
}

void Allocator::cfree(void* ptr) {
    free(ptr);
}

void* Allocator::memalign(size_t align, size_t size) {
    return ::memalign(align, size);
}
void* Allocator::aligned_alloc(size_t align, size_t size) {
    return ::aligned_alloc(align, size);
}

void* Allocator::valloc(size_t size) {
    return ::valloc(size);
}

void* Allocator::pvalloc(size_t size) {
    return ::pvalloc(size);
}

int Allocator::posix_memalign(void** ptr, size_t align, size_t size) {
    return ::posix_memalign(ptr, align, size);
}



}
