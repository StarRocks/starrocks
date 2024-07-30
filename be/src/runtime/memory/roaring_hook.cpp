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
#include "util/logging.h"

namespace starrocks {

void* my_roaring_malloc(size_t bytes) {
    return malloc(bytes);
}

void* my_roaring_realloc(void* ptr, size_t size) {
    return realloc(ptr, size);
}

void* my_roaring_calloc(size_t n, size_t size) {
    return calloc(n, size);
}

void my_roaring_free(void* ptr) {
    return free(ptr);
}

void* my_roaring_aligned_malloc(size_t align, size_t size) {
    return aligned_alloc(align, size);
}

void my_roaring_aligned_free(void* ptr) {
    return free(ptr);
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