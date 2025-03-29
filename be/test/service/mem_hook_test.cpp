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

// FIXME: The case doesn't work on ASAN case since the malloc will be hooked
// when doing the ASAN init. Can't make it work so just disable it for now.
#ifndef ADDRESS_SANITIZER

#include "service/mem_hook.h"

#include <gtest/gtest.h>

#include <vector>

namespace starrocks {

static void try_malloc_memory(size_t size) {
    std::vector<int8_t> arr;
    arr.reserve(size);
    EXPECT_EQ(size, arr.capacity());
}

static void try_calloc_memory(size_t n, size_t size) {
    void* ptr = calloc(n, size);
    if (ptr == nullptr) {
        throw std::bad_alloc();
    }
    free(ptr);
}

TEST(MemhookTest, test_malloc_mem_hook_block_without_try_catch) {
    set_large_memory_alloc_failure_threshold(0);
    int64_t blocking_size = 1024 * 1024;

    set_large_memory_alloc_failure_threshold(blocking_size);
    // successful because blocking_size <= g_large_memory_alloc_failure_threshold
    EXPECT_NO_THROW(try_malloc_memory(blocking_size));

    set_large_memory_alloc_failure_threshold(blocking_size - 1);
    // fail with std::bad_alloc for the same size
    EXPECT_THROW(try_malloc_memory(blocking_size), std::bad_alloc);

    // reset to no limit
    set_large_memory_alloc_failure_threshold(0);
}

TEST(MemhookTest, test_malloc_mem_hook_block_with_try_catch) {
    // IS_BAD_ALLOC_CATCHED is always `false` when builds with -DBE_TEST.
    // There is no easy way to simulate the mem_tracker limit check here.
    // Leave the test body empty as intended for now.
}

TEST(MemhookTest, test_calloc_mem_hook_block_without_try_catch) {
    int64_t blocking_size = 1024 * 1024;

    set_large_memory_alloc_failure_threshold(blocking_size);
    // successful because blocking_size <= g_large_memory_alloc_failure_threshold
    EXPECT_NO_THROW(try_calloc_memory(blocking_size / sizeof(int), sizeof(int)));

    set_large_memory_alloc_failure_threshold(blocking_size - 1);
    // fail with std::bad_alloc for the same size
    EXPECT_THROW(try_calloc_memory(blocking_size / sizeof(int), sizeof(int)), std::bad_alloc);

    // reset to no limit
    set_large_memory_alloc_failure_threshold(0);
}
} // namespace starrocks
#endif
