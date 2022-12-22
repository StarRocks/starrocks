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

#include "column/bytes.h"

#include <gtest/gtest.h>

#include <atomic>
#include <ctime>

#include "malloc.h"

extern "C" {
typedef void (*MallocHook_NewHook)(const void* ptr, size_t size);
int MallocHook_AddNewHook(MallocHook_NewHook hook);
int MallocHook_RemoveNewHook(MallocHook_NewHook hook);
} // extern "C"

extern std::atomic<int64_t> g_mem_usage;

static size_t count = 0;
void NewHook(const void* ptr, size_t size) {
    count += size;
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace starrocks {

#define withHook(expr)                      \
    {                                       \
        MallocHook_AddNewHook(&NewHook);    \
        count = 0;                          \
        expr;                               \
        MallocHook_RemoveNewHook(&NewHook); \
    }

// NOLINTNEXTLINE
TEST(BytesTest, test_reserve) {
    Bytes b;
    withHook({ b.reserve(20); });

    ASSERT_EQ(20 + 16, count);
    ASSERT_EQ(0, b.size());
    ASSERT_EQ(20, b.capacity());
}

// NOLINTNEXTLINE
TEST(BytesTest, test_resize) {
    Bytes b;
    withHook({ b.resize(20); });
    ASSERT_EQ(20 + 16, count);
    ASSERT_EQ(20, b.size());
    ASSERT_EQ(20, b.capacity());
}

// NOLINTNEXTLINE
TEST(BytesTest, test_shrink_to_fit) {
    Bytes b = {0, 1, 2, 3, 4, 5, 6, 7};
    b.reserve(64);
    withHook({ b.shrink_to_fit(); });
    ASSERT_EQ(8 + 16, count);
    ASSERT_EQ(8, b.size());
    ASSERT_EQ(8, b.capacity());
}

// NOLINTNEXTLINE
TEST(BytesTest, test_insert) {
    Bytes b = {0, 1, 2, 3, 4, 5, 6, 7};
    withHook({ b.insert(b.end(), 9); });
    ASSERT_EQ(16 + 16, count);
    ASSERT_EQ(9, b.size());
    ASSERT_EQ(16, b.capacity());
}

// NOLINTNEXTLINE
TEST(BytesTest, test_emplace) {
    Bytes b = {0, 1, 2, 3, 4, 5, 6, 7};
    withHook({ b.emplace(b.end(), 9); });
    ASSERT_EQ(16 + 16, count);
    ASSERT_EQ(9, b.size());
    ASSERT_EQ(16, b.capacity());
}

// NOLINTNEXTLINE
TEST(BytesTest, test_push_back) {
    Bytes b = {0, 1, 2, 3, 4, 5, 6, 7};
    withHook({ b.push_back(8); });
    ASSERT_EQ(16 + 16, count);
    ASSERT_EQ(9, b.size());
    ASSERT_EQ(16, b.capacity());
}

// NOLINTNEXTLINE
TEST(BytesTest, test_emplace_back) {
    Bytes b = {0, 1, 2, 3, 4, 5, 6, 7};
    withHook({ b.emplace_back(8); });
    ASSERT_EQ(16 + 16, count);
    ASSERT_EQ(9, b.size());
    ASSERT_EQ(16, b.capacity());
}

// NOLINTNEXTLINE
TEST(BytesTest, test_alloc_without_init) {
    Bytes b = {0, 1, 2, 3, 4, 5, 6, 7};

    // resize should not initialize 4, 5, 6, 7
    b.resize(4);
    b.resize(8);

    for (int i = 0; i < 8; ++i) {
        ASSERT_EQ(uint8_t(i), b[i]);
    }

    ASSERT_EQ(8, b.size());
    ASSERT_EQ(8, b.capacity());
}

// NOLINTNEXTLINE
TEST(BytesTest, test_hook_malloc) {
    srand((int)time(nullptr));

    void* ptr;
    int before;
    int after;
    int size;

    for (int i = 0; i < 1000; i++) {
        size = rand() % (8 * 1024 * 1024);
        before = g_mem_usage;
        ptr = malloc(size);
        free(ptr);
        after = g_mem_usage;
        ASSERT_EQ(before, after);
    }

    // alloc 0
    before = g_mem_usage;
    ptr = malloc(0);
    free(ptr);
    after = g_mem_usage;
    ASSERT_EQ(before, after);
}

// NOLINTNEXTLINE
TEST(BytesTest, test_hook_realloc) {
    srand((int)time(nullptr));

    void* ptr;
    void* new_ptr;
    int before;
    int after;
    int size;
    int new_size;

    for (int i = 0; i < 1000; i++) {
        size = rand() % (8 * 1024 * 1024);
        before = g_mem_usage;
        ptr = malloc(size);
        new_size = rand() % (8 * 1024 * 1024);
        new_ptr = realloc(ptr, new_size);
        free(new_ptr);
        after = g_mem_usage;
        ASSERT_EQ(before, after);
    }

    // alloc 0
    before = g_mem_usage;
    ptr = malloc(1024);
    new_ptr = realloc(ptr, 0);
    ASSERT_TRUE(new_ptr == nullptr);
    free(ptr);
    after = g_mem_usage;
    ASSERT_EQ(before, after);

    before = g_mem_usage;
    new_ptr = realloc(nullptr, 1024);
    free(new_ptr);
    after = g_mem_usage;
    ASSERT_EQ(before, after);
}

// NOLINTNEXTLINE
TEST(BytesTest, test_hook_calloc) {
    srand((int)time(nullptr));

    void* ptr;
    int before;
    int after;
    int size;
    int count;

    for (int i = 0; i < 1000; i++) {
        size = rand() % (1024 * 1024);
        count = rand() % 10;
        before = g_mem_usage;
        ptr = calloc(count, size);
        if (size == 0) {
            ASSERT_TRUE(ptr == nullptr);
        } else {
            free(ptr);
        }
        after = g_mem_usage;
        ASSERT_EQ(before, after);
    }

    // alloc 0
    before = g_mem_usage;
    ptr = calloc(0, 0);
    ASSERT_TRUE(ptr == nullptr);
    after = g_mem_usage;
    ASSERT_EQ(before, after);
}

// NOLINTNEXTLINE
TEST(BytesTest, test_hook_memalign) {
    srand((int)time(nullptr));

    void* ptr;
    int before;
    int after;
    int size;
    int align;

    for (int i = 0; i < 1000; i++) {
        size = rand() % (8 * 1024 * 1024);
        align = rand() % (8 * 1024 * 1024);
        before = g_mem_usage;
        ptr = memalign(align, size);
        free(ptr);
        after = g_mem_usage;
        ASSERT_EQ(before, after);
    }

    // alloc 0
    before = g_mem_usage;
    ptr = memalign(4, 0);
    free(ptr);
    after = g_mem_usage;
    ASSERT_EQ(before, after);
}

// NOLINTNEXTLINE
TEST(BytesTest, test_hook_aligned_alloc) {
    srand((int)time(nullptr));

    void* ptr;
    int before;
    int after;
    int size;
    int align;

    for (int i = 0; i < 1000; i++) {
        size = rand() % (8 * 1024 * 1024);
        align = rand() % (8 * 1024 * 1024);
        before = g_mem_usage;
        ptr = aligned_alloc(align, size);
        free(ptr);
        after = g_mem_usage;
        ASSERT_EQ(before, after);
    }

    // alloc 0
    before = g_mem_usage;
    ptr = aligned_alloc(4, 0);
    free(ptr);
    after = g_mem_usage;
    ASSERT_EQ(before, after);
}

// NOLINTNEXTLINE
TEST(BytesTest, test_hook_valloc) {
    srand((int)time(nullptr));

    void* ptr;
    int before;
    int after;
    int size;

    for (int i = 0; i < 1000; i++) {
        size = rand() % (8 * 1024 * 1024);
        before = g_mem_usage;
        ptr = valloc(size);
        free(ptr);
        after = g_mem_usage;
        ASSERT_EQ(before, after);
    }

    // alloc 0
    before = g_mem_usage;
    ptr = valloc(0);
    free(ptr);
    after = g_mem_usage;
    ASSERT_EQ(before, after);
}

// NOLINTNEXTLINE
TEST(BytesTest, test_hook_pvalloc) {
    srand((int)time(nullptr));

    void* ptr;
    int before;
    int after;
    int size;

    for (int i = 0; i < 1000; i++) {
        size = rand() % (8 * 1024 * 1024);
        before = g_mem_usage;
        ptr = pvalloc(size);
        free(ptr);
        after = g_mem_usage;
        ASSERT_EQ(before, after);
    }

    // alloc 0
    before = g_mem_usage;
    ptr = pvalloc(0);
    free(ptr);
    after = g_mem_usage;
    ASSERT_EQ(before, after);
}

// NOLINTNEXTLINE
TEST(BytesTest, test_hook_posix_memalign) {
    srand((int)time(nullptr));

    void* ptr = nullptr;
    int before;
    int after;
    int size;
    int align;
    int ret;

    for (int i = 0; i < 1000; i++) {
        size = rand() % (8 * 1024 * 1024);
        align = 2 ^ (3 + rand() % 10);
        before = g_mem_usage;
        if (align % 8 == 0) {
            ret = posix_memalign(&ptr, align, size);
            ASSERT_EQ(ret, 0);
            free(ptr);
        } else {
            ret = posix_memalign(&ptr, align, size);
            ASSERT_NE(ret, 0);
        }
        after = g_mem_usage;
        ASSERT_EQ(before, after);
    }

    // alloc 0
    before = g_mem_usage;
    ret = posix_memalign(&ptr, 8, 0);
    free(ptr);
    after = g_mem_usage;
    ASSERT_EQ(before, after);

    // alloc failed
    ptr = nullptr;
    before = g_mem_usage;
    ret = posix_memalign(&ptr, 1, 8);
    ASSERT_TRUE(ret != 0);
    ASSERT_TRUE(ptr == nullptr);
    after = g_mem_usage;
    ASSERT_EQ(before, after);
}

} // namespace starrocks
