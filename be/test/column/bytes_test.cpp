// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "column/bytes.h"

#include <gtest/gtest.h>

extern "C" {
typedef void (*MallocHook_NewHook)(const void* ptr, size_t size);
int MallocHook_AddNewHook(MallocHook_NewHook hook);
int MallocHook_RemoveNewHook(MallocHook_NewHook hook);
} // extern "C"

static size_t count = 0;
void NewHook(const void* ptr, size_t size) {
    count += size;
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace starrocks::vectorized {

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

} // namespace starrocks::vectorized
