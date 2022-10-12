// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/memory/memory_resource.h"

#include <gtest/gtest.h>

#include <vector>

namespace starrocks {
TEST(StackMemoryResource, Normal) {
    // case allocate from stack
    char buffer[1024];
    stack_memory_resource mr(buffer, sizeof(buffer));
    std::pmr::vector<char> res(&mr);
    // allocate 4 bytes from stack
    res.resize(4);
    ASSERT_EQ((char*)res.data(), buffer);

    // allocate 128 bytes
    res.resize(128 / sizeof(char));
    ASSERT_EQ((char*)res.data(), buffer + 4 * sizeof(char));

    // allocate from heap
    res.resize(1024);
    ASSERT_FALSE((char*)res.data() >= (char*)mr._stack_addr_start && (char*)res.data() <= (char*)mr._stack_addr_end);

    res.resize(0);
    res.shrink_to_fit();

    // allocate from stack
    res.resize(1);
    ASSERT_TRUE((char*)res.data() >= (char*)mr._stack_addr_start && (char*)res.data() <= (char*)mr._stack_addr_end);
}
} // namespace starrocks