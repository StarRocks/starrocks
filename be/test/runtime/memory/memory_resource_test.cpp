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
