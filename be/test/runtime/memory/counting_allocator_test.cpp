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

#include "runtime/memory/counting_allocator.h"

#include <gtest/gtest.h>

#include <vector>

#include "util/phmap/phmap.h"

namespace starrocks {

TEST(CountingAllocatorTest, normal) {
    int64_t memory_usage = 0;
    {
        // stl container
        memory_usage = 0;
        std::vector<int, CountingAllocator<int>> vec{CountingAllocator<int>(&memory_usage)};
        for (int i = 0; i < 100; ++i) {
            vec.push_back(i);
        }
        ASSERT_EQ(memory_usage, 512);
        vec.resize(10);
        ASSERT_EQ(memory_usage, 512);
        vec.shrink_to_fit();
        ASSERT_EQ(memory_usage, 40);
    }
    ASSERT_EQ(memory_usage, 0);
    {
        // phmap
        phmap::flat_hash_map<int, int, phmap::priv::hash_default_hash<int>, phmap::priv::hash_default_eq<int>,
                             CountingAllocator<int>>
                m{CountingAllocator<int>(&memory_usage)};
        m.insert({1, 1});
        ASSERT_EQ(memory_usage, 28);
        m.insert({2, 2});
        ASSERT_EQ(memory_usage, 44);
    }
    ASSERT_EQ(memory_usage, 0);
}
} // namespace starrocks