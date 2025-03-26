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

#include "storage/fixed_fifo_cache.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace starrocks {

TEST(FixedFifoCacheTest, test_get_put) {
    size_t capacity = 2;
    size_t expire = 2000; // 2000ms
    FixedFIFOCache<int, int> cache(capacity, expire);

    cache.put(1, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    cache.put(2, 2);
    auto v1 = cache.get(1);
    EXPECT_TRUE(v1);
    EXPECT_EQ(1, *v1);
    auto v2 = cache.get(2);
    EXPECT_TRUE(v2);
    EXPECT_EQ(2, *v2);
    auto v3 = cache.get(3);
    EXPECT_FALSE(v3);

    // will evict the key(1) due to the capacity exceeded
    cache.put(3, 3);
    auto v4 = cache.get(3);
    EXPECT_TRUE(v4);
    EXPECT_EQ(3, *v4);

    auto v5 = cache.get(1);
    EXPECT_FALSE(v5);
}

TEST(FixedFifoCacheTest, test_auto_expire) {
    size_t expire = 1000; // 2000ms
    FixedFIFOCache<int, int> cache1(2, expire);
    FixedFIFOCache<int, int> cache2(5, expire);

    cache1.put(1, 1);
    cache2.put(1, 1);
    {
        auto v1 = cache1.get(1);
        auto v2 = cache2.get(1);
        EXPECT_TRUE(v1);
        EXPECT_TRUE(v2);
        EXPECT_EQ(1, *v1);
        EXPECT_EQ(1, *v2);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    cache1.put(2, 2);
    cache2.put(2, 2);
    {
        auto v1 = cache1.get(1);
        auto v2 = cache2.get(1);
        EXPECT_TRUE(v1);
        EXPECT_TRUE(v2);
        EXPECT_EQ(1, *v1);
        EXPECT_EQ(1, *v2);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    cache1.put(3, 3); // will evict key(1), remain key(2), key(3)
    cache2.put(3, 3); // remain key(2), key(3)
    {
        auto v1 = cache1.get(1);
        auto v2 = cache2.get(1);
        EXPECT_FALSE(v1);
        EXPECT_TRUE(v2);
        EXPECT_EQ(1, *v2);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(expire));
    // sleep expire ms, both cache will not be able to retrieve the key(2)
    {
        auto v1 = cache1.get(2);
        auto v2 = cache2.get(2);
        EXPECT_FALSE(v1);
        EXPECT_FALSE(v2);
    }
}

} // namespace starrocks
