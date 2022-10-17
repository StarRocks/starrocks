// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/page_cache_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/page_cache.h"

#include <gtest/gtest.h>

#include "runtime/mem_tracker.h"

namespace starrocks {

class StoragePageCacheTest : public testing::Test {
public:
    StoragePageCacheTest() { _mem_tracker = std::make_unique<MemTracker>(); }
    virtual ~StoragePageCacheTest() {}

private:
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
};

// NOLINTNEXTLINE
TEST_F(StoragePageCacheTest, normal) {
    StoragePageCache cache(_mem_tracker.get(), kNumShards * 2048);

    StoragePageCache::CacheKey key("abc", 0);
    StoragePageCache::CacheKey memory_key("mem", 0);

    {
        // insert normal page
        char* buf = new char[1024];
        PageCacheHandle handle;
        Slice data(buf, 1024);
        cache.insert(key, data, &handle, false);

        ASSERT_EQ(handle.data().data, buf);

        auto found = cache.lookup(key, &handle);
        ASSERT_TRUE(found);
        ASSERT_EQ(buf, handle.data().data);
    }

    {
        // insert in_memory page
        char* buf = new char[1024];
        PageCacheHandle handle;
        Slice data(buf, 1024);
        cache.insert(memory_key, data, &handle, true);

        ASSERT_EQ(handle.data().data, buf);

        auto found = cache.lookup(memory_key, &handle);
        ASSERT_TRUE(found);
    }

    // put too many page to eliminate first page
    for (int i = 0; i < 10 * kNumShards; ++i) {
        StoragePageCache::CacheKey key("bcd", i);
        PageCacheHandle handle;
        Slice data(new char[1024], 1024);
        cache.insert(key, data, &handle, false);
    }

    // cache miss
    {
        PageCacheHandle handle;
        StoragePageCache::CacheKey miss_key("abc", 1);
        auto found = cache.lookup(miss_key, &handle);
        ASSERT_FALSE(found);
    }

    // cache miss for eliminated key
    {
        PageCacheHandle handle;
        auto found = cache.lookup(key, &handle);
        ASSERT_FALSE(found);
    }

    // set capacity
    {
        size_t ori = cache.get_capacity();
        cache.set_capacity(ori / 2);
        ASSERT_EQ(ori / 2, cache.get_capacity());
        cache.set_capacity(ori);
    }

    // adjust capacity
    {
        size_t ori = cache.get_capacity();
        for (int i = 1; i <= 10; i++) {
            cache.adjust_capacity(32);
            ASSERT_EQ(cache.get_capacity(), ori + 32 * i);
        }
        cache.set_capacity(ori);
        for (int i = 1; i <= 10; i++) {
            cache.adjust_capacity(-32);
            ASSERT_EQ(cache.get_capacity(), ori - 32 * i);
        }
        cache.set_capacity(ori);

        int64_t delta = ori;
        ASSERT_FALSE(cache.adjust_capacity(-delta / 2, ori));
        ASSERT_EQ(cache.get_capacity(), ori);
        ASSERT_TRUE(cache.adjust_capacity(-delta / 2, ori / 4));
        cache.set_capacity(ori);

        // overflow
        cache.set_capacity(kNumShards);
        ASSERT_FALSE(cache.adjust_capacity(-2 * kNumShards, 0));
    }
}

} // namespace starrocks
