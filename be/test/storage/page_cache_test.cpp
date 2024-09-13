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

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "runtime/mem_tracker.h"

DECLARE_int32(v);

namespace starrocks {

class StoragePageCacheTest : public testing::TestWithParam<bool> {
public:
    StoragePageCacheTest() { _mem_tracker = std::make_unique<MemTracker>(); }
    ~StoragePageCacheTest() override = default;

    std::shared_ptr<ObjectCache> create_obj_cache(size_t capacity) {
        bool based_on_datacache = GetParam();
        ObjectCacheOptions options;
        options.capacity = capacity;
        options.module = ObjectCacheModuleType::LRUCACHE;
#ifdef WITH_STARCACHE
        if (based_on_datacache) {
            options.module = ObjectCacheModuleType::STARCACHE;
        }
#endif
        auto obj_cache = std::make_shared<ObjectCache>();
        (void)obj_cache->init(options);
        return obj_cache;
    }  

private:
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
    std::shared_ptr<ObjectCache> _obj_cache = nullptr;
    bool _based_on_datacache;
};

// NOLINTNEXTLINE
TEST_P(StoragePageCacheTest, normal) {
    size_t capacity = kNumShards * 2048;
    auto obj_cache = create_obj_cache(capacity);
    StoragePageCache cache(_mem_tracker.get(), capacity, obj_cache.get());

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
        Status st = cache.insert(key, data, &handle, false);
        if (!st.ok()) {
            // Some items may be inserted failed in starcache module because the shard distribution.
            LOG(INFO) << "free data when inserting failed";
            delete[] data.data;
        }
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

    // set capactity = 0
    {
        cache.set_capacity(0);
        ASSERT_EQ(cache.get_capacity(), 0);
    }

    obj_cache->shutdown();
}

TEST_P(StoragePageCacheTest, metrics) {
    size_t capacity = kNumShards * 2048;
    auto obj_cache = create_obj_cache(capacity);
    StoragePageCache cache(_mem_tracker.get(), capacity, obj_cache.get());

    // Insert a piece of data, but the application layer does not release it.
    StoragePageCache::CacheKey key1("abc", 0);
    char* buf = new char[1024];
    PageCacheHandle handle;
    Slice data(buf, 1024);
    cache.insert(key1, data, &handle, false);

    StoragePageCache::CacheKey key2("def", 0);
    {
        // Insert another piece of data and release it from the application layer.
        char* buf = new char[1024];
        PageCacheHandle handle;
        Slice data(buf, 1024);
        cache.insert(key2, data, &handle, false);
    }

    // At this point the cache should have two entries, one for user owner and one for cache Owenr.
    PageCacheHandle handle1;
    auto found = cache.lookup(key1, &handle);
    ASSERT_TRUE(found);
    PageCacheHandle handle2;
    found = cache.lookup(key2, &handle2);
    ASSERT_TRUE(found);
    ASSERT_EQ(cache.get_lookup_count(), 2);
    ASSERT_EQ(cache.get_hit_count(), 2);
    // Test the cache miss
    for (int i = 0; i < 1024; i++) {
        PageCacheHandle handle;
        StoragePageCache::CacheKey key(std::to_string(i), 0);
        cache.lookup(key, &handle);
    }
    ASSERT_EQ(cache.get_lookup_count(), 2 + 1024);
    ASSERT_EQ(cache.get_hit_count(), 2);

    obj_cache->shutdown();
}

INSTANTIATE_TEST_SUITE_P(StoragePageCacheTest, StoragePageCacheTest, ::testing::Values(false, true));

} // namespace starrocks
