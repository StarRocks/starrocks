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

#include <gtest/gtest.h>

#include "testutil/assert.h"

namespace starrocks {

class StoragePageCacheTest : public testing::Test {
protected:
    StoragePageCacheTest() = default;
    ~StoragePageCacheTest() override = default;

    void SetUp() override;
    void create_obj_cache(size_t capacity);

    std::shared_ptr<ObjectCache> _obj_cache;
    std::shared_ptr<StoragePageCache> _page_cache;
    size_t _capacity = kNumShards * 2048;
};

void StoragePageCacheTest::SetUp() {
    create_obj_cache(_capacity);
    _page_cache = std::make_shared<StoragePageCache>(_obj_cache.get());
}

void StoragePageCacheTest::create_obj_cache(size_t capacity) {
    ObjectCacheOptions options;
    options.capacity = capacity;
    options.module = ObjectCacheModuleType::LRUCACHE;

    _obj_cache = std::make_shared<ObjectCache>();
    ASSERT_OK(_obj_cache->init(options));
}

// NOLINTNEXTLINE
TEST_F(StoragePageCacheTest, normal) {
    {
        // insert normal page
        StoragePageCache::CacheKey key("abc", 0);
        Slice data(new char[1024], 1024);
        PageCacheHandle handle;

        ASSERT_OK(_page_cache->insert(key, data, &handle, false));
        ASSERT_EQ(handle.data().data, data.data);

        ASSERT_TRUE(_page_cache->lookup(key, &handle));
        ASSERT_EQ(data.data, handle.data().data);
    }

    {
        // insert in_memory page
        StoragePageCache::CacheKey memory_key("mem", 0);
        Slice data(new char[1024], 1024);
        PageCacheHandle handle;

        ASSERT_OK(_page_cache->insert(memory_key, data, &handle, true));
        ASSERT_EQ(handle.data().data, data.data);

        ASSERT_TRUE(_page_cache->lookup(memory_key, &handle));
    }

    // put too many page to eliminate first page
    for (int i = 0; i < 10 * kNumShards; ++i) {
        StoragePageCache::CacheKey key("bcd", i);
        Slice data(new char[1024], 1024);
        PageCacheHandle handle;
        ASSERT_OK(_page_cache->insert(key, data, &handle, false));
    }

    // cache miss
    {
        PageCacheHandle handle;
        StoragePageCache::CacheKey miss_key("abc", 1);
        auto found = _page_cache->lookup(miss_key, &handle);
        ASSERT_FALSE(found);
    }

    // cache miss for eliminated key
    {
        StoragePageCache::CacheKey key("abc", 0);
        PageCacheHandle handle;
        ASSERT_FALSE(_page_cache->lookup(key, &handle));
    }

    // set capacity
    {
        size_t ori = _page_cache->get_capacity();
        _page_cache->set_capacity(ori / 2);
        ASSERT_EQ(ori / 2, _page_cache->get_capacity());
        _page_cache->set_capacity(ori);
    }

    // adjust capacity
    {
        size_t ori = _page_cache->get_capacity();
        for (int i = 1; i <= 10; i++) {
            _page_cache->adjust_capacity(32);
            ASSERT_EQ(_page_cache->get_capacity(), ori + 32 * i);
        }
        _page_cache->set_capacity(ori);
        for (int i = 1; i <= 10; i++) {
            _page_cache->adjust_capacity(-32);
            ASSERT_EQ(_page_cache->get_capacity(), ori - 32 * i);
        }
        _page_cache->set_capacity(ori);

        int64_t delta = ori;
        ASSERT_FALSE(_page_cache->adjust_capacity(-delta / 2, ori));
        ASSERT_EQ(_page_cache->get_capacity(), ori);
        ASSERT_TRUE(_page_cache->adjust_capacity(-delta / 2, ori / 4));
        _page_cache->set_capacity(ori);

        // overflow
        _page_cache->set_capacity(kNumShards);
        ASSERT_FALSE(_page_cache->adjust_capacity(-2 * kNumShards, 0));
    }

    // set capactity = 0
    {
        _page_cache->set_capacity(0);
        ASSERT_EQ(_page_cache->get_capacity(), 0);
    }
}

TEST_F(StoragePageCacheTest, metrics) {
    StoragePageCache::CacheKey key1("abc", 0);
    StoragePageCache::CacheKey key2("def", 0);
    PageCacheHandle handle1;

    {
        // Insert a piece of data, but the application layer does not release it.
        Slice data(new char[1024], 1024);
        ASSERT_OK(_page_cache->insert(key1, data, &handle1, false));
    }

    {
        // Insert another piece of data and release it from the application layer.
        Slice data(new char[1024], 1024);
        PageCacheHandle handle2;
        ASSERT_OK(_page_cache->insert(key2, data, &handle2, false));
    }

    {
        // At this point the cache should have two entries, one for user owner and one for cache owner.
        PageCacheHandle handle3;
        ASSERT_TRUE(_page_cache->lookup(key1, &handle3));
        PageCacheHandle handle4;
        ASSERT_TRUE(_page_cache->lookup(key2, &handle4));

        ASSERT_EQ(_page_cache->get_lookup_count(), 2);
        ASSERT_EQ(_page_cache->get_hit_count(), 2);

        // Test the cache miss
        for (int i = 0; i < 1024; i++) {
            PageCacheHandle handle;
            StoragePageCache::CacheKey key(std::to_string(i), 0);
            ASSERT_FALSE(_page_cache->lookup(key, &handle));
        }

        ASSERT_EQ(_page_cache->get_lookup_count(), 2 + 1024);
        ASSERT_EQ(_page_cache->get_hit_count(), 2);
    }
}

} // namespace starrocks
