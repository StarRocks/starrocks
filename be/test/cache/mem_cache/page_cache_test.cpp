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

#include "cache/mem_cache/page_cache.h"

#include <gtest/gtest.h>

#include "cache/mem_cache/lrucache_engine.h"
#include "testutil/assert.h"

namespace starrocks {

class StoragePageCacheTest : public testing::Test {
protected:
    StoragePageCacheTest() = default;
    ~StoragePageCacheTest() override = default;

    void SetUp() override;

    std::shared_ptr<LRUCacheEngine> _lru_cache;
    std::shared_ptr<StoragePageCache> _page_cache;
    size_t _capacity = kNumShards * 2048;
};

void StoragePageCacheTest::SetUp() {
    MemCacheOptions opts{.mem_space_size = _capacity};
    _lru_cache = std::make_shared<LRUCacheEngine>();
    ASSERT_OK(_lru_cache->init(opts));
    _page_cache = std::make_shared<StoragePageCache>(_lru_cache.get());
}

TEST_F(StoragePageCacheTest, insert_with_deleter) {
    struct Value {
        Value(std::string v) : value(std::move(v)) {}
        std::string value;
    };
    auto deleter = [](const starrocks::CacheKey& key, void* value) { delete (Value*)value; };

    {
        std::string key("123");
        auto* value = new Value("value_of_123");
        PageCacheHandle handle;
        MemCacheWriteOptions opts;

        ASSERT_OK(_page_cache->insert(key, (void*)value, 15, deleter, opts, &handle));
        Value* handle_value = (Value*)handle.data();
        ASSERT_EQ(handle_value->value, "value_of_123");
    }
    {
        std::string key("123");
        PageCacheHandle handle;

        ASSERT_TRUE(_page_cache->lookup(key, &handle));
        Value* handle_value = (Value*)handle.data();
        ASSERT_EQ(handle_value->value, "value_of_123");
    }
}

// NOLINTNEXTLINE
TEST_F(StoragePageCacheTest, normal) {
    {
        // insert normal page
        std::string key("abc0");
        auto data = std::make_unique<std::vector<uint8_t>>(1024);
        PageCacheHandle handle;

        MemCacheWriteOptions opts;
        ASSERT_OK(_page_cache->insert(key, data.get(), opts, &handle));
        ASSERT_EQ(handle.data(), data.get());
        auto* check_data = data.release();

        ASSERT_TRUE(_page_cache->lookup(key, &handle));
        ASSERT_EQ(handle.data(), check_data);
    }

    {
        // insert in_memory page
        std::string key("mem0");
        auto data = std::make_unique<std::vector<uint8_t>>(1024);
        PageCacheHandle handle;

        MemCacheWriteOptions opts{.priority = true};
        ASSERT_OK(_page_cache->insert(key, data.get(), opts, &handle));
        ASSERT_EQ(handle.data(), data.get());
        data.release();

        ASSERT_TRUE(_page_cache->lookup(key, &handle));
    }

    // put too many page to eliminate first page
    for (int i = 0; i < 10 * kNumShards; ++i) {
        std::string key("bcd");
        key.append(std::to_string(i));
        auto data = std::make_unique<std::vector<uint8_t>>(1024);
        PageCacheHandle handle;
        MemCacheWriteOptions opts;
        ASSERT_OK(_page_cache->insert(key, data.get(), opts, &handle));
        data.release();
    }

    // cache miss
    {
        PageCacheHandle handle;
        std::string miss_key("abc1");
        ASSERT_FALSE(_page_cache->lookup(miss_key, &handle));
    }

    // cache miss for eliminated key
    {
        std::string key("abc0");
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

    // set capacity = 0
    {
        _page_cache->set_capacity(0);
        ASSERT_EQ(_page_cache->get_capacity(), 0);
    }
}

TEST_F(StoragePageCacheTest, metrics) {
    std::string key1("abc0");
    std::string key2("def0");
    PageCacheHandle handle1;

    {
        // Insert a piece of data, but the application layer does not release it.
        auto data = std::make_unique<std::vector<uint8_t>>(1024);
        MemCacheWriteOptions opts;
        ASSERT_OK(_page_cache->insert(key1, data.get(), opts, &handle1));
        data.release();
    }

    {
        // Insert another piece of data and release it from the application layer.
        auto data = std::make_unique<std::vector<uint8_t>>(1024);
        PageCacheHandle handle2;
        MemCacheWriteOptions opts;
        ASSERT_OK(_page_cache->insert(key2, data.get(), opts, &handle2));
        data.release();
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
            std::string key(std::to_string(i));
            key.append("0");
            ASSERT_FALSE(_page_cache->lookup(key, &handle));
        }

        ASSERT_EQ(_page_cache->get_lookup_count(), 2 + 1024);
        ASSERT_EQ(_page_cache->get_hit_count(), 2);
    }
}

} // namespace starrocks