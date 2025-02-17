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
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/lru_cache_test.cpp

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

#include "util/lru_cache.h"

#include <gtest/gtest.h>

#include <vector>

using namespace starrocks;
using namespace std;

namespace starrocks {

void PutFixed32(std::string* dst, uint32_t value) {
    char buf[sizeof(value)];
    memcpy(buf, &value, sizeof(value));
    dst->append(buf, sizeof(buf));
}

uint32_t DecodeFixed32(const char* ptr) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result)); // gcc optimizes this to a plain load
    return result;
}

// Conversions between numeric keys/values and the types expected by Cache.
const CacheKey EncodeKey(std::string* result, int k) {
    PutFixed32(result, k);
    return CacheKey(result->c_str(), result->size());
}

static int DecodeKey(const CacheKey& k) {
    assert(k.size() == 4);
    return DecodeFixed32(k.data());
}
static void* EncodeValue(uintptr_t v) {
    return reinterpret_cast<void*>(v);
}
static int DecodeValue(void* v) {
    return reinterpret_cast<uintptr_t>(v);
}

class CacheTest : public testing::Test {
public:
    static CacheTest* _s_current;

    static void Deleter(const CacheKey& key, void* v) {
        _s_current->_deleted_keys.push_back(DecodeKey(key));
        _s_current->_deleted_values.push_back(DecodeValue(v));
    }

    static const int kCacheSize = kNumShards * 1000;
    std::vector<int> _deleted_keys;
    std::vector<int> _deleted_values;
    Cache* _cache;

    CacheTest() : _cache(new_lru_cache(kCacheSize)) { _s_current = this; }

    ~CacheTest() override { delete _cache; }

    int Lookup(int key) {
        std::string result;
        Cache::Handle* handle = _cache->lookup(EncodeKey(&result, key));
        const int r = (handle == nullptr) ? -1 : DecodeValue(_cache->value(handle));

        if (handle != nullptr) {
            _cache->release(handle);
        }

        return r;
    }

    void Insert(int key, int value, int charge) {
        std::string result;
        _cache->release(
                _cache->insert(EncodeKey(&result, key), EncodeValue(value), charge, charge, &CacheTest::Deleter));
    }

    void InsertDurable(int key, int value, int charge) {
        std::string result;
        _cache->release(_cache->insert(EncodeKey(&result, key), EncodeValue(value), charge, charge, &CacheTest::Deleter,
                                       CachePriority::DURABLE));
    }

    void Erase(int key) {
        std::string result;
        _cache->erase(EncodeKey(&result, key));
    }

    void SetUp() override {}

    void TearDown() override {}
};
CacheTest* CacheTest::_s_current;

// Here we declare the variable kCacheSize to avoid undefined reference with google test.
// Please reference https://stackoverflow.com/questions/42756443/undefined-reference-with-gtest
const int CacheTest::kCacheSize;

TEST_F(CacheTest, HitAndMiss) {
    ASSERT_EQ(-1, Lookup(100));

    Insert(100, 101, 1);
    ASSERT_EQ(101, Lookup(100));
    ASSERT_EQ(-1, Lookup(200));
    ASSERT_EQ(-1, Lookup(300));

    Insert(200, 201, 1);
    ASSERT_EQ(101, Lookup(100));
    ASSERT_EQ(201, Lookup(200));
    ASSERT_EQ(-1, Lookup(300));

    Insert(100, 102, 1);
    ASSERT_EQ(102, Lookup(100));
    ASSERT_EQ(201, Lookup(200));
    ASSERT_EQ(-1, Lookup(300));

    ASSERT_EQ(1, _deleted_keys.size());
    ASSERT_EQ(100, _deleted_keys[0]);
    ASSERT_EQ(101, _deleted_values[0]);
}

TEST_F(CacheTest, Erase) {
    Erase(200);
    ASSERT_EQ(0, _deleted_keys.size());

    Insert(100, 101, 1);
    Insert(200, 201, 1);
    Erase(100);
    ASSERT_EQ(-1, Lookup(100));
    ASSERT_EQ(201, Lookup(200));
    ASSERT_EQ(1, _deleted_keys.size());
    ASSERT_EQ(100, _deleted_keys[0]);
    ASSERT_EQ(101, _deleted_values[0]);

    Erase(100);
    ASSERT_EQ(-1, Lookup(100));
    ASSERT_EQ(201, Lookup(200));
    ASSERT_EQ(1, _deleted_keys.size());
}

TEST_F(CacheTest, EntriesArePinned) {
    Insert(100, 101, 1);
    std::string result1;
    Cache::Handle* h1 = _cache->lookup(EncodeKey(&result1, 100));
    ASSERT_EQ(101, DecodeValue(_cache->value(h1)));

    Insert(100, 102, 1);
    std::string result2;
    Cache::Handle* h2 = _cache->lookup(EncodeKey(&result2, 100));
    ASSERT_EQ(102, DecodeValue(_cache->value(h2)));
    ASSERT_EQ(0, _deleted_keys.size());

    _cache->release(h1);
    ASSERT_EQ(1, _deleted_keys.size());
    ASSERT_EQ(100, _deleted_keys[0]);
    ASSERT_EQ(101, _deleted_values[0]);

    Erase(100);
    ASSERT_EQ(-1, Lookup(100));
    ASSERT_EQ(1, _deleted_keys.size());

    _cache->release(h2);
    ASSERT_EQ(2, _deleted_keys.size());
    ASSERT_EQ(100, _deleted_keys[1]);
    ASSERT_EQ(102, _deleted_values[1]);
}

TEST_F(CacheTest, EvictionPolicy) {
    Insert(100, 101, 1);
    Insert(200, 201, 1);

    // Frequently used entry must be kept around
    for (int i = 0; i < kCacheSize + 100; i++) {
        Insert(1000 + i, 2000 + i, 1);
        ASSERT_EQ(2000 + i, Lookup(1000 + i));
        ASSERT_EQ(101, Lookup(100));
    }

    ASSERT_EQ(101, Lookup(100));
    ASSERT_EQ(-1, Lookup(200));
}

TEST_F(CacheTest, EvictionPolicyWithDurable) {
    Insert(100, 101, 1);
    InsertDurable(200, 201, 1);
    Insert(300, 101, 1);

    // Frequently used entry must be kept around
    for (int i = 0; i < kCacheSize + 100; i++) {
        Insert(1000 + i, 2000 + i, 1);
        ASSERT_EQ(2000 + i, Lookup(1000 + i));
        ASSERT_EQ(101, Lookup(100));
    }

    ASSERT_EQ(-1, Lookup(300));
    ASSERT_EQ(101, Lookup(100));
    ASSERT_EQ(201, Lookup(200));
}

static void deleter(const CacheKey& key, void* v) {
    std::cout << "delete key " << key.to_string() << std::endl;
}

static void insert_LRUCache(LRUCache& cache, const CacheKey& key, int value, CachePriority priority) {
    uint32_t hash = key.hash(key.data(), key.size(), 0);
    cache.release(cache.insert(key, hash, EncodeValue(value), value, value, &deleter, priority));
}

TEST_F(CacheTest, Usage) {
    LRUCache cache;
    cache.set_capacity(1000);

    CacheKey key1("100");
    insert_LRUCache(cache, key1, 100, CachePriority::NORMAL);
    ASSERT_EQ(100, cache.get_usage());

    CacheKey key2("200");
    insert_LRUCache(cache, key2, 200, CachePriority::DURABLE);
    ASSERT_EQ(300, cache.get_usage());

    CacheKey key3("300");
    insert_LRUCache(cache, key3, 300, CachePriority::NORMAL);
    ASSERT_EQ(600, cache.get_usage());

    CacheKey key4("400");
    insert_LRUCache(cache, key4, 400, CachePriority::NORMAL);
    ASSERT_EQ(1000, cache.get_usage());

    CacheKey key5("500");
    insert_LRUCache(cache, key5, 500, CachePriority::NORMAL);
    ASSERT_EQ(700, cache.get_usage());

    CacheKey key6("600");
    insert_LRUCache(cache, key6, 600, CachePriority::NORMAL);
    ASSERT_EQ(800, cache.get_usage());

    CacheKey key7("950");
    insert_LRUCache(cache, key7, 950, CachePriority::DURABLE);
    ASSERT_EQ(950, cache.get_usage());
}

TEST_F(CacheTest, HeavyEntries) {
    // Add a bunch of light and heavy entries and then count the combined
    // size of items still in the cache, which must be approximately the
    // same as the total capacity.
    const int kLight = 1;
    const int kHeavy = 10;
    int added = 0;
    int index = 0;

    while (added < 2 * kCacheSize) {
        const int weight = (index & 1) ? kLight : kHeavy;
        Insert(index, 1000 + index, weight);
        added += weight;
        index++;
    }

    int cached_weight = 0;

    for (int i = 0; i < index; i++) {
        const int weight = (i & 1 ? kLight : kHeavy);
        int r = Lookup(i);

        if (r >= 0) {
            cached_weight += weight;
            ASSERT_EQ(1000 + i, r);
        }
    }

    ASSERT_LE(cached_weight, kCacheSize + kCacheSize / 10);
}

TEST_F(CacheTest, NewId) {
    uint64_t a = _cache->new_id();
    uint64_t b = _cache->new_id();
    ASSERT_NE(a, b);
}

TEST_F(CacheTest, SetCapacity) {
    // Test1: increase capacity
    // Lets insert 32 elements, then increase capacity to 2*kCacheSize,
    // returned capacity should be 2*kCacheSize, usage=32
    std::vector<Cache::Handle*> handles(64, nullptr);
    // Insert kCacheSize entries, but not releasing.
    for (int i = 0; i < 32; i++) {
        std::string result;
        handles[i] = _cache->insert(EncodeKey(&result, i), EncodeValue(1000 + kCacheSize), 1, 1, &CacheTest::Deleter);
    }
    ASSERT_EQ(kCacheSize, _cache->get_capacity());
    ASSERT_EQ(32, _cache->get_memory_usage());
    _cache->set_capacity(kCacheSize * 2);
    ASSERT_EQ(kCacheSize * 2, _cache->get_capacity());
    ASSERT_EQ(32, _cache->get_memory_usage());

    // Test2: decrease capacity
    // insert more elements to cache, then release 32,
    // then decrease capacity to 32, final capacity should be 32.
    // then release 32, usage should be 32.
    for (int i = 32; i < 64; i++) {
        std::string result;
        handles[i] = _cache->insert(EncodeKey(&result, i), EncodeValue(1000 + kCacheSize), 1, 1, &CacheTest::Deleter);
    }
    ASSERT_EQ(kCacheSize * 2, _cache->get_capacity());
    ASSERT_EQ(64, _cache->get_memory_usage());
    for (int i = 0; i < 32; i++) {
        _cache->release(handles[i]);
    }
    ASSERT_EQ(kCacheSize * 2, _cache->get_capacity());
    ASSERT_EQ(64, _cache->get_memory_usage());
    _cache->set_capacity(32);
    ASSERT_EQ(32, _cache->get_capacity());
    for (int i = 32; i < 64; i++) {
        _cache->release(handles[i]);
    }
    ASSERT_EQ(32, _cache->get_memory_usage());
}

} // namespace starrocks
