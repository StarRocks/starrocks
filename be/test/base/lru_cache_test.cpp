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

#include "base/container/lru_cache.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <memory>
#include <thread>
#include <unordered_map>
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
        _cache->release(_cache->insert(EncodeKey(&result, key), EncodeValue(value), charge, &CacheTest::Deleter));
    }

    void InsertDurable(int key, int value, int charge) {
        std::string result;
        _cache->release(_cache->insert(EncodeKey(&result, key), EncodeValue(value), charge, &CacheTest::Deleter,
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

static uint32_t hash_cache_key(const CacheKey& key) {
    return key.hash(key.data(), key.size(), 0);
}

static int lookup_cache(Cache* cache, int key) {
    std::string encoded;
    Cache::Handle* handle = cache->lookup(EncodeKey(&encoded, key));
    const int result = (handle == nullptr) ? -1 : DecodeValue(cache->value(handle));
    if (handle != nullptr) {
        cache->release(handle);
    }
    return result;
}

static void insert_cache(Cache* cache, int key, int value, size_t charge,
                         CachePriority priority = CachePriority::NORMAL) {
    std::string encoded;
    cache->release(cache->insert(EncodeKey(&encoded, key), EncodeValue(value), charge, &CacheTest::Deleter, priority));
}

static void touch_cache(Cache* cache, int key) {
    std::string encoded;
    cache->touch(EncodeKey(&encoded, key));
}

static uint32_t shard_for_int_key(int key) {
    std::string encoded;
    return hash_cache_key(EncodeKey(&encoded, key)) >> (32 - kNumShardBits);
}

static std::array<int, 3> find_int_keys_in_same_shard() {
    std::unordered_map<uint32_t, std::vector<int>> shard_keys;
    for (int key = 0;; key++) {
        auto& keys = shard_keys[shard_for_int_key(key)];
        keys.push_back(key);
        if (keys.size() == 3) {
            return {keys[0], keys[1], keys[2]};
        }
    }
}

static void insert_LRUCache(LRUCache& cache, const CacheKey& key, int value, size_t charge,
                            CachePriority priority = CachePriority::NORMAL) {
    cache.release(cache.insert(key, hash_cache_key(key), EncodeValue(value), charge, &deleter, priority));
}

static int decode_LRUCache_value(Cache::Handle* handle) {
    return DecodeValue(reinterpret_cast<LRUHandle*>(handle)->value);
}

static int lookup_LRUCache(LRUCache& cache, const CacheKey& key) {
    Cache::Handle* handle = cache.lookup(key, hash_cache_key(key));
    const int result = (handle == nullptr) ? -1 : decode_LRUCache_value(handle);
    if (handle != nullptr) {
        cache.release(handle);
    }
    return result;
}

static void touch_LRUCache(LRUCache& cache, const CacheKey& key) {
    cache.touch(key, hash_cache_key(key));
}

static size_t entry_charge_for_int_key() {
    std::string encoded;
    CacheKey key = EncodeKey(&encoded, 0);
    return 1 + LRUCache::key_handle_size(key);
}

TEST_F(CacheTest, Usage) {
    LRUCache cache;
    cache.set_capacity(1000);

    CacheKey key1("100");
    size_t key_mem_usage = sizeof(LRUHandle) - 1 + key1.size();
    insert_LRUCache(cache, key1, 100, 100, CachePriority::NORMAL);
    // 100 + 90
    ASSERT_EQ(100 + key_mem_usage, cache.get_usage());

    CacheKey key2("200");
    insert_LRUCache(cache, key2, 200, 200, CachePriority::DURABLE);
    // 300 + 180
    ASSERT_EQ(300 + key_mem_usage * 2, cache.get_usage());

    CacheKey key3("300");
    insert_LRUCache(cache, key3, 300, 300, CachePriority::NORMAL);
    // 600 + 270
    ASSERT_EQ(600 + key_mem_usage * 3, cache.get_usage());

    CacheKey key4("400");
    insert_LRUCache(cache, key4, 400, 400, CachePriority::NORMAL);
    // 600 + 180
    ASSERT_EQ(600 + key_mem_usage * 2, cache.get_usage());

    CacheKey key5("500");
    insert_LRUCache(cache, key5, 500, 500, CachePriority::NORMAL);
    // 700 + 180
    ASSERT_EQ(700 + key_mem_usage * 2, cache.get_usage());

    CacheKey key6("600");
    insert_LRUCache(cache, key6, 600, 600, CachePriority::NORMAL);
    // 800 + 180
    ASSERT_EQ(800 + key_mem_usage * 2, cache.get_usage());

    CacheKey key7("950");
    // 900 + 90
    insert_LRUCache(cache, key7, 900, 900, CachePriority::DURABLE);
    ASSERT_EQ(900 + key_mem_usage, cache.get_usage());
}

TEST_F(CacheTest, TouchUpdatesRecencyWithoutLookupSideEffects) {
    const size_t entry_charge = entry_charge_for_int_key();

    {
        LRUCache cache;
        cache.set_capacity(entry_charge * 2);

        std::string k100_buf;
        std::string k200_buf;
        std::string k300_buf;
        CacheKey k100 = EncodeKey(&k100_buf, 100);
        CacheKey k200 = EncodeKey(&k200_buf, 200);
        CacheKey k300 = EncodeKey(&k300_buf, 300);

        insert_LRUCache(cache, k100, 101, 1);
        insert_LRUCache(cache, k200, 201, 1);
        insert_LRUCache(cache, k300, 301, 1);

        ASSERT_EQ(-1, lookup_LRUCache(cache, k100));
        ASSERT_EQ(201, lookup_LRUCache(cache, k200));
        ASSERT_EQ(301, lookup_LRUCache(cache, k300));
    }

    {
        LRUCache cache;
        cache.set_capacity(entry_charge * 2);

        std::string k100_buf;
        std::string k200_buf;
        std::string k300_buf;
        CacheKey k100 = EncodeKey(&k100_buf, 100);
        CacheKey k200 = EncodeKey(&k200_buf, 200);
        CacheKey k300 = EncodeKey(&k300_buf, 300);

        insert_LRUCache(cache, k100, 101, 1);
        insert_LRUCache(cache, k200, 201, 1);
        const uint64_t lookup_count_before_touch = cache.get_lookup_count();
        const uint64_t hit_count_before_touch = cache.get_hit_count();
        touch_LRUCache(cache, k100);
        ASSERT_EQ(lookup_count_before_touch, cache.get_lookup_count());
        ASSERT_EQ(hit_count_before_touch, cache.get_hit_count());
        insert_LRUCache(cache, k300, 301, 1);

        ASSERT_EQ(101, lookup_LRUCache(cache, k100));
        ASSERT_EQ(-1, lookup_LRUCache(cache, k200));
        ASSERT_EQ(301, lookup_LRUCache(cache, k300));
    }
}

TEST_F(CacheTest, TouchUpdatesRecencyOnShardedCache) {
    const size_t entry_charge = entry_charge_for_int_key();
    const auto keys = find_int_keys_in_same_shard();
    std::unique_ptr<Cache> cache(new_lru_cache(entry_charge * 2 * kNumShards));

    insert_cache(cache.get(), keys[0], 1000, 1);
    insert_cache(cache.get(), keys[1], 2000, 1);
    touch_cache(cache.get(), keys[0]);
    insert_cache(cache.get(), keys[2], 3000, 1);

    ASSERT_EQ(1000, lookup_cache(cache.get(), keys[0]));
    ASSERT_EQ(-1, lookup_cache(cache.get(), keys[1]));
    ASSERT_EQ(3000, lookup_cache(cache.get(), keys[2]));
}

static Cache::Handle* insert_if_absent_cache(Cache* cache, int key, int value, size_t charge, bool* inserted) {
    std::string encoded;
    return cache->insert_if_absent(EncodeKey(&encoded, key), EncodeValue(value), charge, &CacheTest::Deleter, inserted);
}

static bool update_charge_cache(Cache* cache, int key, size_t new_value_size,
                                bool (*pred)(void* value, const void* ctx) = nullptr, const void* ctx = nullptr) {
    std::string encoded;
    return cache->update_charge_if(EncodeKey(&encoded, key), new_value_size, pred, ctx);
}

static bool value_equals(void* value, const void* ctx) {
    return DecodeValue(value) == static_cast<int>(reinterpret_cast<intptr_t>(ctx));
}

TEST_F(CacheTest, InsertIfAbsentInsertsWhenKeyMissing) {
    std::unique_ptr<Cache> cache(new_lru_cache(entry_charge_for_int_key() * kNumShards));

    bool inserted = false;
    auto* handle = insert_if_absent_cache(cache.get(), 100, 1000, 1, &inserted);
    ASSERT_NE(nullptr, handle);
    ASSERT_TRUE(inserted);
    ASSERT_EQ(1000, DecodeValue(cache->value(handle)));
    cache->release(handle);

    ASSERT_EQ(1000, lookup_cache(cache.get(), 100));
    ASSERT_TRUE(_deleted_keys.empty());
}

TEST_F(CacheTest, InsertIfAbsentKeepsExistingEntry) {
    std::unique_ptr<Cache> cache(new_lru_cache(entry_charge_for_int_key() * 2 * kNumShards));

    insert_cache(cache.get(), 100, 1000, 1);

    bool inserted = true;
    auto* handle = insert_if_absent_cache(cache.get(), 100, 2000, 1, &inserted);
    ASSERT_NE(nullptr, handle);
    ASSERT_FALSE(inserted);
    // The existing value is returned; the rejected one is never handed to the deleter,
    // it stays the caller's responsibility.
    ASSERT_EQ(1000, DecodeValue(cache->value(handle)));
    cache->release(handle);

    ASSERT_EQ(1000, lookup_cache(cache.get(), 100));
    ASSERT_TRUE(_deleted_keys.empty());
}

TEST_F(CacheTest, InsertIfAbsentEvictsToStayWithinCapacity) {
    const size_t entry_charge = entry_charge_for_int_key();
    const auto keys = find_int_keys_in_same_shard();
    std::unique_ptr<Cache> cache(new_lru_cache(entry_charge * 2 * kNumShards));

    insert_cache(cache.get(), keys[0], 1000, 1);
    insert_cache(cache.get(), keys[1], 2000, 1);

    bool inserted = false;
    cache->release(insert_if_absent_cache(cache.get(), keys[2], 3000, 1, &inserted));
    ASSERT_TRUE(inserted);

    ASSERT_EQ(-1, lookup_cache(cache.get(), keys[0]));
    ASSERT_EQ(2000, lookup_cache(cache.get(), keys[1]));
    ASSERT_EQ(3000, lookup_cache(cache.get(), keys[2]));
    ASSERT_EQ(std::vector<int>({keys[0]}), _deleted_keys);
}

TEST_F(CacheTest, InsertIfAbsentIsAtomicUnderConcurrency) {
    constexpr int kThreads = 16;
    std::unique_ptr<Cache> cache(new_lru_cache(entry_charge_for_int_key() * 64 * kNumShards));

    std::atomic<int> inserted_count{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; i++) {
        threads.emplace_back([&, i]() {
            bool inserted = false;
            auto* handle = insert_if_absent_cache(cache.get(), 100, 1000 + i, 1, &inserted);
            if (inserted) {
                inserted_count.fetch_add(1);
            }
            cache->release(handle);
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    ASSERT_EQ(1, inserted_count.load());
    ASSERT_TRUE(_deleted_keys.empty());
}

TEST_F(CacheTest, UpdateChargeIfAdjustsUsage) {
    const size_t entry_charge = entry_charge_for_int_key();
    std::unique_ptr<Cache> cache(new_lru_cache(entry_charge * 16 * kNumShards));

    insert_cache(cache.get(), 100, 1000, 1);
    const size_t base_usage = cache->get_memory_usage();

    ASSERT_TRUE(update_charge_cache(cache.get(), 100, 101));
    ASSERT_EQ(base_usage + 100, cache->get_memory_usage());

    ASSERT_TRUE(update_charge_cache(cache.get(), 100, 1));
    ASSERT_EQ(base_usage, cache->get_memory_usage());

    // The entry itself is untouched, only its accounted size changed.
    ASSERT_EQ(1000, lookup_cache(cache.get(), 100));
    ASSERT_TRUE(_deleted_keys.empty());
}

TEST_F(CacheTest, UpdateChargeIfIgnoresMissingKeyAndRejectedPredicate) {
    const size_t entry_charge = entry_charge_for_int_key();
    std::unique_ptr<Cache> cache(new_lru_cache(entry_charge * 16 * kNumShards));

    insert_cache(cache.get(), 100, 1000, 1);
    const size_t base_usage = cache->get_memory_usage();

    ASSERT_FALSE(update_charge_cache(cache.get(), 200, 101));
    ASSERT_EQ(base_usage, cache->get_memory_usage());

    ASSERT_FALSE(update_charge_cache(cache.get(), 100, 101, value_equals,
                                     reinterpret_cast<const void*>(static_cast<intptr_t>(2000))));
    ASSERT_EQ(base_usage, cache->get_memory_usage());

    ASSERT_TRUE(update_charge_cache(cache.get(), 100, 101, value_equals,
                                    reinterpret_cast<const void*>(static_cast<intptr_t>(1000))));
    ASSERT_EQ(base_usage + 100, cache->get_memory_usage());
}

TEST_F(CacheTest, UpdateChargeIfRefreshesRecencyBeforeEvicting) {
    const size_t entry_charge = entry_charge_for_int_key();
    const auto keys = find_int_keys_in_same_shard();
    std::unique_ptr<Cache> cache(new_lru_cache(entry_charge * 2 * kNumShards));

    insert_cache(cache.get(), keys[0], 1000, 1);
    insert_cache(cache.get(), keys[1], 2000, 1);

    // Grow the *older* entry past the shard capacity. Because the update refreshes it to
    // the MRU end first, the newer entry is the one evicted, not the entry being updated.
    ASSERT_TRUE(update_charge_cache(cache.get(), keys[0], 1 + entry_charge));

    ASSERT_EQ(1000, lookup_cache(cache.get(), keys[0]));
    ASSERT_EQ(-1, lookup_cache(cache.get(), keys[1]));
    ASSERT_EQ(std::vector<int>({keys[1]}), _deleted_keys);
}

TEST_F(CacheTest, UpdateChargeByHandleAdjustsUsageAndEvicts) {
    const size_t entry_charge = entry_charge_for_int_key();
    const auto keys = find_int_keys_in_same_shard();
    std::unique_ptr<Cache> cache(new_lru_cache(entry_charge * 2 * kNumShards));

    insert_cache(cache.get(), keys[0], 1000, 1);
    insert_cache(cache.get(), keys[1], 2000, 1);

    std::string encoded;
    Cache::Handle* handle = cache->lookup(EncodeKey(&encoded, keys[0]));
    ASSERT_NE(nullptr, handle);

    // The updated entry is pinned by |handle|, so growing it to the shard
    // capacity evicts the other unpinned entry in the same shard.
    ASSERT_TRUE(cache->update_charge(handle, 1 + entry_charge));
    ASSERT_EQ(entry_charge * 2, cache->get_memory_usage());
    ASSERT_EQ(std::vector<int>({keys[1]}), _deleted_keys);
    ASSERT_EQ(1000, DecodeValue(cache->value(handle)));

    // Shrinking through the same handle updates the charge without replacing
    // the cached value.
    ASSERT_TRUE(cache->update_charge(handle, 1));
    ASSERT_EQ(entry_charge, cache->get_memory_usage());
    cache->release(handle);

    ASSERT_EQ(1000, lookup_cache(cache.get(), keys[0]));
    ASSERT_EQ(-1, lookup_cache(cache.get(), keys[1]));
}

TEST_F(CacheTest, UpdateChargeByHandleRejectsNullAndErasedEntry) {
    const size_t entry_charge = entry_charge_for_int_key();
    std::unique_ptr<Cache> cache(new_lru_cache(entry_charge * kNumShards));

    ASSERT_FALSE(cache->update_charge(nullptr, 1));

    std::string encoded;
    CacheKey key = EncodeKey(&encoded, 100);
    Cache::Handle* handle = cache->insert(key, EncodeValue(1000), 1, &CacheTest::Deleter);
    ASSERT_NE(nullptr, handle);
    ASSERT_EQ(entry_charge, cache->get_memory_usage());

    // erase() drops the cache's reference, but |handle| keeps the entry alive.
    // Updating that detached entry must be rejected and leave its charge intact
    // until the caller releases the handle.
    cache->erase(key);
    ASSERT_FALSE(cache->update_charge(handle, 101));
    ASSERT_EQ(entry_charge, cache->get_memory_usage());
    cache->release(handle);

    ASSERT_EQ(0, cache->get_memory_usage());
    ASSERT_EQ(std::vector<int>({100}), _deleted_keys);
}

TEST_F(CacheTest, TouchMissingKeyDoesNotAffectRecencyOrStats) {
    const size_t entry_charge = entry_charge_for_int_key();
    LRUCache cache;
    cache.set_capacity(entry_charge * 2);

    std::string k100_buf;
    std::string k200_buf;
    std::string k300_buf;
    std::string k999_buf;
    CacheKey k100 = EncodeKey(&k100_buf, 100);
    CacheKey k200 = EncodeKey(&k200_buf, 200);
    CacheKey k300 = EncodeKey(&k300_buf, 300);
    CacheKey k999 = EncodeKey(&k999_buf, 999);

    insert_LRUCache(cache, k100, 101, 1);
    insert_LRUCache(cache, k200, 201, 1);

    const uint64_t lookup_count_before = cache.get_lookup_count();
    const uint64_t hit_count_before = cache.get_hit_count();
    touch_LRUCache(cache, k999);

    ASSERT_EQ(lookup_count_before, cache.get_lookup_count());
    ASSERT_EQ(hit_count_before, cache.get_hit_count());

    insert_LRUCache(cache, k300, 301, 1);

    ASSERT_EQ(-1, lookup_LRUCache(cache, k100));
    ASSERT_EQ(201, lookup_LRUCache(cache, k200));
    ASSERT_EQ(301, lookup_LRUCache(cache, k300));
}

TEST_F(CacheTest, TouchPinnedEntryIsNoOpAndDoesNotChangeStats) {
    const size_t entry_charge = entry_charge_for_int_key();
    LRUCache cache;
    cache.set_capacity(entry_charge * 2);

    std::string k100_buf;
    std::string k200_buf;
    std::string k300_buf;
    CacheKey k100 = EncodeKey(&k100_buf, 100);
    CacheKey k200 = EncodeKey(&k200_buf, 200);
    CacheKey k300 = EncodeKey(&k300_buf, 300);

    insert_LRUCache(cache, k100, 101, 1);
    insert_LRUCache(cache, k200, 201, 1);

    Cache::Handle* pinned = cache.lookup(k100, hash_cache_key(k100));
    ASSERT_NE(nullptr, pinned);
    ASSERT_EQ(101, decode_LRUCache_value(pinned));

    const uint64_t lookup_count_after_pin = cache.get_lookup_count();
    const uint64_t hit_count_after_pin = cache.get_hit_count();
    touch_LRUCache(cache, k100);

    ASSERT_EQ(lookup_count_after_pin, cache.get_lookup_count());
    ASSERT_EQ(hit_count_after_pin, cache.get_hit_count());

    insert_LRUCache(cache, k300, 301, 1);

    ASSERT_EQ(-1, lookup_LRUCache(cache, k200));
    ASSERT_EQ(301, lookup_LRUCache(cache, k300));
    ASSERT_EQ(101, decode_LRUCache_value(pinned));

    cache.release(pinned);
    ASSERT_EQ(101, lookup_LRUCache(cache, k100));
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
    size_t key_mem_size_1 = 0;
    for (int i = 0; i < 32; i++) {
        std::string result;
        auto cache_key = EncodeKey(&result, i);
        key_mem_size_1 += sizeof(LRUHandle) - 1 + cache_key.size();
        handles[i] = _cache->insert(cache_key, EncodeValue(1000 + kCacheSize), 1, &CacheTest::Deleter);
    }
    ASSERT_EQ(kCacheSize, _cache->get_capacity());
    ASSERT_EQ(32 + key_mem_size_1, _cache->get_memory_usage());
    _cache->set_capacity(kCacheSize * 2);
    ASSERT_EQ(kCacheSize * 2, _cache->get_capacity());
    ASSERT_EQ(32 + key_mem_size_1, _cache->get_memory_usage());

    // Test2: decrease capacity
    // insert more elements to cache, then release 32,
    // then decrease capacity to 32, final capacity should be 32.
    // then release 32, usage should be 32.
    size_t key_mem_size_2 = 0;
    for (int i = 32; i < 64; i++) {
        std::string result;
        auto cache_key = EncodeKey(&result, i);
        key_mem_size_2 += sizeof(LRUHandle) - 1 + cache_key.size();
        handles[i] = _cache->insert(cache_key, EncodeValue(1000 + kCacheSize), 1, &CacheTest::Deleter);
    }
    ASSERT_EQ(kCacheSize * 2, _cache->get_capacity());
    ASSERT_EQ(64 + key_mem_size_1 + key_mem_size_2, _cache->get_memory_usage());
    for (int i = 0; i < 32; i++) {
        _cache->release(handles[i]);
    }
    ASSERT_EQ(kCacheSize * 2, _cache->get_capacity());
    ASSERT_EQ(64 + key_mem_size_1 + key_mem_size_2, _cache->get_memory_usage());
    _cache->set_capacity(32 + key_mem_size_2);
    ASSERT_EQ(32 + key_mem_size_2, _cache->get_capacity());
    for (int i = 32; i < 64; i++) {
        _cache->release(handles[i]);
    }
    ASSERT_EQ(32 + key_mem_size_2, _cache->get_memory_usage());
}

} // namespace starrocks
