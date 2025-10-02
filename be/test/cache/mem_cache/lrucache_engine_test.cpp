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

#include "cache/mem_cache/lrucache_engine.h"

#include <gtest/gtest.h>

#include "testutil/assert.h"

namespace starrocks {
class LRUCacheEngineTest : public ::testing::Test {
protected:
    void SetUp() override;
    void TearDown() override {}

    static void Deleter(const CacheKey& k, void* v) { free(v); }
    void _insert_data();
    void _check_not_found(int value);
    void _check_found(int value);

    static std::string _int_to_string(size_t length, int num) {
        std::ostringstream oss;
        oss << std::setw(length) << std::setfill('0') << num;
        return oss.str();
    }

    std::shared_ptr<LRUCacheEngine> _cache;
    MemCacheWriteOptions _write_opt;
    MemCacheReadOptions _read_opt;

    size_t _capacity = 16384;
    size_t _key_size = sizeof(LRUHandle) - 1 + 6;
    size_t _value_size = 4;
    size_t _kv_size = _key_size + _value_size;
};

void LRUCacheEngineTest::_insert_data() {
    // insert
    for (int i = 0; i < 128; i++) {
        std::string key = _int_to_string(6, i);
        int* ptr = (int*)malloc(_value_size);
        *ptr = i;

        MemCacheHandlePtr handle = nullptr;
        ASSERT_OK(_cache->insert(key, (void*)ptr, _value_size, &Deleter, &handle, _write_opt));
        _cache->release(handle);
    }
}

void LRUCacheEngineTest::_check_not_found(int value) {
    std::string key = _int_to_string(6, value);
    Status st = _cache->lookup(key, nullptr, &_read_opt);
    ASSERT_TRUE(st.is_not_found());
}

void LRUCacheEngineTest::_check_found(int value) {
    std::string key = _int_to_string(6, value);
    MemCacheHandlePtr handle;
    ASSERT_OK(_cache->lookup(key, &handle, &_read_opt));
    ASSERT_EQ(*(int*)(_cache->value(handle)), value);
    _cache->release(handle);
}

void LRUCacheEngineTest::SetUp() {
    _cache = std::make_shared<LRUCacheEngine>();
    MemCacheOptions opts{.mem_space_size = _capacity};
    ASSERT_OK(_cache->init(opts));
}

TEST_F(LRUCacheEngineTest, test_init) {
    ASSERT_EQ(_cache->mem_quota(), _capacity);
}

TEST_F(LRUCacheEngineTest, test_insert_lookup) {
    // insert
    _insert_data();

    // usage
    ASSERT_EQ(_cache->mem_usage(), _kv_size * 128);

    // not found
    _check_not_found(254);

    // found
    _check_found(30);

    // remove
    ASSERT_OK(_cache->remove(_int_to_string(6, 30)));

    // not found
    _check_not_found(30);
}

TEST_F(LRUCacheEngineTest, test_value) {
    _insert_data();

    std::string key = _int_to_string(6, 30);
    MemCacheHandlePtr handle = nullptr;
    ASSERT_OK(_cache->lookup(key, &handle, &_read_opt));

    const void* value = _cache->value(handle);
    ASSERT_EQ(*(const int*)value, 30);
    _cache->release(handle);
}

TEST_F(LRUCacheEngineTest, test_set_capacity) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->mem_quota(), _capacity);
    ASSERT_EQ(_cache->mem_usage(), _kv_size * 128);

    ASSERT_OK(_cache->update_mem_quota(8192, false));
    ASSERT_EQ(_cache->mem_quota(), 8192);
    ASSERT_GT(_cache->mem_usage(), 0);
    ASSERT_LT(_cache->mem_usage(), 8192);

    // not found
    _check_not_found(0);

    // found
    _check_found(127);
}

TEST_F(LRUCacheEngineTest, test_adjust_capacity) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->mem_quota(), _capacity);
    ASSERT_EQ(_cache->mem_usage(), _kv_size * 128);

    ASSERT_OK(_cache->adjust_mem_quota(-8192, 1024));
    ASSERT_EQ(_cache->mem_quota(), 8192);
    ASSERT_GT(_cache->mem_usage(), 0);
    ASSERT_LT(_cache->mem_usage(), 8192);

    // not found
    _check_not_found(0);

    // found
    _check_found(127);
}

TEST_F(LRUCacheEngineTest, test_prune) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->mem_usage(), _kv_size * 128);
    ASSERT_OK(_cache->prune());
    ASSERT_EQ(_cache->mem_usage(), 0);
}

TEST_F(LRUCacheEngineTest, test_shutdown) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->mem_usage(), _kv_size * 128);
    ASSERT_OK(_cache->shutdown());
    ASSERT_EQ(_cache->mem_usage(), 0);
}
} // namespace starrocks
