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

#include "cache/object_cache/lrucache_module.h"

#include <gtest/gtest.h>

#include "testutil/assert.h"

namespace starrocks {
class LRUCacheModuleTest : public ::testing::Test {
protected:
    void SetUp() override { _cache = std::make_shared<LRUCacheModule>(_cache_opt); }
    void TearDown() override {}

protected:
    static void Deleter(const CacheKey& k, void* v) { free(v); }
    void _insert_data();
    void _check_not_found(int value);
    void _check_found(int value);

    static std::string int_to_string(size_t length, int num) {
        std::ostringstream oss;
        oss << std::setw(length) << std::setfill('0') << num;
        return oss.str();
    }

    std::shared_ptr<LRUCacheModule> _cache;
    ObjectCacheOptions _cache_opt{.capacity = 16384};
    ObjectCacheWriteOptions _write_opt;
    ObjectCacheReadOptions _read_opt;

    size_t _key_size = sizeof(LRUHandle) - 1 + 6;
    size_t _value_size = 4;
    size_t _kv_size = _key_size + _value_size;
};

void LRUCacheModuleTest::_insert_data() {
    // insert
    for (int i = 0; i < 128; i++) {
        std::string key = int_to_string(6, i);
        int* ptr = (int*)malloc(_value_size);
        *ptr = i;

        ObjectCacheHandlePtr handle = nullptr;
        ASSERT_OK(_cache->insert(key, (void*)ptr, _value_size, _value_size, &Deleter, &handle, &_write_opt));
        _cache->release(handle);
    }
}

void LRUCacheModuleTest::_check_not_found(int value) {
    std::string key = int_to_string(6, value);
    Status st = _cache->lookup(key, nullptr, &_read_opt);
    ASSERT_TRUE(st.is_not_found());
}

void LRUCacheModuleTest::_check_found(int value) {
    std::string key = int_to_string(6, value);
    ObjectCacheHandlePtr handle;
    ASSERT_OK(_cache->lookup(key, &handle, &_read_opt));
    ASSERT_EQ(*(int*)(_cache->value(handle)), value);
    _cache->release(handle);
}

TEST_F(LRUCacheModuleTest, test_init) {
    ASSERT_EQ(_cache->capacity(), _cache_opt.capacity);
}

TEST_F(LRUCacheModuleTest, test_insert_lookup) {
    // insert
    _insert_data();

    // usage
    ASSERT_EQ(_cache->usage(), _kv_size * 128);

    // not found
    _check_not_found(254);

    // found
    _check_found(30);

    // remove
    ASSERT_OK(_cache->remove(int_to_string(6, 30)));

    // not found
    _check_not_found(30);
}

TEST_F(LRUCacheModuleTest, test_value_slice) {
    _insert_data();

    std::string key = int_to_string(6, 30);
    ObjectCacheHandlePtr handle = nullptr;
    ASSERT_OK(_cache->lookup(key, &handle, &_read_opt));

    Slice value = _cache->value_slice(handle);
    ASSERT_EQ(value.size, _value_size);
    ASSERT_EQ(*(int*)value.data, 30);
    _cache->release(handle);
}

TEST_F(LRUCacheModuleTest, test_set_capacity) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->capacity(), _cache_opt.capacity);
    ASSERT_EQ(_cache->usage(), _kv_size * 128);

    ASSERT_OK(_cache->set_capacity(8192));
    ASSERT_EQ(_cache->capacity(), 8192);
    ASSERT_GT(_cache->usage(), 0);
    ASSERT_LT(_cache->usage(), 8192);

    // not found
    _check_not_found(0);

    // found
    _check_found(127);
}

TEST_F(LRUCacheModuleTest, test_adjust_capacity) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->capacity(), _cache_opt.capacity);
    ASSERT_EQ(_cache->usage(), _kv_size * 128);

    ASSERT_OK(_cache->adjust_capacity(-8192, 1024));
    ASSERT_EQ(_cache->capacity(), 8192);
    ASSERT_GT(_cache->usage(), 0);
    ASSERT_LT(_cache->usage(), 8192);

    // not found
    _check_not_found(0);

    // found
    _check_found(127);
}

TEST_F(LRUCacheModuleTest, test_prune) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->usage(), _kv_size * 128);
    ASSERT_OK(_cache->prune());
    ASSERT_EQ(_cache->usage(), 0);
}

TEST_F(LRUCacheModuleTest, test_shutdown) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->usage(), _kv_size * 128);
    ASSERT_OK(_cache->shutdown());
    ASSERT_EQ(_cache->usage(), 0);
}

TEST_F(LRUCacheModuleTest, test_metrics) {
    // insert
    _insert_data();

    for (int i = 200; i < 300; i++) {
        _check_not_found(i);
    }

    for (int i = 50; i < 100; i++) {
        _check_found(i);
    }

    ASSERT_EQ(_cache->lookup_count(), 150);
    ASSERT_EQ(_cache->hit_count(), 50);
    ObjectCacheMetrics metrics = _cache->metrics();
    ASSERT_EQ(metrics.lookup_count, 150);
    ASSERT_EQ(metrics.hit_count, 50);
    ASSERT_EQ(metrics.usage, _kv_size * 128);
    ASSERT_EQ(metrics.capacity, _cache_opt.capacity);
    ASSERT_EQ(metrics.object_item_count, 0);
}
} // namespace starrocks