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

#include "cache/object_cache/object_cache.h"

#include <gtest/gtest.h>

#include "cache/block_cache/block_cache.h"
#include "cache/object_cache/lrucache_module.h"
#include "cache/object_cache/starcache_module.h"
#include "fs/fs_util.h"
#include "testutil/assert.h"

namespace starrocks {
class ObjectCacheTest : public ::testing::TestWithParam<ObjectCacheModuleType> {
protected:
    void SetUp() override {
        _mode = GetParam();
        if (_mode == ObjectCacheModuleType::LRUCACHE) {
            _value_size = 4;
            _kv_size = _key_size + _value_size;
            _mem_quota = 16384;
            _cache_opt.capacity = _mem_quota;
            _cache = std::make_shared<LRUCacheModule>(_cache_opt);
        } else {
            _value_size = 300 * 1024;
            _kv_size = _value_size;
            _mem_quota = 64 * 1024 * 1024;
            _cache_opt.capacity = _mem_quota;
            ASSERT_OK(fs::create_directories(_cache_dir));
            _init_block_cache();
            _cache = std::make_shared<StarCacheModule>(_block_cache->starcache_instance());
        }
    }
    void TearDown() override {
        if (_mode == ObjectCacheModuleType::STARCACHE) {
            ASSERT_OK(_block_cache->shutdown());
            ASSERT_OK(fs::remove_all(_cache_dir));
        }
    }

    static void Deleter(const CacheKey& k, void* v) { free(v); }
    void _insert_data();
    void _check_not_found(int value);
    void _check_found(int value);
    void _init_block_cache();

    static std::string int_to_string(size_t length, int num) {
        std::ostringstream oss;
        oss << std::setw(length) << std::setfill('0') << num;
        return oss.str();
    }

    ObjectCacheModuleType _mode;
    std::string _cache_dir = "./object_cache_test";
    int64_t _mem_quota = 0;
    std::shared_ptr<BlockCache> _block_cache;
    std::shared_ptr<ObjectCache> _cache;
    ObjectCacheOptions _cache_opt;
    ObjectCacheWriteOptions _write_opt;
    ObjectCacheReadOptions _read_opt;

    size_t _key_size = sizeof(LRUHandle) - 1 + 6;
    size_t _value_size = 0;
    size_t _kv_size = 0;
};

void ObjectCacheTest::_init_block_cache() {
    _block_cache = std::make_shared<BlockCache>();

    CacheOptions options;
    options.mem_space_size = _mem_quota;
    size_t quota = 50 * 1024 * 1024;
    options.disk_spaces.push_back({.path = _cache_dir, .size = quota});
    options.block_size = 256 * 1024;
    options.max_concurrent_inserts = 100000;
    options.max_flying_memory_mb = 100;
    options.engine = "starcache";
    ASSERT_OK(_block_cache->init(options));
}

void ObjectCacheTest::_insert_data() {
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

void ObjectCacheTest::_check_not_found(int value) {
    std::string key = int_to_string(6, value);
    ObjectCacheHandlePtr handle = nullptr;
    Status st = _cache->lookup(key, &handle, &_read_opt);
    ASSERT_TRUE(st.is_not_found());
}

void ObjectCacheTest::_check_found(int value) {
    std::string key = int_to_string(6, value);
    ObjectCacheHandlePtr handle = nullptr;
    ASSERT_OK(_cache->lookup(key, &handle, &_read_opt));
    ASSERT_EQ(*(int*)(_cache->value(handle)), value);
    _cache->release(handle);
}

TEST_P(ObjectCacheTest, test_init) {
    ASSERT_TRUE(_cache->initialized());
    ASSERT_TRUE(_cache->available());
    ASSERT_EQ(_cache->capacity(), _cache_opt.capacity);

    ASSERT_OK(_cache->set_capacity(0));
    ASSERT_FALSE(_cache->available());
}

TEST_P(ObjectCacheTest, test_insert_lookup) {
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

TEST_P(ObjectCacheTest, test_value_slice) {
    _insert_data();

    std::string key = int_to_string(6, 30);
    ObjectCacheHandlePtr handle = nullptr;
    ASSERT_OK(_cache->lookup(key, &handle, &_read_opt));

    Slice value = _cache->value_slice(handle);
    ASSERT_EQ(value.size, _value_size);
    ASSERT_EQ(*(int*)value.data, 30);
    _cache->release(handle);
}

TEST_P(ObjectCacheTest, test_set_capacity) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->capacity(), _cache_opt.capacity);
    ASSERT_EQ(_cache->usage(), _kv_size * 128);

    ASSERT_OK(_cache->set_capacity(_mem_quota / 2));
    ASSERT_EQ(_cache->capacity(), _mem_quota / 2);
    ASSERT_GT(_cache->usage(), 0);
    ASSERT_LE(_cache->usage(), _mem_quota / 2);

    // not found
    _check_not_found(0);

    // found
    _check_found(127);
}

TEST_P(ObjectCacheTest, test_adjust_capacity) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->capacity(), _cache_opt.capacity);
    ASSERT_EQ(_cache->usage(), _kv_size * 128);

    ASSERT_OK(_cache->adjust_capacity(-_mem_quota / 2, 1024));
    ASSERT_EQ(_cache->capacity(), _mem_quota / 2);
    ASSERT_GT(_cache->usage(), 0);
    ASSERT_LE(_cache->usage(), _mem_quota / 2);

    // not found
    _check_not_found(0);

    // found
    _check_found(127);
}

TEST_P(ObjectCacheTest, test_prune) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->usage(), _kv_size * 128);
    ASSERT_OK(_cache->prune());
    ASSERT_EQ(_cache->usage(), 0);
}

TEST_P(ObjectCacheTest, test_shutdown) {
    // insert
    _insert_data();

    ASSERT_EQ(_cache->usage(), _kv_size * 128);
    ASSERT_OK(_cache->shutdown());
    ASSERT_EQ(_cache->usage(), 0);
}

TEST_P(ObjectCacheTest, test_metrics) {
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
    if (_mode == ObjectCacheModuleType::STARCACHE) {
        ASSERT_EQ(metrics.object_item_count, 128);
    } else {
        ASSERT_EQ(metrics.object_item_count, 0);
    }
}

INSTANTIATE_TEST_SUITE_P(ObjectCacheTest, ObjectCacheTest,
                         ::testing::Values(ObjectCacheModuleType::LRUCACHE, ObjectCacheModuleType::STARCACHE));
} // namespace starrocks