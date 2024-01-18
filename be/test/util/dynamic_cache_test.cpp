// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "util/dynamic_cache.h"

#include <gtest/gtest.h>

#include <iostream>
#include <vector>

#include "util/logging.h"

namespace starrocks {

TEST(DynamicCacheTest, cache) {
    DynamicCache<int32_t, int64_t> cache(10);
    for (int i = 0; i < 20; i++) {
        auto e = cache.get_or_create(i);
        cache.update_object_size(e, 1);
        cache.release(e);
    }
    // only last 10 (11~19) left
    for (int i = 0; i < 20; i++) {
        auto e = cache.get(i);
        if (i < 10) {
            ASSERT_TRUE(e == nullptr);
        } else {
            ASSERT_TRUE(e != nullptr);
            cache.release(e);
        }
    }
    // reset capacity, only last 5 (15~19) left
    cache.set_capacity(5);
    for (int i = 0; i < 20; i++) {
        auto e = cache.get(i);
        if (i < 15) {
            ASSERT_TRUE(e == nullptr);
        } else {
            ASSERT_TRUE(e != nullptr);
            cache.release(e);
        }
    }
    auto e = cache.get(15);
    cache.release(e);
    e = cache.get_or_create(20);
    cache.update_object_size(e, 1);
    cache.release(e);
    // check 16 is evicted
    ASSERT_TRUE(cache.get(16) == nullptr);
    cache.clear_expired();
    // nothing expired
    ASSERT_EQ(5, cache.size());
    e = cache.get(19);
    e->update_expire_time(MonotonicMillis() - 10);
    cache.release(e);
    cache.clear_expired();
    ASSERT_EQ(4, cache.size());
    ASSERT_TRUE(cache.get(19) == nullptr);
}

TEST(DynamicCacheTest, cache2) {
    int N = 1000;
    DynamicCache<int32_t, int64_t> cache(N);
    for (int i = 0; i < N; i++) {
        auto e = cache.get_or_create(i);
        cache.update_object_size(e, 1);
        cache.release(e);
    }
    std::vector<DynamicCache<int32_t, int64_t>::Entry*> entry_list;
    ASSERT_TRUE(cache.TEST_evict(0, &entry_list));
    ASSERT_EQ(entry_list.size(), N);
    for (DynamicCache<int32_t, int64_t>::Entry* entry : entry_list) {
        delete entry;
    }
}

} // namespace starrocks
