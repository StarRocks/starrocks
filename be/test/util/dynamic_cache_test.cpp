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

#include "util/dynamic_cache.h"

#include <gtest/gtest.h>

#include <iostream>

#include "util/logging.h"

namespace starrocks {

TEST(DynamicCacheTest, cache) {
    DynamicCache<int32_t, int64_t> cache(10);
    for (int i = 0; i < 20; i++) {
        auto e = cache.get_or_create(i);
        cache.update_object_size(e, 1);
        cache.release(e);
    }
    // only last 8 (12~19) left
    for (int i = 0; i < 20; i++) {
        auto e = cache.get(i);
        if (i < 12) {
            ASSERT_TRUE(e == nullptr);
        } else {
            ASSERT_TRUE(e != nullptr);
            cache.release(e);
        }
    }
    // reset capacity, cache size is greater than capacity, release all entry
    // which can be released
    cache.set_capacity(5);
    for (int i = 0; i < 20; i++) {
        auto e = cache.get(i);
        ASSERT_TRUE(e == nullptr);
    }
    for (int i = 15; i < 20; i++) {
        auto e = cache.get_or_create(i);
        cache.update_object_size(e, 1);
        cache.release(e);
    }

    auto e = cache.get(16);
    cache.release(e);
    e = cache.get_or_create(20);
    cache.update_object_size(e, 1);
    cache.release(e);
    // check 17 is evicted
    ASSERT_TRUE(cache.get(17) == nullptr);
    cache.clear_expired();
    // nothing expired
    ASSERT_EQ(4, cache.size());
    e = cache.get(19);
    e->update_expire_time(MonotonicMillis() - 10);
    cache.release(e);
    cache.clear_expired();
    ASSERT_EQ(3, cache.size());
    ASSERT_TRUE(cache.get(19) == nullptr);
}

} // namespace starrocks
