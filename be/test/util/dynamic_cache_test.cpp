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
#include <vector>

#include "base/logging.h"

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
    ASSERT_EQ(4, cache.get_entry_sizes().size());
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

// Default tracker accounting is additive: a plain consume() walks the ancestor
// chain, so the cache tracker AND its root (process) tracker both grow. This is the
// long-standing behavior other DynamicCache users rely on.
TEST(DynamicCacheTest, mem_tracker_default_additive) {
    MemTracker root(-1, "root", nullptr);
    MemTracker child(-1, "child", &root);
    DynamicCache<int32_t, int64_t> cache(1000);
    cache.set_mem_tracker(&child); // default: exclude_root = false

    auto* e = cache.get_or_create(1);
    cache.update_object_size(e, 100);
    EXPECT_EQ(100, child.consumption());
    EXPECT_EQ(100, root.consumption());

    cache.release(e);
    cache.clear();
    EXPECT_EQ(0, child.consumption());
    EXPECT_EQ(0, root.consumption());
}

// With exclude_root, the cache tracker labels usage WITHOUT re-adding to the root
// (process) tracker. This is required for caches whose objects live in the normal
// heap (e.g. the vector index cache): the allocator hook already charges those bytes
// to process once, so an additive consume here would double-count process and can
// spuriously trip the process mem_limit.
TEST(DynamicCacheTest, mem_tracker_exclude_root) {
    MemTracker root(-1, "root", nullptr);
    MemTracker child(-1, "child", &root);
    DynamicCache<int32_t, int64_t> cache(1000);
    cache.set_mem_tracker_excluding_root(&child);

    auto* e = cache.get_or_create(1);
    cache.update_object_size(e, 100);
    EXPECT_EQ(100, child.consumption()); // child is labeled
    EXPECT_EQ(0, root.consumption());    // root is NOT double-charged

    cache.release(e);
    cache.clear();
    EXPECT_EQ(0, child.consumption()); // release_without_root returns child to zero
    EXPECT_EQ(0, root.consumption());  // root was never touched
}

// End-to-end reproduction of the process double-count, at the MemTracker level.
//
// The real bug stacks two charges on the process tracker for the same heap bytes:
//   count #1: the allocator hook charges process once while the index loads;
//   count #2: DynamicCache::update_object_size charges memory_usage() again.
// The mem_hook -> MemTracker wiring is stubbed in BE_TEST builds (mem_hook.cpp
// routes to g_mem_usage, not the tracker tree), so we stand in for count #1 with an
// explicit consume() on the process tracker -- its net effect on the process counter
// is identical to the hook's. Count #2 exercises the real cache code path.
TEST(DynamicCacheTest, mem_tracker_no_double_count_on_process) {
    constexpr int64_t kHookBytes = 4096;    // what the allocator hook charges process
    constexpr int64_t kLogicalBytes = 4000; // what the cache reports via memory_usage()

    // Additive cache (pre-fix behavior): process carries BOTH copies -> double-count.
    {
        MemTracker process(-1, "process", nullptr);
        MemTracker vi(-1, "vector_index", &process);
        DynamicCache<int32_t, int64_t> cache(1000);
        cache.set_mem_tracker(&vi);

        process.consume(kHookBytes); // count #1 (allocator hook, modeled)
        auto* e = cache.get_or_create(1);
        cache.update_object_size(e, kLogicalBytes); // count #2 (cache), propagates to process
        EXPECT_EQ(kHookBytes + kLogicalBytes, process.consumption());
        cache.release(e);
        cache.clear();
    }

    // exclude_root cache (the fix): process is counted exactly once.
    {
        MemTracker process(-1, "process", nullptr);
        MemTracker vi(-1, "vector_index", &process);
        DynamicCache<int32_t, int64_t> cache(1000);
        cache.set_mem_tracker_excluding_root(&vi);

        process.consume(kHookBytes); // count #1 (allocator hook, modeled)
        auto* e = cache.get_or_create(1);
        cache.update_object_size(e, kLogicalBytes);   // count #2 now labels vi only
        EXPECT_EQ(kHookBytes, process.consumption()); // process: single count
        EXPECT_EQ(kLogicalBytes, vi.consumption());   // vi: labels the logical size

        cache.release(e);
        cache.clear();
        EXPECT_EQ(kHookBytes, process.consumption()); // cache release must NOT touch process
        EXPECT_EQ(0, vi.consumption());

        process.release(kHookBytes); // free-side hook (modeled): full cycle returns to zero
        EXPECT_EQ(0, process.consumption());
    }
}

TEST(DynamicCacheTest, get_all_entries) {
    DynamicCache<int32_t, int64_t> cache(100);
    for (int i = 0; i < 20; i++) {
        auto e = cache.get_or_create(i, 1);
        cache.release(e);
    }
    std::vector<DynamicCache<int32_t, int64_t>::Entry*> entries = cache.get_all_entries();
    ASSERT_EQ(20, entries.size());
    for (int i = 0; i < 20; i++) {
        auto e = entries[i];
        ASSERT_EQ(i, e->key());
        cache.release(e);
    }
}

} // namespace starrocks
