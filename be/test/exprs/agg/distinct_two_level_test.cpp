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

#include <gtest/gtest.h>

#include <set>
#include <string>
#include <vector>

#include "common/memory/mem_hook_allocator.h"
#include "exprs/agg/aggregate_memory_threshold.h"
#include "exprs/agg/distinct.h"
#include "runtime/mem_pool.h"
#include "types/logical_type.h"

namespace starrocks {

// AdaptiveSliceHashSet switches to a two-level set once it has taken in a multiple of 65536
// distinct keys and the key pool is over agg::two_level_memory_threshold(). The switch releases
// the single-level set, so it must run after lazy_emplace() has returned rather than inside the
// callback that inserted the key -- and it must carry every key across.
class AdaptiveSliceHashSetTest : public ::testing::Test {
protected:
    // One batch past the 65536th distinct key, so the switch happens mid-stream and more keys
    // land in the two-level set afterwards.
    static constexpr int kNumKeys = 65536 + 128;

    // The switch also needs the key pool to be over agg::two_level_memory_threshold(), which is
    // 64 bytes when NDEBUG is off but the L3 size under it. Reserving that much up front makes
    // the switch fire in every build and on every machine, instead of only in debug builds.
    static void reserve_past_the_threshold(MemPool* pool) {
        pool->allocate(static_cast<int64_t>(agg::two_level_memory_threshold()));
        ASSERT_GE(pool->total_allocated_bytes(), static_cast<int64_t>(agg::two_level_memory_threshold()));
    }

    static std::vector<std::string> make_keys() {
        std::vector<std::string> keys(kNumKeys);
        for (int i = 0; i < kNumKeys; i++) {
            keys[i] = "key-" + std::to_string(i);
        }
        return keys;
    }

    static void check_switched_and_complete(const AdaptiveSliceHashSet& set, const std::vector<std::string>& keys) {
        EXPECT_EQ(kNumKeys, set.size());
        EXPECT_TRUE(set.set == nullptr) << "the single-level set should have been released";
        ASSERT_TRUE(set.two_level_set != nullptr) << "the switch to the two-level set never happened";

        std::vector<std::string> dumped;
        set.fill_vector(dumped);
        ASSERT_EQ(static_cast<size_t>(kNumKeys), dumped.size());
        EXPECT_EQ(std::set<std::string>(keys.begin(), keys.end()), std::set<std::string>(dumped.begin(), dumped.end()));
    }
};

TEST_F(AdaptiveSliceHashSetTest, EmplaceKeepsAllKeysAcrossTheSwitch) {
    MemHookAllocator allocator;
    SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&allocator);

    MemPool pool;
    ASSERT_NO_FATAL_FAILURE(reserve_past_the_threshold(&pool));

    AdaptiveSliceHashSet set;
    auto keys = make_keys();
    for (const auto& key : keys) {
        set.emplace(&pool, Slice(key));
    }
    // Duplicates must neither move the counter nor trigger a second switch.
    for (int i = 0; i < 1024; i++) {
        set.emplace(&pool, Slice(keys[i]));
    }

    check_switched_and_complete(set, keys);
}

TEST_F(AdaptiveSliceHashSetTest, LazyEmplaceWithHashKeepsAllKeysAcrossTheSwitch) {
    MemHookAllocator allocator;
    SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&allocator);

    MemPool pool;
    ASSERT_NO_FATAL_FAILURE(reserve_past_the_threshold(&pool));

    AdaptiveSliceHashSet set;
    auto keys = make_keys();
    for (const auto& key : keys) {
        set.lazy_emplace_with_hash(&pool, Slice(key), set.hash_function()(Slice(key)));
    }
    for (int i = 0; i < 1024; i++) {
        set.lazy_emplace_with_hash(&pool, Slice(keys[i]), set.hash_function()(Slice(keys[i])));
    }

    check_switched_and_complete(set, keys);
}

// The serialized form is what a count(distinct) state ships between BEs, and it is produced by
// iterating whichever container is live. Round-trip it after the switch so a two-level state that
// serializes short or long cannot slip through.
TEST_F(AdaptiveSliceHashSetTest, SerializeRoundTripsAfterTheSwitch) {
    MemHookAllocator allocator;
    SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&allocator);

    MemPool pool;
    ASSERT_NO_FATAL_FAILURE(reserve_past_the_threshold(&pool));

    DistinctAggregateState<TYPE_VARCHAR, TYPE_BIGINT> src;
    auto keys = make_keys();
    for (const auto& key : keys) {
        src.update(&pool, Slice(key));
    }
    ASSERT_TRUE(src.set.two_level_set != nullptr) << "the switch to the two-level set never happened";
    ASSERT_EQ(kNumKeys, src.distinct_count());

    std::vector<uint8_t> buffer(src.serialize_size());
    src.serialize(buffer.data());

    MemPool merged_pool;
    DistinctAggregateState<TYPE_VARCHAR, TYPE_BIGINT> merged;
    merged.deserialize_and_merge(&merged_pool, buffer.data(), buffer.size());
    EXPECT_EQ(kNumKeys, merged.distinct_count());

    std::vector<std::string> dumped;
    merged.set.fill_vector(dumped);
    ASSERT_EQ(static_cast<size_t>(kNumKeys), dumped.size());
    EXPECT_EQ(std::set<std::string>(keys.begin(), keys.end()), std::set<std::string>(dumped.begin(), dumped.end()));
}

// clear() has to put the state back on the single-level container; leaving the two-level one in
// place would keep a recycled state on the slower path forever.
TEST_F(AdaptiveSliceHashSetTest, ClearRestoresTheSingleLevelSet) {
    MemHookAllocator allocator;
    SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&allocator);

    MemPool pool;
    ASSERT_NO_FATAL_FAILURE(reserve_past_the_threshold(&pool));

    AdaptiveSliceHashSet set;
    auto keys = make_keys();
    for (const auto& key : keys) {
        set.emplace(&pool, Slice(key));
    }
    ASSERT_TRUE(set.two_level_set != nullptr) << "the switch to the two-level set never happened";

    set.clear();
    EXPECT_EQ(0, set.size());
    EXPECT_TRUE(set.two_level_set == nullptr);
    ASSERT_TRUE(set.set != nullptr);

    set.emplace(&pool, Slice(keys[0]));
    EXPECT_EQ(1, set.size());
    EXPECT_TRUE(set.set != nullptr) << "a single key must not re-trigger the switch";
}

} // namespace starrocks
