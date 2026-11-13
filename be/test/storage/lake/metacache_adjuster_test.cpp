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

#include "storage/lake/metacache_adjuster.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <string>

#include "common/config_cache_fwd.h"
#include "common/config_lake_fwd.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/metacache.h"

namespace starrocks::lake {

static constexpr int64_t kKB = 1024;
static constexpr int64_t kMB = 1024 * kKB;
static constexpr int64_t kGB = 1024 * kMB;
static constexpr int64_t kSecondInNs = 1000L * 1000 * 1000;

class MetacacheAdjusterTest : public ::testing::Test {
protected:
    void SetUp() override {
        _memory_high_level = config::memory_high_level;
        _memory_urgent_level = config::memory_urgent_level;
        _tablet_percent = config::lake_tablet_metadata_cache_limit_percent;
        _segment_percent = config::lake_segment_metadata_cache_limit_percent;
        _tablet_min_limit = config::lake_tablet_metadata_cache_min_limit;
        _segment_min_limit = config::lake_segment_metadata_cache_min_limit;
        config::memory_high_level = 75;
        config::memory_urgent_level = 85;
    }

    void TearDown() override {
        config::memory_high_level = _memory_high_level;
        config::memory_urgent_level = _memory_urgent_level;
        config::lake_tablet_metadata_cache_limit_percent = _tablet_percent;
        config::lake_segment_metadata_cache_limit_percent = _segment_percent;
        config::lake_tablet_metadata_cache_min_limit = _tablet_min_limit;
        config::lake_segment_metadata_cache_min_limit = _segment_min_limit;
    }

    // Caches that fill every byte of their capacities at once, inside a process using |other| bytes besides them.
    struct SimulatedProcess {
        MetacacheCapacityStates caches;
        int64_t other = 0;

        int64_t usage() const { return other + caches[0].usage + caches[1].usage; }

        void apply(const MetacacheCapacities& capacities) {
            for (size_t i = 0; i < caches.size(); ++i) {
                caches[i].capacity = capacities[i];
                caches[i].usage = capacities[i];
            }
        }
    };

    static MetacacheCapacityStates full_caches(int64_t tablet, int64_t segment) {
        return {MetacacheCapacityState{.usage = tablet, .capacity = tablet, .floor = 1 * kGB, .ceiling = 10 * kGB},
                MetacacheCapacityState{.usage = segment, .capacity = segment, .floor = 1 * kGB, .ceiling = 20 * kGB}};
    }

    int64_t _memory_high_level = 0;
    int64_t _memory_urgent_level = 0;
    int32_t _tablet_percent = 0;
    int32_t _segment_percent = 0;
    int64_t _tablet_min_limit = 0;
    int64_t _segment_min_limit = 0;
};

TEST_F(MetacacheAdjusterTest, shrink_shares_by_usage_above_floor) {
    MetacacheCapacityStates caches = full_caches(6 * kGB, 20 * kGB);
    caches[0].capacity = 10 * kGB;
    // 5 GB and 19 GB above the floors: 6 GB splits into 1.25 GB and 4.75 GB.
    auto capacities = metacache_shrink_capacities(caches, 6 * kGB);
    EXPECT_EQ(6 * kGB - 5 * kGB / 4, capacities[0]);
    EXPECT_EQ(20 * kGB - 19 * kGB / 4, capacities[1]);
}

TEST_F(MetacacheAdjusterTest, shrink_stops_at_floors) {
    auto capacities = metacache_shrink_capacities(full_caches(6 * kGB, 20 * kGB), 100 * kGB);
    EXPECT_EQ(1 * kGB, capacities[0]);
    EXPECT_EQ(1 * kGB, capacities[1]);
}

TEST_F(MetacacheAdjusterTest, shrink_caps_a_cache_below_its_floor_at_the_floor) {
    MetacacheCapacityStates caches = full_caches(512 * kMB, 20 * kGB);
    caches[0].capacity = 10 * kGB;
    auto capacities = metacache_shrink_capacities(caches, 2 * kGB);
    // Nothing above the floor to release, but the capacity no longer leaves room to refill.
    EXPECT_EQ(1 * kGB, capacities[0]);
    EXPECT_EQ(18 * kGB, capacities[1]);
}

TEST_F(MetacacheAdjusterTest, shrink_of_nothing_caps_capacities_at_usage) {
    MetacacheCapacityStates caches = full_caches(6 * kGB, 8 * kGB);
    caches[0].capacity = 10 * kGB;
    caches[1].capacity = 20 * kGB;
    auto capacities = metacache_shrink_capacities(caches, 0);
    EXPECT_EQ(6 * kGB, capacities[0]);
    EXPECT_EQ(8 * kGB, capacities[1]);
}

TEST_F(MetacacheAdjusterTest, shrink_never_raises_a_capacity) {
    MetacacheCapacityStates caches = full_caches(6 * kGB, 8 * kGB);
    // An in-place charge update can leave the usage above the capacity for a while.
    caches[0].capacity = 5 * kGB;
    auto capacities = metacache_shrink_capacities(caches, 0);
    EXPECT_EQ(5 * kGB, capacities[0]);
    EXPECT_EQ(8 * kGB, capacities[1]);
}

TEST_F(MetacacheAdjusterTest, shard_level_cuts_only_the_fullest_shards) {
    // Nothing to release: the fullest shard, which evicts nothing.
    EXPECT_EQ(10 * kMB, metacache_shard_level({2 * kMB, 10 * kMB, 10 * kMB, 2 * kMB}, 0));
    // 4 MB comes from the two 10 MB shards alone.
    EXPECT_EQ(8 * kMB, metacache_shard_level({2 * kMB, 10 * kMB, 10 * kMB, 2 * kMB}, 4 * kMB));
    // 18 MB brings the two 10 MB shards down to 2 MB, then all four shards share the remaining 6 MB.
    EXPECT_EQ(3 * kMB / 2, metacache_shard_level({2 * kMB, 10 * kMB, 10 * kMB, 2 * kMB}, 18 * kMB));
    EXPECT_EQ(0, metacache_shard_level({2 * kMB, 10 * kMB, 10 * kMB, 2 * kMB}, 100 * kMB));
    EXPECT_EQ(0, metacache_shard_level({}, 1 * kMB));
}

TEST_F(MetacacheAdjusterTest, shrink_releases_the_share_from_the_fullest_shards) {
    MetacacheCapacityStates caches = full_caches(1 * kGB, 24 * kMB);
    caches[1].floor = 0;
    caches[1].capacity = 20 * kGB;
    caches[1].shard_usages = {2 * kMB, 10 * kMB, 10 * kMB, 2 * kMB};
    auto capacities = metacache_shrink_capacities(caches, 4 * kMB);
    // Each shard gets 8 MB: only the two 10 MB shards give up 2 MB each.
    EXPECT_EQ(4 * 8 * kMB, capacities[1]);
}

TEST_F(MetacacheAdjusterTest, shrink_of_nothing_with_uneven_shards_evicts_nothing) {
    // Sixteen 100 MB entries in sixteen of 32 shards: a total capacity of their 1.6 GB usage would give each shard
    // 50 MB and evict all of them.
    MetacacheCapacityStates caches = full_caches(1 * kGB, 1600 * kMB);
    caches[1].capacity = 20 * kGB;
    caches[1].shard_usages.assign(32, 0);
    std::fill_n(caches[1].shard_usages.begin(), 16, 100 * kMB);
    auto capacities = metacache_shrink_capacities(caches, 0);
    EXPECT_EQ(32 * 100 * kMB, capacities[1]);
}

TEST_F(MetacacheAdjusterTest, grow_shares_by_room_below_ceiling) {
    MetacacheCapacityStates caches = full_caches(2 * kGB, 4 * kGB);
    // 8 GB and 16 GB of room: 6 GB splits into 2 GB and 4 GB.
    auto capacities = metacache_grow_capacities(caches, 6 * kGB);
    EXPECT_EQ(4 * kGB, capacities[0]);
    EXPECT_EQ(8 * kGB, capacities[1]);
}

TEST_F(MetacacheAdjusterTest, grow_stops_at_ceilings) {
    auto capacities = metacache_grow_capacities(full_caches(2 * kGB, 4 * kGB), 100 * kGB);
    EXPECT_EQ(10 * kGB, capacities[0]);
    EXPECT_EQ(20 * kGB, capacities[1]);
}

TEST_F(MetacacheAdjusterTest, above_urgent_releases_the_excess_at_once) {
    MonoTime now(kSecondInNs);
    MetacacheAdjuster adjuster(now);
    const int64_t limit = 100 * kGB;
    MetacacheCapacityStates caches = full_caches(6 * kGB, 20 * kGB);
    now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
    auto capacities = adjuster.next_capacities(now, limit, 90 * kGB, caches);
    const int64_t released = (6 * kGB - capacities[0]) + (20 * kGB - capacities[1]);
    // At least the 5 GB above 85%, at most that plus the 10 GB band down to 75%.
    EXPECT_GE(released, 5 * kGB);
    EXPECT_LE(released, 15 * kGB);
    // Shared 5 : 19, the usages above the floors.
    EXPECT_NEAR(static_cast<double>(6 * kGB - capacities[0]) / static_cast<double>(released), 5.0 / 24, 1e-6);
}

TEST_F(MetacacheAdjusterTest, above_high_releases_gradually_down_to_high) {
    MonoTime now(kSecondInNs);
    MetacacheAdjuster adjuster(now);
    const int64_t limit = 100 * kGB;
    const int64_t high = limit * 75 / 100;
    SimulatedProcess process{.caches = full_caches(6 * kGB, 20 * kGB), .other = 54 * kGB};
    ASSERT_EQ(80 * kGB, process.usage());

    for (int round = 0; round < 3; ++round) {
        now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
        process.apply(adjuster.next_capacities(now, limit, process.usage(), process.caches));
    }
    EXPECT_GT(process.usage(), high) << "the first rounds must not release the whole excess";
    EXPECT_LT(process.usage(), 80 * kGB);

    for (int round = 0; round < MetacacheAdjuster::kShrinkPeriod + 5; ++round) {
        now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
        process.apply(adjuster.next_capacities(now, limit, process.usage(), process.caches));
    }
    EXPECT_LE(process.usage(), high);
    EXPECT_GT(process.usage(), high - 1 * kGB);
    EXPECT_GT(process.caches[0].capacity, 1 * kGB);
    EXPECT_GT(process.caches[1].capacity, 1 * kGB);
}

TEST_F(MetacacheAdjusterTest, below_high_grows_back_to_ceilings) {
    MonoTime now(kSecondInNs);
    MetacacheAdjuster adjuster(now);
    const int64_t limit = 100 * kGB;
    SimulatedProcess process{.caches = full_caches(1 * kGB, 1 * kGB), .other = 20 * kGB};

    now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
    process.apply(adjuster.next_capacities(now, limit, process.usage(), process.caches));
    EXPECT_LT(process.caches[0].capacity + process.caches[1].capacity, 30 * kGB)
            << "the first round must not grow all the way";

    for (int round = 0; round < MetacacheAdjuster::kGrowPeriod + 5; ++round) {
        now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
        process.apply(adjuster.next_capacities(now, limit, process.usage(), process.caches));
    }
    EXPECT_EQ(10 * kGB, process.caches[0].capacity);
    EXPECT_EQ(20 * kGB, process.caches[1].capacity);
}

TEST_F(MetacacheAdjusterTest, growth_stays_below_high) {
    MonoTime now(kSecondInNs);
    MetacacheAdjuster adjuster(now);
    const int64_t limit = 100 * kGB;
    const int64_t high = limit * 75 / 100;
    SimulatedProcess process{.caches = full_caches(1 * kGB, 1 * kGB), .other = 63 * kGB};

    for (int round = 0; round < 3 * MetacacheAdjuster::kGrowPeriod; ++round) {
        now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
        process.apply(adjuster.next_capacities(now, limit, process.usage(), process.caches));
        EXPECT_LE(process.usage(), high);
    }
    EXPECT_GT(process.caches[0].capacity, 1 * kGB);
    EXPECT_LT(process.caches[1].capacity, 20 * kGB);
}

TEST_F(MetacacheAdjusterTest, a_later_pressure_episode_is_released_gradually_too) {
    MonoTime now(kSecondInNs);
    MetacacheAdjuster adjuster(now);
    const int64_t limit = 100 * kGB;
    const int64_t high = limit * 75 / 100;
    SimulatedProcess process{.caches = full_caches(6 * kGB, 20 * kGB), .other = 57 * kGB};
    auto round = [&] {
        now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
        process.apply(adjuster.next_capacities(now, limit, process.usage(), process.caches));
    };
    // A first episode 8 GB above high, which ends abruptly when a large query finishes.
    ASSERT_EQ(high + 8 * kGB, process.usage());
    for (int i = 0; i < 2; ++i) {
        round();
    }
    process.other -= 30 * kGB;
    // Half an hour below high.
    for (int i = 0; i < 180; ++i) {
        round();
    }
    ASSERT_LT(process.usage(), high);

    // A second episode 5 GB above high starts gradually, as the first one did: its first two rounds release about
    // 300 MB, where an advisor still holding the first episode would release all 5 GB at once.
    process.other += high + 5 * kGB - process.usage();
    const int64_t usage = process.usage();
    for (int i = 0; i < 2; ++i) {
        round();
    }
    EXPECT_LT(usage - process.usage(), 1 * kGB);
}

TEST_F(MetacacheAdjusterTest, growth_after_a_pressure_episode_is_gradual_too) {
    MonoTime now(kSecondInNs);
    MetacacheAdjuster adjuster(now);
    const int64_t limit = 100 * kGB;
    const int64_t high = limit * 75 / 100;
    SimulatedProcess process{.caches = full_caches(1 * kGB, 1 * kGB), .other = 20 * kGB};
    auto round = [&] {
        now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
        process.apply(adjuster.next_capacities(now, limit, process.usage(), process.caches));
    };
    // Growth starts, and memory pressure interrupts it while most of the room is left.
    for (int i = 0; i < 3; ++i) {
        round();
    }
    process.other = high + 4 * kGB - process.caches[0].usage - process.caches[1].usage;
    for (int i = 0; i < 30; ++i) {
        round();
    }

    // Once the pressure is gone, growth starts gradually again.
    process.other = 20 * kGB;
    const int64_t capacity = process.caches[0].capacity + process.caches[1].capacity;
    round();
    EXPECT_LT(process.caches[0].capacity + process.caches[1].capacity - capacity, 1 * kGB);
}

TEST_F(MetacacheAdjusterTest, invalid_water_levels_keep_capacities) {
    config::memory_high_level = 90;
    config::memory_urgent_level = 80;
    MonoTime now(kSecondInNs);
    MetacacheAdjuster adjuster(now);
    MetacacheCapacityStates caches = full_caches(6 * kGB, 20 * kGB);
    now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
    auto capacities = adjuster.next_capacities(now, 100 * kGB, 95 * kGB, caches);
    EXPECT_EQ(6 * kGB, capacities[0]);
    EXPECT_EQ(20 * kGB, capacities[1]);
}

// Fills |metacache| with entries of about |entry_size| bytes until neither cache grows any more.
static void fill(Metacache* metacache, int64_t entry_size) {
    for (int i = 0; i < 4096; ++i) {
        auto log = std::make_shared<TxnLogPB>();
        log->mutable_op_write()->mutable_rowset()->add_segment_metas()->set_filename(std::string(entry_size, 'x'));
        metacache->cache_txn_log(MetacacheKey::TEST_from_string(fmt::format("txn_log_{}", i)), std::move(log));
        metacache->cache_tablet_schema(MetacacheKey::TEST_from_string(fmt::format("schema_{}", i)), nullptr,
                                       entry_size);
    }
}

TEST_F(MetacacheAdjusterTest, adjust_shrinks_the_caches_under_pressure) {
    const int64_t limit = 320 * kMB;
    config::lake_tablet_metadata_cache_limit_percent = 10;
    config::lake_segment_metadata_cache_limit_percent = 20;
    config::lake_tablet_metadata_cache_min_limit = 8 * kMB;
    config::lake_segment_metadata_cache_min_limit = 8 * kMB;
    Metacache metacache(32 * kMB, 64 * kMB);
    fill(&metacache, 16 * kKB);
    const auto tablet_usage = static_cast<int64_t>(metacache.tablet_metadata_memory_usage());
    const auto segment_usage = static_cast<int64_t>(metacache.segment_metadata_memory_usage());
    ASSERT_GT(tablet_usage, 16 * kMB);
    ASSERT_GT(segment_usage, 32 * kMB);

    MonoTime now(kSecondInNs);
    MetacacheAdjuster adjuster(now);
    now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
    // 95% of the limit: at least the 10% above memory_urgent_level is released at once.
    adjuster.adjust(&metacache, now, limit, limit * 95 / 100);

    const int64_t released = (tablet_usage - static_cast<int64_t>(metacache.tablet_metadata_memory_usage())) +
                             (segment_usage - static_cast<int64_t>(metacache.segment_metadata_memory_usage()));
    EXPECT_GE(released, limit / 10);
    EXPECT_LT(static_cast<int64_t>(metacache.tablet_metadata_capacity()), tablet_usage);
    EXPECT_LT(static_cast<int64_t>(metacache.segment_metadata_capacity()), segment_usage);
    EXPECT_GE(static_cast<int64_t>(metacache.tablet_metadata_capacity()), 8 * kMB);
    EXPECT_GE(static_cast<int64_t>(metacache.segment_metadata_capacity()), 8 * kMB);
    EXPECT_LE(metacache.tablet_metadata_memory_usage(), metacache.tablet_metadata_capacity());
    EXPECT_LE(metacache.segment_metadata_memory_usage(), metacache.segment_metadata_capacity());
}

TEST_F(MetacacheAdjusterTest, adjust_just_above_high_keeps_large_entries) {
    const int64_t limit = 100 * kGB;
    config::lake_tablet_metadata_cache_limit_percent = 10;
    config::lake_segment_metadata_cache_limit_percent = 20;
    config::lake_tablet_metadata_cache_min_limit = 1 * kGB;
    config::lake_segment_metadata_cache_min_limit = 1 * kGB;
    Metacache metacache(10 * kGB, 20 * kGB);
    for (int i = 0; i < 16; ++i) {
        metacache.cache_tablet_schema(MetacacheKey::TEST_from_string(fmt::format("large_{}", i)), nullptr, 100 * kMB);
    }
    // The LRU also charges each entry for its handle and key.
    const auto usage = static_cast<int64_t>(metacache.segment_metadata_memory_usage());
    ASSERT_GE(usage, 1600 * kMB);

    MonoTime now(kSecondInNs);
    MetacacheAdjuster adjuster(now);
    now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
    // Just above memory_high_level: the first round of a gradual release frees nothing yet, but stops the caches
    // from growing.
    adjuster.adjust(&metacache, now, limit, limit * 76 / 100);
    EXPECT_EQ(usage, static_cast<int64_t>(metacache.segment_metadata_memory_usage()));
    EXPECT_LT(static_cast<int64_t>(metacache.segment_metadata_capacity()), 20 * kGB);
    EXPECT_EQ(1 * kGB, static_cast<int64_t>(metacache.tablet_metadata_capacity()));
}

TEST_F(MetacacheAdjusterTest, adjust_keeps_a_floor_above_the_ceiling_at_the_ceiling) {
    const int64_t limit = 320 * kMB;
    config::lake_tablet_metadata_cache_limit_percent = 10;
    config::lake_segment_metadata_cache_limit_percent = 20;
    config::lake_tablet_metadata_cache_min_limit = 1 * kGB;
    config::lake_segment_metadata_cache_min_limit = 1 * kGB;
    Metacache metacache(32 * kMB, 64 * kMB);
    fill(&metacache, 16 * kKB);

    MonoTime now(kSecondInNs);
    MetacacheAdjuster adjuster(now);
    now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
    adjuster.adjust(&metacache, now, limit, limit * 95 / 100);
    EXPECT_EQ(32 * kMB, static_cast<int64_t>(metacache.tablet_metadata_capacity()));
    EXPECT_EQ(64 * kMB, static_cast<int64_t>(metacache.segment_metadata_capacity()));
}

TEST_F(MetacacheAdjusterTest, adjust_follows_a_lowered_limit_config) {
    const int64_t limit = 320 * kMB;
    config::lake_tablet_metadata_cache_limit_percent = 5;
    config::lake_segment_metadata_cache_limit_percent = 10;
    config::lake_tablet_metadata_cache_min_limit = 8 * kMB;
    config::lake_segment_metadata_cache_min_limit = 8 * kMB;
    Metacache metacache(32 * kMB, 64 * kMB);

    MonoTime now(kSecondInNs);
    MetacacheAdjuster adjuster(now);
    now += MonoDelta::FromSeconds(MetacacheAdjuster::kIntervalSeconds);
    adjuster.adjust(&metacache, now, limit, limit / 10);
    EXPECT_EQ(16 * kMB, static_cast<int64_t>(metacache.tablet_metadata_capacity()));
    EXPECT_EQ(32 * kMB, static_cast<int64_t>(metacache.segment_metadata_capacity()));
}

} // namespace starrocks::lake
