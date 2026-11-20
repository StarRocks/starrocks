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

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <vector>

#include "base/time/monotime.h"
#include "gutil/macros.h"

namespace starrocks {
class GCHelper;
} // namespace starrocks

namespace starrocks::lake {

class Metacache;

// One metacache LRU as an adjustment round sees it. floor <= ceiling, and capacity lies between them.
struct MetacacheCapacityState {
    int64_t usage = 0;
    int64_t capacity = 0;
    int64_t floor = 0;
    int64_t ceiling = 0;
    // Usage of each shard the LRU divides its capacity evenly among. Empty stands for one shard holding |usage|.
    std::vector<int64_t> shard_usages;
};

// Index 0 is the tablet metadata cache, index 1 the segment metadata cache.
using MetacacheCapacityStates = std::array<MetacacheCapacityState, 2>;
using MetacacheCapacities = std::array<int64_t, 2>;

// The capacity of each shard, all shards getting the same, at which shards holding |shard_usages| give up at least
// |bytes| in total: the highest level whose excess over the shards adds up to |bytes|. Only the fullest shards are
// cut down to it. With |bytes| <= 0 it is the usage of the fullest shard, which evicts nothing; with |bytes| above the
// total usage it is 0.
int64_t metacache_shard_level(std::vector<int64_t> shard_usages, int64_t bytes);

// The capacities that release |bytes| of cached entries. Each cache gives up a share in proportion to how far its
// usage is above its floor. A capacity is derived from the cache's current usage rather than from its old capacity,
// since a cache that is not full releases nothing when only its capacity is lowered: it is the shard level that
// releases the share (metacache_shard_level()) times the shard count. The level is computed per shard because an
// LRU divides its capacity evenly among its shards, so a total capacity of usage minus share would also cut every
// shard holding more than the average. A cache whose share is 0 is capped at its fullest shard, so that neither cache
// refills while memory is short. No capacity rises or drops below its floor. An LRU evicts whole entries, so a
// shard may release up to one entry more than its part of the share.
MetacacheCapacities metacache_shrink_capacities(const MetacacheCapacityStates& caches, int64_t bytes);

// The capacities that add |bytes| of room, shared in proportion to how far each capacity is below its ceiling.
// No capacity exceeds its ceiling.
MetacacheCapacities metacache_grow_capacities(const MetacacheCapacityStates& caches, int64_t bytes);

// Moves the capacities of the tablet and segment metadata caches between their floors
// (lake_*_metadata_cache_min_limit) and ceilings (lake_*_metadata_cache_limit_percent) with the process memory usage,
// using the same water levels as the data cache memory adjustment:
//  - above memory_urgent_level, the usage above it is released at once, and the band down to memory_high_level is
//    released gradually;
//  - above memory_high_level, the usage above it is released gradually;
//  - otherwise, the capacities grow back gradually towards their ceilings, by no more than the room left below
//    memory_high_level.
// "Gradually" spreads a release over kShrinkPeriod rounds of kIntervalSeconds (about 50 seconds), so that the caches
// give memory back soon under sustained pressure, and a growth over kGrowPeriod rounds (about 200 seconds), so that
// they refill slowly. Raising a capacity allocates nothing; it only lets later cache misses fill the room. Not
// thread-safe: one thread calls adjust().
class MetacacheAdjuster {
public:
    static constexpr int64_t kIntervalSeconds = 10;
    static constexpr int64_t kShrinkPeriod = 5;
    static constexpr int64_t kGrowPeriod = 20;

    explicit MetacacheAdjuster(const MonoTime& now);
    ~MetacacheAdjuster();

    DISALLOW_COPY_AND_MOVE(MetacacheAdjuster);

    // The capacities for one round, given each cache's state and the process memory usage and limit, under the
    // current water level configs. Returns the current capacities when the water levels are invalid.
    MetacacheCapacities next_capacities(const MonoTime& now, int64_t process_mem_limit, int64_t process_mem_usage,
                                        const MetacacheCapacityStates& caches);

    // Runs one round against |metacache|: reads the caches' state and the limit configs, then applies and logs the
    // new capacities.
    void adjust(Metacache* metacache, const MonoTime& now, int64_t process_mem_limit, int64_t process_mem_usage);

private:
    std::unique_ptr<GCHelper> _dec_advisor;
    std::unique_ptr<GCHelper> _inc_advisor;
};

} // namespace starrocks::lake
