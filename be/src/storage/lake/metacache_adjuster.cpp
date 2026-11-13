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

#include <bvar/bvar.h>
#include <fmt/format.h>

#include <algorithm>
#include <functional>
#include <string>

#include "base/gc/gc_helper.h"
#include "base/utility/pretty_printer.h"
#include "common/config_cache_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/logging.h"
#include "storage/lake/metacache.h"

namespace starrocks::lake {

// Total charge of the entries the adjustment rounds evicted by lowering the capacities, apart from the evictions of
// an LRU that is full.
static bvar::Adder<int64_t> g_metacache_adjust_evicted_bytes("lake", "metacache_adjust_evicted_bytes");

static constexpr int64_t kAlwaysLogEvictedBytes = 256L * 1024 * 1024;

// |bytes| * |part| / |total|, rounded down or up. The product does not fit in int64_t: both factors can be tens of
// GB. With bytes <= total, neither rounding exceeds |part|.
static int64_t proportional_share(int64_t bytes, int64_t part, int64_t total, bool round_up) {
    const __int128 product = static_cast<__int128>(bytes) * part;
    return static_cast<int64_t>(round_up ? (product + total - 1) / total : product / total);
}

int64_t metacache_shard_level(std::vector<int64_t> shard_usages, int64_t bytes) {
    if (shard_usages.empty()) {
        return 0;
    }
    std::sort(shard_usages.begin(), shard_usages.end(), std::greater<>());
    bytes = std::max<int64_t>(bytes, 0);
    // Lower the fullest k shards together until they give up |bytes|, adding the next shard once the level reaches
    // its usage.
    int64_t top_usage = 0;
    for (size_t k = 0; k < shard_usages.size(); ++k) {
        top_usage += shard_usages[k];
        const int64_t next_usage = k + 1 < shard_usages.size() ? shard_usages[k + 1] : 0;
        const auto top_shards = static_cast<int64_t>(k + 1);
        if (top_usage - bytes >= next_usage * top_shards) {
            // Rounded down, so that the top shards give up at least |bytes|.
            return (top_usage - bytes) / top_shards;
        }
    }
    return 0;
}

MetacacheCapacities metacache_shrink_capacities(const MetacacheCapacityStates& caches, int64_t bytes) {
    std::array<int64_t, 2> above_floor{};
    int64_t total_above_floor = 0;
    for (size_t i = 0; i < caches.size(); ++i) {
        above_floor[i] = std::max<int64_t>(0, caches[i].usage - caches[i].floor);
        total_above_floor += above_floor[i];
    }
    bytes = std::clamp<int64_t>(bytes, 0, total_above_floor);
    MetacacheCapacities capacities{};
    for (size_t i = 0; i < caches.size(); ++i) {
        // Rounded up, so that the shares add up to at least |bytes|.
        const int64_t share =
                bytes > 0 ? proportional_share(bytes, above_floor[i], total_above_floor, /*round_up=*/true) : 0;
        const auto& cache = caches[i];
        const bool sharded = !cache.shard_usages.empty();
        const int64_t level =
                metacache_shard_level(sharded ? cache.shard_usages : std::vector<int64_t>{cache.usage}, share);
        const auto shards = static_cast<int64_t>(sharded ? cache.shard_usages.size() : 1);
        // Capped at the fullest shard even when the share is 0, so that neither cache refills while memory is short.
        capacities[i] = std::min(cache.capacity, std::max(cache.floor, level * shards));
    }
    return capacities;
}

MetacacheCapacities metacache_grow_capacities(const MetacacheCapacityStates& caches, int64_t bytes) {
    std::array<int64_t, 2> room{};
    int64_t total_room = 0;
    for (size_t i = 0; i < caches.size(); ++i) {
        room[i] = std::max<int64_t>(0, caches[i].ceiling - caches[i].capacity);
        total_room += room[i];
    }
    bytes = std::clamp<int64_t>(bytes, 0, total_room);
    MetacacheCapacities capacities{};
    for (size_t i = 0; i < caches.size(); ++i) {
        capacities[i] = caches[i].capacity +
                        (bytes > 0 ? proportional_share(bytes, room[i], total_room, /*round_up=*/false) : 0);
    }
    return capacities;
}

MetacacheAdjuster::MetacacheAdjuster(const MonoTime& now)
        : _dec_advisor(std::make_unique<GCHelper>(kShrinkPeriod, kIntervalSeconds, now)),
          _inc_advisor(std::make_unique<GCHelper>(kGrowPeriod, kIntervalSeconds, now)) {}

MetacacheAdjuster::~MetacacheAdjuster() = default;

MetacacheCapacities MetacacheAdjuster::next_capacities(const MonoTime& now, int64_t process_mem_limit,
                                                       int64_t process_mem_usage,
                                                       const MetacacheCapacityStates& caches) {
    // Every round feeds both advisors, the idle one with 0. A GCHelper forgets an excess only when it is called with a
    // smaller one, so an advisor left alone between two memory pressure episodes would start the next one from the
    // last excess it saw, and release (or grow) the whole of the new one in its first round instead of gradually.
    const int64_t urgent_level = config::memory_urgent_level;
    const int64_t high_level = config::memory_high_level;
    if (!(urgent_level > high_level && high_level >= 1 && urgent_level <= 100)) {
        // The other users of these levels already report the misconfiguration.
        _dec_advisor->bytes_should_gc(now, 0);
        _inc_advisor->bytes_should_gc(now, 0);
        return {caches[0].capacity, caches[1].capacity};
    }
    const int64_t urgent = process_mem_limit * urgent_level / 100;
    const int64_t high = process_mem_limit * high_level / 100;
    if (process_mem_usage > urgent) {
        _inc_advisor->bytes_should_gc(now, 0);
        const int64_t gradual = static_cast<int64_t>(_dec_advisor->bytes_should_gc(now, urgent - high));
        return metacache_shrink_capacities(caches, process_mem_usage - urgent + gradual);
    }
    if (process_mem_usage > high) {
        _inc_advisor->bytes_should_gc(now, 0);
        const int64_t gradual = static_cast<int64_t>(_dec_advisor->bytes_should_gc(now, process_mem_usage - high));
        return metacache_shrink_capacities(caches, gradual);
    }
    _dec_advisor->bytes_should_gc(now, 0);
    int64_t room = 0;
    for (const auto& cache : caches) {
        room += cache.ceiling - cache.capacity;
    }
    room = std::max<int64_t>(room, 0);
    const int64_t gradual =
            static_cast<int64_t>(_inc_advisor->bytes_should_gc(now, std::min(room, high - process_mem_usage)));
    if (room == 0) {
        return {caches[0].capacity, caches[1].capacity};
    }
    return metacache_grow_capacities(caches, gradual);
}

void MetacacheAdjuster::adjust(Metacache* metacache, const MonoTime& now, int64_t process_mem_limit,
                               int64_t process_mem_usage) {
    MetacacheCapacityStates caches;
    caches[0].ceiling = metacache_capacity(process_mem_limit, config::lake_tablet_metadata_cache_limit_percent);
    caches[0].floor = std::clamp<int64_t>(config::lake_tablet_metadata_cache_min_limit, 0, caches[0].ceiling);
    caches[0].usage = static_cast<int64_t>(metacache->tablet_metadata_memory_usage());
    caches[0].capacity = static_cast<int64_t>(metacache->tablet_metadata_capacity());
    for (size_t usage : metacache->tablet_metadata_shard_memory_usages()) {
        caches[0].shard_usages.push_back(static_cast<int64_t>(usage));
    }
    caches[1].ceiling = metacache_capacity(process_mem_limit, config::lake_segment_metadata_cache_limit_percent);
    caches[1].floor = std::clamp<int64_t>(config::lake_segment_metadata_cache_min_limit, 0, caches[1].ceiling);
    caches[1].usage = static_cast<int64_t>(metacache->segment_metadata_memory_usage());
    caches[1].capacity = static_cast<int64_t>(metacache->segment_metadata_capacity());
    for (size_t usage : metacache->segment_metadata_shard_memory_usages()) {
        caches[1].shard_usages.push_back(static_cast<int64_t>(usage));
    }
    const MetacacheCapacities old_capacities = {caches[0].capacity, caches[1].capacity};

    // A limit config changed since the last round may leave a capacity outside its new bounds.
    for (auto& cache : caches) {
        cache.capacity = std::clamp(cache.capacity, cache.floor, cache.ceiling);
    }
    const MetacacheCapacities capacities = next_capacities(now, process_mem_limit, process_mem_usage, caches);
    if (capacities == old_capacities) {
        return;
    }

    // Counted by the resize itself: a usage snapshot before and after would also take in the entries that concurrent
    // queries and loads insert or erase meanwhile.
    const auto tablet_evicted = static_cast<int64_t>(metacache->update_tablet_metadata_capacity(capacities[0]));
    const auto segment_evicted = static_cast<int64_t>(metacache->update_segment_metadata_capacity(capacities[1]));
    const int64_t evicted = tablet_evicted + segment_evicted;
    g_metacache_adjust_evicted_bytes << evicted;
    const std::string message = fmt::format(
            "Adjusted lake metadata cache capacities at process memory {} of {}. tablet metadata cache: capacity {} -> "
            "{}, usage {}, evicted {}. segment metadata cache: capacity {} -> {}, usage {}, evicted {}",
            PrettyPrinter::print_bytes(process_mem_usage), PrettyPrinter::print_bytes(process_mem_limit),
            PrettyPrinter::print_bytes(old_capacities[0]), PrettyPrinter::print_bytes(capacities[0]),
            PrettyPrinter::print_bytes(caches[0].usage), PrettyPrinter::print_bytes(tablet_evicted),
            PrettyPrinter::print_bytes(old_capacities[1]), PrettyPrinter::print_bytes(capacities[1]),
            PrettyPrinter::print_bytes(caches[1].usage), PrettyPrinter::print_bytes(segment_evicted));
    // A process hovering around memory_high_level nudges the capacities every round, so small adjustments are logged
    // at most once a minute. A round that evicts a sizable amount is always logged.
    if (evicted >= kAlwaysLogEvictedBytes) {
        LOG(INFO) << message;
    } else {
        LOG_EVERY_T(INFO, 60) << message;
    }
}

} // namespace starrocks::lake
