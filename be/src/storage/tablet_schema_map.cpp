// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/tablet_schema_map.h"

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bvar/bvar.h>
DIAGNOSTIC_POP

namespace starrocks {

static void get_stats(std::ostream& os, void*) {
    TabletSchemaMap::Stats stats = GlobalTabletSchemaMap::Instance()->stats();
    os << stats.num_items << "/" << stats.memory_usage << "/" << stats.saved_memory_usage;
}

// NOLINTNEXTLINE
bvar::PassiveStatus<std::string> g_schema_map_stats("tablet_schema_map", get_stats, NULL);

std::pair<TabletSchemaMap::TabletSchemaPtr, bool> TabletSchemaMap::emplace(const TabletSchemaPB& schema_pb) {
    SchemaId id = schema_pb.id();
    DCHECK_NE(TabletSchema::invalid_id(), id);
    MapShard* shard = get_shard(id);
    std::unique_lock l(shard->mtx);

    auto it = shard->map.find(id);
    if (it == shard->map.end()) {
        auto ptr = std::make_shared<const TabletSchema>(schema_pb, this);
        shard->map.emplace(id, ptr);
        return std::make_pair(ptr, true);
    } else {
        std::shared_ptr<const TabletSchema> ptr = it->second.lock();
        if (UNLIKELY(!ptr)) {
            ptr = std::make_shared<const TabletSchema>(schema_pb, this);
            it->second = std::weak_ptr<const TabletSchema>(ptr);
            return std::make_pair(ptr, true);
        } else {
            return std::make_pair(ptr, false);
        }
    }
}

size_t TabletSchemaMap::erase(SchemaId id) {
    MapShard* shard = get_shard(id);
    std::lock_guard l(shard->mtx);
    return shard->map.erase(id);
}

bool TabletSchemaMap::contains(SchemaId id) const {
    const MapShard* shard = get_shard(id);
    std::lock_guard l(shard->mtx);
    return shard->map.contains(id);
}

TabletSchemaMap::Stats TabletSchemaMap::stats() const {
    Stats stats;
    for (const auto& shard : _map_shards) {
        std::lock_guard l(shard.mtx);
        stats.num_items += shard.map.size();
        for (const auto& [_, weak_ptr] : shard.map) {
            if (auto schema_ptr = weak_ptr.lock(); schema_ptr) {
                auto use_cnt = schema_ptr.use_count();
                auto schema_size = schema_ptr->mem_usage();
                // The temporary variable schema_ptr took one reference, should exclude it.
                stats.memory_usage += use_cnt >= 2 ? schema_size : 0;
                stats.saved_memory_usage += (use_cnt >= 2) ? (use_cnt - 2) * schema_size : 0;
            }
        }
    }
    return stats;
}

} // namespace starrocks
