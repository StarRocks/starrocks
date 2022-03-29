// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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

bool TabletSchemaMap::check_schema_unique_id(const TabletSchemaPB& schema_pb,
                                             const std::shared_ptr<const TabletSchema>& schema_ptr) {
    if (schema_pb.next_column_unique_id() != schema_ptr->next_column_unique_id() ||
        schema_pb.column_size() != schema_ptr->num_columns()) {
        return false;
    }

    for (size_t i = 0; i < schema_ptr->num_columns(); ++i) {
        int32_t pb_unique_id = schema_pb.column(i).unique_id();
        int32_t unique_id = schema_ptr->column(i).unique_id();
        if (pb_unique_id != unique_id) {
            return false;
        }
    }

    return true;
}

std::pair<TabletSchemaMap::TabletSchemaPtr, bool> TabletSchemaMap::emplace(const TabletSchemaPB& schema_pb) {
    SchemaId id = schema_pb.id();
    DCHECK_NE(TabletSchema::invalid_id(), id);
    MapShard* shard = get_shard(id);
    std::unique_lock l(shard->mtx);

    auto it = shard->map.find(id);
    if (it == shard->map.end()) {
        auto ptr = TabletSchema::create(_mem_tracker, schema_pb, this);
        shard->map.emplace(id, ptr);
        return std::make_pair(ptr, true);
    } else {
        std::shared_ptr<const TabletSchema> ptr = it->second.lock();
        if (UNLIKELY(!ptr)) {
            ptr = TabletSchema::create(_mem_tracker, schema_pb, this);
            it->second = std::weak_ptr<const TabletSchema>(ptr);
            return std::make_pair(ptr, true);
        } else {
            if (UNLIKELY(!check_schema_unique_id(schema_pb, ptr))) {
                ptr = TabletSchema::create(_mem_tracker, schema_pb, this);
                it->second = std::weak_ptr<const TabletSchema>(ptr);
                return std::make_pair(ptr, true);
            }
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
