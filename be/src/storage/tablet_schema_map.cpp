// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/tablet_schema_map.h"

#include <bvar/bvar.h>

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
    // |result|: return value
    // |ptr|: the shared_ptr whose id is equal to `id` in map
    // |insert|: insert into map or not
    // We use shared schema to save mem usage, but the premise is that we need to ensure that no two different
    // TabletSchemaPBs have the same id. But we can't guarantee it after schema change so far, so we should
    // check the consistent of tablet schema.
    // If check failed, we will create a new tablet_schema as return value, but we must hold the original schema
    // in map until the shard lock is release. If not, we may be deconstruct the original schema which will cause
    // a dead lock(#issue 5646)
    TabletSchemaPtr result = nullptr;
    TabletSchemaPtr ptr = nullptr;
    bool insert = false;
    {
        std::unique_lock l(shard->mtx);
        auto it = shard->map.find(id);
        if (it == shard->map.end()) {
            result = TabletSchema::create(schema_pb, this);
            shard->map.emplace(id, result);
            insert = true;
        } else {
            ptr = it->second.lock();
            if (UNLIKELY(!ptr)) {
                result = TabletSchema::create(schema_pb, this);
                it->second = std::weak_ptr<const TabletSchema>(result);
                insert = true;
            } else {
                if (UNLIKELY(!check_schema_unique_id(schema_pb, ptr))) {
                    result = TabletSchema::create(schema_pb, nullptr);
                } else {
                    result = ptr;
                }
                insert = false;
            }
        }
    }

    return std::make_pair(result, insert);
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
        std::vector<std::shared_ptr<const TabletSchema>> tmp_shared_ptrs;
        {
            std::lock_guard l(shard.mtx);
            stats.num_items += shard.map.size();
            for (const auto& [_, weak_ptr] : shard.map) {
                if (auto schema_ptr = weak_ptr.lock(); schema_ptr) {
                    auto use_cnt = schema_ptr.use_count();
                    auto schema_size = schema_ptr->mem_usage();
                    // The temporary variable schema_ptr took one reference, should exclude it.
                    stats.memory_usage += use_cnt >= 2 ? schema_size : 0;
                    stats.saved_memory_usage += (use_cnt >= 2) ? (use_cnt - 2) * schema_size : 0;
                    // save the `schema_ptr` into the vector, prevent it to be the last reference and be destroyed under the lock
                    // which will be a dead lock.
                    tmp_shared_ptrs.push_back(std::move(schema_ptr));
                }
            }
        }
        // release all the shared_ptr instances under no lock
        tmp_shared_ptrs.clear();
    }
    return stats;
}

} // namespace starrocks
