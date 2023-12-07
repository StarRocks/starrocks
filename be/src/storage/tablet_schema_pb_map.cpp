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

#include "storage/tablet_schema_pb_map.h"

#include <bvar/bvar.h>

namespace starrocks {

static void get_stats(std::ostream& os, void*) {
    TabletSchemaPBMap::Stats stats = GlobalTabletSchemaPBMap::Instance()->stats();
    os << stats.num_items << "/" << stats.memory_usage << "/" << stats.saved_memory_usage;
}

// NOLINTNEXTLINE
bvar::PassiveStatus<std::string> g_schema_pb_map_stats("tablet_schema_pb_map", get_stats, NULL);

KeyId TabletSchemaPBMap::gen_id(SchemaId schema_id, SchemaVersion schema_version) {
    return (((int128_t)schema_id) << 64) + schema_version;
}

std::pair<TabletSchemaPBMap::TabletSchemaPBPtr, bool> TabletSchemaPBMap::emplace(const TabletSchemaPB schema_pb) {
    SchemaId schema_id = schema_pb.id();
    SchemaVersion schema_version = schema_pb.schema_version();
    DCHECK_NE(TabletSchema::invalid_id(), id);
    KeyId id = gen_id(schema_id, schema_version);
    MapShard* shard = get_shard(id);

    //
    //
    TabletSchemaPBPtr result = nullptr;
    TabletSchemaPBPtr prt = nullptr;
    bool insert = false;
    {
        std::unique_lock l(shard->mtx);
        auto it = shard->map.find(id);
        if (it == shard->map.end()) {
            result = std::make_shared<TabletSchemaPB>(schema_pb);
            shard->map.emplace(id, result);
            insert = true;
        } else {
            ptr = it->second.lock();
            if (UNLIKELY(!ptr)) {
                result = std::make_shared<TabletSchemaPB>(schema_pb);
                it->second = std::weak_ptr<const TabletSchemaPB>(result);
                insert = true;
            } else {
                result = ptr;
                insert = false;
            }
        }
    }
    return std::make_pair(result, insert);
}

size_t TabletSchemaPBMap::erase(SchemaId schema_id, SchemaVersion schema_version) {
    KeyId id = gen_id(schema_id, schema_version);
    MapShard* shard = get_shard(id);
    std::lock_guard l(shard->mtx);
    return shard->map.erase(id);
}

bool TabletSchemaPBMap::contains(SchemaId schema_id, SchemaVersion schema_version) const {
    KeyId id = gen_id(schema_id, schema_version);
    const MapShard* shard = get_shard(id);
    std::lock_guard l(shard->mtx);
    return shard->map.contains(id);
}

TabletSchemaMap::Stats TabletSchemaPBMap::stats() const {
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