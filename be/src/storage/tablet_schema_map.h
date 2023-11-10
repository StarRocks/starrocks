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

#include <memory>
#include <mutex>

#include "storage/tablet_schema.h"
#include "util/phmap/phmap.h"

namespace starrocks {

using SchemaId = TabletSchema::SchemaId;
using TabletSchemaPtr = std::shared_ptr<const TabletSchema>;
using SchemaVerison = int32_t;


class MapShard {
public:

    using SchemaMapByVersion = phmap::flat_hash_map<SchemaVerison, std::weak_ptr<const TabletSchema>>;

    // Upsert a new TabletSchema into the shard. If not exist before, insert a new TabletSchema. If exist
    // before, replace the old value
    // [thread safe] 
    void upsert(SchemaId id, SchemaVerison version, TabletSchemaPtr result) {
        std::unique_lock l(_mtx);
        auto iter = _map.find(id);
        if (iter == _map.end()) {
            SchemaMapByVersion schema_map;
            schema_map.emplace(version, result);
            _map[id] = schema_map;
        } else {
            auto& schema_map = iter->second;
            auto schema_iter = schem_map.find(version);
            if (schema_iter == schema_map.end()) {
                schema_map.emplace(version, result);
            } else {
                schema_iter->second = std::weak_ptr<const TabletSchema>(result);
            }
        }
    }

    // Removes the TabletSchema (if one exists) with the id equivalent to id.
    //
    // Returns number of elements removed (0 or 1).
    // [thread-safe]
    size_t erase(SchemaId id, SchemaVerison version) {
        std::unique_lock l(_mtx);
        auto iter = _map.find(id);
        if (iter == _map.end()) {
            return 0
        }
        return iter->second.erase(version);
    }

    bool contains(SchemaId id, SchemaVersion version) {
        std::unique_lock l(_mtx);
        auto iter = _map.find(id);
        if (iter == _map.end()) {
            return false;
        }
        return iter->second.contains(version);
    }

    // return tablet schme
    std::weak_ptr<const TabletSchema> get(SchemaId id, SchemaVersion version) {
        std::unique_lock l(_mtx);
        auto iter = _map.find(id);
        if (iter == _map.end()) {
            return nullptr;
        }
        auto res = iter->second.find(version);
        if (res == iter->second.end()) {
            return nullptr;
        }
        if (UNLIKELY(!res->second.lock())) {
            return nullptr;
        }
        return res->second;
    }


private:
    mutable std::mutex _mtx;
    phmap::flat_hash_map<SchemaId, SchemaMapByVersion> _map;
}

// TabletSchemaMap is a map from 64 bits integer to std::shared_ptr<const TabletSchema>.
// This class is used to share the same TabletSchema object among all tablets with the same schema to reduce
// memory usage.
// Use `GlobalTabletSchemaMap::Instance()` to access the global object.
class TabletSchemaMap {
public:

    struct Stats {
        // The total number of items stored in the map.
        size_t num_items = 0;
        // How many bytes occupied by the items stored in this map.
        size_t memory_usage = 0;
        // How many bytes saved by using the TabletSchemaMap.
        size_t saved_memory_usage = 0;
    };

    TabletSchemaMap() = default;

    // Inserts a new TabletSchema into the container constructed with the given arg if there is no TabletSchema with
    // the id of schema_pb in the container.
    //
    // Only `id()` of the schema_pb will be compared to determine whether the schema already exist, it's the caller's
    // duty to ensure that no two different TabletSchemaPB`s have the same `id()`.
    //
    // REQUIRE: schema_pb.id() != TabletSchema::invalid_id()
    //
    // Returns a pair consisting of a pointer to the inserted element, or the already-existing element if no insertion
    // happened, and a bool denoting whether the insertion took place (true if insertion happened, false if it did not).
    // [thread-safe]
    std::pair<TabletSchemaPtr, bool> emplace(const TabletSchemaPB& schema_pb);

    std::pair<TabletSchemaPtr, bool> emplace(const TabletSchemaPtr& tablet_schema);

    // Removes the TabletSchema (if one exists) with the id equivalent to id.
    //
    // Returns number of elements removed (0 or 1).
    // [thread-safe]
    size_t erase(SchemaId id, SchemaVersion version);

    // Checks if there is an element with unique id equivalent to id in the container.
    //
    // Returns true if there is such an element, otherwise false.
    bool contains(SchemaId id, SchemaVersion version) const;

    // NOTE: time complexity of method is high, don't call this method too often.
    // [thread-safe]
    Stats stats() const;

private:
    constexpr static int kShardSize = 16;

    bool check_schema_unique_id(const TabletSchemaPB& schema_pb, const TabletSchemaCSPtr& schema_ptr);
    bool check_schema_unique_id(const TabletSchemaCSPtr& in_schema, const TabletSchemaCSPtr& ori_schema);

    MapShard* get_shard(SchemaId id) { return &_map_shards[id % kShardSize]; }
    const MapShard* get_shard(SchemaId id) const { return &_map_shards[id % kShardSize]; }

    MapShard _map_shards[kShardSize];
};

class GlobalTabletSchemaMap final : public TabletSchemaMap {
public:
    static GlobalTabletSchemaMap* Instance() {
        static GlobalTabletSchemaMap instance;
        return &instance;
    }

private:
    GlobalTabletSchemaMap() = default;
};

} // namespace starrocks
