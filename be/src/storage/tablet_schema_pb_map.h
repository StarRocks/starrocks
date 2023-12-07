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

#include "gen_cpp/olap_file.pb.h"
#include "storage/tablet_schema.h"
#include "util/phmap/phmap.h"

namespace starrocks {

class TabletSchemaPBMap {
public:
    using SchemaId = TabletSchema::SchemaId;
    using SchemaVersion = int32_t;
    using TabletSchemaPBPtr = std::shared_ptr<const TabletSchemaPB>;
    using KeyId = int128_t;

    struct Stats {
        // The total number of items stored in the map.
        size_t num_items = 0;
        // How many bytes occupied by the items stored in this map.
        size_t memory_usage = 0;
        // How many bytes saved by using the TabletSchemaMap.
        size_t saved_memory_usage = 0;
    };

    TabletSchemaPBMap() = default;

    // Generate a unique id from schema id and schema verison
    //
    // Return the unique id
    // [thread-safe]
    KeyId gen_id(SchemaId schema_id, SchemaVersion schema_verison);

    // Inserts a new TabletSchemaPB into the container constructed with the given arg if there is no TabletSchemaPB with
    // the id and version in the container.
    //
    // The `id` and `version` of the schema_pb will be compared to determine whether the schema already exist. 
    // Actually, only `id` is enough to determine the TabletShemaPB right now. But we also use schema verison to 
    //
    // REQUIRE: schema_pb.id() != TabletSchema::invalid_id()
    //
    // Returns a pair consisting of a pointer to the inserted element, or the already-existing element if no insertion
    // happened, and a bool denoting whether the insertion took place (true if insertion happened, false if it did not).
    // [thread-safe]
    std::pair<TabletSchemaPBPtr, bool> emplace(const TabletSchemaPB& schema_pb);

    // Removes the TabletSchemaPB (if one exists) with the id equivalent to id and version 
    // equivalent to version.
    //
    // Returns number of elements removed (0 or 1).
    // [thread-safe]
    size_t erase(SchemaId id, SchemaVersion version);

    // Checks if there is an element with unique id equivalent to id in the container
    // and version equivalent to version.
    //
    // Returns true if there is such an element, otherwise false.
    bool contains(SchemaId id, SchemaVersion version) const;

    // NOTE: time complexity of method is high, don't call this method too often.
    // [thread-safe]
    Stats stats() const;

private:
    constexpr static int kShardSize = 16;
    struct MapShard {
        mutable std::mutex mtx;
        phmap::flat_hash_map<KeyId, std::weak_ptr<const TabletSchemaPB>> map;
    };

    MapShard* get_shard(KeyId id) { return &_map_shars[id % kShardSize]; }
    const MapShard* get_shard(KeyId id) const { return &_map_shards[id % kShardSize]; }

    MapShard _map_shards[kShardSize];
};

class GlobalTabletSchemaPBMap final : public TabletSchemaPBMap {
public:
    static GlobalTabletSchemaPBMap* Instance() {
        static GlobalTabletSchemaPBMap instance;
        return &instance;
    }

private:
    GlobalTabletSchemaPBMap() = default;
};

} // namespace starrocks