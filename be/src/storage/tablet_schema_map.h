// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>
#include <mutex>

#include "storage/tablet_schema.h"
#include "util/phmap/phmap.h"

namespace starrocks {

class GlobalTabletSchemaMap {
public:
    using UniqueId = TabletSchema::UniqueId;
    using SchemaPtr = std::shared_ptr<const TabletSchema>;

    struct Stats {
        // The total number of items stored in the map.
        size_t num_items = 0;
        // How many bytes occupied by the items stored in this map.
        size_t memory_usage = 0;
        // How many bytes saved by using the GlobalTabletSchemaMap.
        size_t saved_memory_usage = 0;
    };

    static GlobalTabletSchemaMap* Instance() {
        static GlobalTabletSchemaMap instance;
        return &instance;
    }

    // Inserts a new TabletSchema into the container constructed with the given arg if there is no TabletSchema with
    // the the id of arg in the container.
    //
    // REQUIRE: arg.unique_id() != TabletSchema::invalid_id()
    //
    // Returns a pair consisting of a pointer to the inserted element, or the already-existing element if no insertion
    // happened, and a bool denoting whether the insertion took place (true if insertion happened, false if it did not).
    // [thread-safe]
    std::pair<SchemaPtr, bool> emplace(const TabletSchemaPB& arg);

    // Removes the TabletSchema (if one exists) with the id equivalent to id.
    //
    // Returns number of elements removed (0 or 1).
    // [thread-safe]
    size_t erase(UniqueId id);

    // NOTE: time complexity of method is high, don't call this method too often.
    // [thread-safe]
    Stats stats() const;

private:
    GlobalTabletSchemaMap() = default;

    constexpr static int kShardSize = 16;

    struct MapShard {
        mutable std::mutex mtx;
        phmap::flat_hash_map<UniqueId, std::weak_ptr<const TabletSchema>> map;
    };

    MapShard* get_shard(UniqueId id) { return &_map_shards[id % kShardSize]; }

    MapShard _map_shards[kShardSize];
};

} // namespace starrocks
