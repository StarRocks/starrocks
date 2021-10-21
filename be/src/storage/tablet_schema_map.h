// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>
#include <mutex>

#include "storage/tablet_schema.h"
#include "util/phmap/phmap.h"

namespace starrocks {

class TabletSchemaMap {
public:
    static TabletSchemaMap* Instance() {
        static TabletSchemaMap instance;
        return &instance;
    }

    std::shared_ptr<const TabletSchema> get_or_create(int32_t schema_hash, const TabletSchemaPB& schema_pb);

    void erase(SchemaHash schema_hash);

private:
    TabletSchemaMap() = default;

    constexpr static int kShardSize = 16;

    struct MapShard {
        std::mutex mtx;
        phmap::flat_hash_map<int32_t, std::weak_ptr<const TabletSchema>> map;
    };

    MapShard* get_shard(int32_t h) { return &_map_shards[h % kShardSize]; }

    MapShard _map_shards[kShardSize];
};

} // namespace starrocks
