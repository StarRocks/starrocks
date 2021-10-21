// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/tablet_schema_map.h"

namespace starrocks {

std::shared_ptr<const TabletSchema> TabletSchemaMap::get_or_create(SchemaHash schema_hash,
                                                                   const TabletSchemaPB& schema_pb) {
    // We have to create a `TabletSchema` object for equality check.
    auto* schema_raw_ptr = new TabletSchema();
    schema_raw_ptr->init_from_pb(schema_pb);
    std::shared_ptr<const TabletSchema> schema_ptr(schema_raw_ptr);

    MapShard* shard = get_shard(schema_hash);
    std::lock_guard l(shard->mtx);

    auto it = shard->map.find(schema_hash);
    if (it == shard->map.end()) {
        shard->map.emplace(schema_hash, schema_ptr);
        return schema_ptr;
    } else {
        std::shared_ptr<const TabletSchema> old = it->second.lock();
        if (!old) {
            schema_raw_ptr->set_share_key(schema_hash);
            it->second = std::weak_ptr<const TabletSchema>(schema_ptr);
        } else if (*old == *schema_ptr) {
            schema_ptr = old;
        }
        return schema_ptr;
    }
}

void TabletSchemaMap::erase(SchemaHash schema_hash) {
    MapShard* shard = get_shard(schema_hash);
    std::lock_guard l(shard->mtx);

    shard->map.erase(schema_hash);
}

} // namespace starrocks
