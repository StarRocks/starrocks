// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>

#include "common/config.h"
#include "storage/tablet_schema.h"
#include "util/lru_cache.h"

namespace starrocks::lake {

using TabletSchemaPtr = std::shared_ptr<const starrocks::TabletSchema>;

class TabletSchemaCache {
public:
    explicit TabletSchemaCache(int capacity) : _schema_cache(new_lru_cache(capacity)){};

    bool fill_schema_cache(int64_t key, const TabletSchemaPtr& ptr, int mem_size);

    TabletSchemaPtr lookup_schema_cache(int64_t key);

    void erase_schema_cache(int64_t key);

    void prune_schema_cache();

private:
    std::unique_ptr<Cache> _schema_cache;
};

class GlobalTabletSchemaCache final : public TabletSchemaCache {
public:
    explicit GlobalTabletSchemaCache(int capacity) : TabletSchemaCache(capacity){};

    static GlobalTabletSchemaCache* Instance() {
        static GlobalTabletSchemaCache instance(config::tablet_schema_cache_size);
        return &instance;
    }
};

} // namespace starrocks::lake
