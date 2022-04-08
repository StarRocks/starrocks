// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/s3_uri.h"
#include "common/statusor.h"
#include "env/env.h"
#include "env/env_s3.h"
#include "lru_cache.h"
#include "storage/tablet.h"

namespace starrocks {

enum MetaCache_Type {
    METACACHE_LRU,
};

class TabletMetaCache {
public:
    virtual TabletSharedPtr get(int64_t tablet_id) = 0;
    virtual bool put(const TabletSharedPtr& tabletptr) = 0;
    virtual bool remove(int64_t tablet_id) = 0;
};

class LRUTabletMetaCache : public TabletMetaCache {
    using TabletMap = std::unordered_map<int64_t, TabletSharedPtr>;

public:
    TabletSharedPtr get(int64_t tablet_id);
    bool put(const TabletSharedPtr& tabletptr);
    bool remove(int64_t tablet_id);

private:
    TabletMap tabletmap;
};

} // namespace starrocks
