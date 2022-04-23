// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/s3_uri.h"
#include "common/statusor.h"
#include "env/env.h"
#include "env/env_s3.h"
#include "storage/tablet.h"
#include "util/lru_cache.h"

namespace starrocks {

enum MetaCache_Type {
    METACACHE_LRU,
};

class TabletMetaCache {
public:
    virtual TabletSharedPtr get(int64_t tablet_id) = 0;
    virtual bool put(const TabletSharedPtr& tabletptr) = 0;
    virtual bool remove(int64_t tablet_id) = 0;
    virtual ~TabletMetaCache() = default;
};

class LRUTabletMetaCache : public TabletMetaCache {
    using TabletMap = std::unordered_map<int64_t, TabletSharedPtr>;

public:
    TabletSharedPtr get(int64_t tablet_id) override;
    bool put(const TabletSharedPtr& tabletptr) override;
    bool remove(int64_t tablet_id) override;

private:
    TabletMap tabletmap;
};

} // namespace starrocks
