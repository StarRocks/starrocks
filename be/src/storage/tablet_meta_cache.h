// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifndef STARROCKS_METACACHE_H
#define STARROCKS_METACACHE_H
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
    virtual StatusOr<TabletSharedPtr> get(int64_t tabletid) = 0;
    virtual Status put(int64_t tabletid, const TabletSharedPtr& tabletptr) = 0;
    virtual Status remove(int64_t tabletid) = 0;
};

class LRUTabletMetaCache : public TabletMetaCache {
    using TabletMap = std::unordered_map<int64_t, TabletSharedPtr>;

public:
    StatusOr<TabletSharedPtr> get(int64_t tabletid);
    Status put(int64_t tabletid, const TabletSharedPtr& tabletptr);
    Status remove(int64_t tabletid);

private:
    TabletMap tabletmap;
};

} // namespace starrocks
#endif //STARROCKS_METACACHE_H
