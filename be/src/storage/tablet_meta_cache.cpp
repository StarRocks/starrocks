// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/tablet_meta_cache.h"

#include "env/env_s3.h"

namespace starrocks {
StatusOr<TabletSharedPtr> LRUTabletMetaCache::get(int64_t tabletid) {
    auto it = tabletmap.find(tabletid);
    if (it != tabletmap.end()) {
        return it->second;
    }
    return Status::NotFound("Not found");
}

Status LRUTabletMetaCache::put(int64_t tabletid, const TabletSharedPtr& meta) {
    auto [it, inserted] = tabletmap.emplace(tabletid, meta);
    if (inserted) {
        return Status::OK();
    }
    return Status::InternalError(fmt::format("tablet {} already exist in map", tabletid));
}

Status LRUTabletMetaCache::remove(int64_t tabletid) {
    tabletmap.erase(tabletid);
    return Status::OK();
}

} // namespace starrocks
