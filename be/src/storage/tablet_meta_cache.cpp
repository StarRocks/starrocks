// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/tablet_meta_cache.h"

#include "env/env_s3.h"

namespace starrocks {
TabletSharedPtr FullTabletMetaCache::get(int64_t tablet_id) {
    auto it = tabletmap.find(tablet_id);
    if (it != tabletmap.end()) {
        return it->second;
    }
    return nullptr;
}

bool FullTabletMetaCache::put(const TabletSharedPtr& meta) {
    auto [it, inserted] = tabletmap.emplace(meta->tablet_id(), meta);
    if (inserted) {
        return true;
    }
    return false;
}

bool FullTabletMetaCache::remove(int64_t tablet_id) {
    tabletmap.erase(tablet_id);
    return true;
}

} // namespace starrocks
