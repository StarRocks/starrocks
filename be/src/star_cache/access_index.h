// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include "common/status.h"
#include "star_cache/cache_item.h"

namespace starrocks {

class AccessIndex {
public:
    virtual ~AccessIndex() = default;

    virtual bool insert(const CacheId& id, const CacheItemPtr& item) = 0;
    // TODO: Maybe a interface with right reference is needed so that we
    // can move the item content directly.
    // virtual Status insert(const CacheId& id, CacheItem&& item) = 0;
    virtual CacheItemPtr find(const CacheId& id) = 0;
    virtual bool remove(const CacheId& id) = 0;
};

} // namespace starrocks
