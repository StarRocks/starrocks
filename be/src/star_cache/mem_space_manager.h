// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <butil/iobuf.h>
#include <butil/memory/singleton.h>

namespace starrocks::starcache {

class BlockSegment;

class MemSpaceManager {
public:
    static MemSpaceManager* GetInstance() {
        return Singleton<MemSpaceManager>::get();
    }

    void add_cache_zone(void* base_addr, size_t size);

    BlockSegment* alloc_segment(size_t size);
    void free_segment(BlockSegment* segment);

    size_t quota_bytes() const {
        return _quota_bytes;
    }

    size_t used_bytes() const {
        return _used_bytes;
    }

private:
    MemSpaceManager() {}
    friend struct DefaultSingletonTraits<MemSpaceManager>;
    DISALLOW_COPY_AND_ASSIGN(MemSpaceManager);

    size_t _quota_bytes = 0;
    std::atomic<size_t> _used_bytes = 0;
};

} // namespace starrocks::starcache
