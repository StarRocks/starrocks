// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <shared_mutex>
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

    bool inc_mem(size_t size, bool urgent);
    void dec_mem(size_t size);

    size_t quota_bytes() const {
        return _quota_bytes;
    }
    size_t used_bytes() {
        std::shared_lock<std::shared_mutex> rlck(_mutex);
        return _used_bytes;
    }

private:
    MemSpaceManager() {}
    friend struct DefaultSingletonTraits<MemSpaceManager>;
    DISALLOW_COPY_AND_ASSIGN(MemSpaceManager);

    size_t _quota_bytes = 0;
    size_t _used_bytes = 0;

    size_t _private_quota_bytes = 0;
    int64_t _need_bytes = 0;

    std::shared_mutex _mutex;
};

} // namespace starrocks::starcache
