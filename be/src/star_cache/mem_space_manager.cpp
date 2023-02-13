// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/mem_space_manager.h"

#include <fmt/format.h>
#include "common/logging.h"
#include "star_cache/block_item.h"

namespace starrocks::starcache {

void MemSpaceManager::add_cache_zone(void* base_addr, size_t size) {
    // TODO: Add availble shared memory areas, and then we will allocate segment
    // from them.
    _quota_bytes += size;
}

bool MemSpaceManager::inc_mem(size_t size, bool urgent) {
    std::unique_lock<std::shared_mutex> wlck(_mutex);
    if (urgent && _private_quota_bytes >= size) {
        _private_quota_bytes -= size;
        _used_bytes += size;
        return true;
    }

    size_t public_quota_bytes = _quota_bytes - _private_quota_bytes;
    size_t upper_threshold = public_quota_bytes * 9 / 10;
    if (_used_bytes + size < upper_threshold) {
        _used_bytes += size;
        return true;
    }

    _need_bytes += size;
    return false;
}

void MemSpaceManager::dec_mem(size_t size) {
    // TODO: free the segment from shared memory area
    std::unique_lock<std::shared_mutex> wlck(_mutex);
    if (_need_bytes >= size) {
        _private_quota_bytes += size;
        _need_bytes -= size;
    } else {
        _private_quota_bytes += _need_bytes;
        _need_bytes = 0;
    }
    _used_bytes -= size;
}

} // namespace starrocks::starcache
