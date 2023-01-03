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

bool MemSpaceManager::inc_mem(size_t size) {
    static size_t upper_threshold = _quota_bytes * 9 / 10;
    size_t old_used_bytes = _used_bytes;
    size_t new_used_bytes = 0;
    do {
        new_used_bytes = old_used_bytes + size;
        if (new_used_bytes > upper_threshold) {
            return false;
        }
    } while (!_used_bytes.compare_exchange_weak(old_used_bytes, new_used_bytes));
    return true;
}

void MemSpaceManager::dec_mem(size_t size) {
    // TODO: free the segment from shared memory area
    _used_bytes -= size;
}

} // namespace starrocks::starcache
