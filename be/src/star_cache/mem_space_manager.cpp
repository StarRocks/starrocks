// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/mem_space_manager.h"

#include <fmt/format.h>
#include "common/logging.h"
#include "common/config.h"
#include "star_cache/block_item.h"

namespace starrocks {

void MemSpaceManager::add_cache_zone(void* base_addr, size_t size) {
    // TODO: Add availble shared memory areas, and then we will allocate segment
    // from them.
    _quota_bytes += size;
}

BlockSegment* MemSpaceManager::alloc_segment(size_t size) {
    // TODO: allocate the segment from shared memory area
    if (_used_bytes + size > _quota_bytes * 9 / 10) {
        return nullptr;
    }
    BlockSegment* segment = new BlockSegment;
    segment->buf.resize(size);
    _used_bytes += size;
    return segment;
}

void MemSpaceManager::free_segment(BlockSegment* segment) {
    // TODO: free the segment from shared memory area
    _used_bytes -= segment->buf.size();
    delete segment;
}

} // namespace starrocks
