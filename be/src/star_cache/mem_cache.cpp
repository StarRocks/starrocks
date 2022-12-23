// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/mem_cache.h"

#include <limits.h>
#include "common/logging.h"
#include "star_cache/util.h"
#include "star_cache/block_item.h"
#include "star_cache/mem_space_manager.h"
#include "star_cache/lru_eviction_policy.h"

namespace starrocks::starcache {

Status MemCache::init(const MemCacheOptions& options) {
    _space_manager = MemSpaceManager::GetInstance();
    _space_manager->add_cache_zone(nullptr, options.mem_quota_bytes);
    // TODO: This capacity is not accurate because we usually don't cache the whole block in memory
    // size_t capacity = _space_manager->quota_bytes() / config::FLAGS_block_size * 2;

    // In fact, we can provide an big enough capacity because we need to hold all items and evict 
    // them manully.
    // The maximum of `int32_t` is big enought, so we choose it instead of maximum of `size_t`, to
    // avoid overflow in some cases.
    size_t capacity = std::numeric_limits<int32_t>::max();
    _eviction_policy = new LruEvictionPolicy<BlockKey>(capacity);
    return Status::OK();
}

Status MemCache::write_block(const BlockKey& key, MemBlockItem* block, const std::vector<BlockSegment*>& segments) const {
    for (auto& seg : segments) {
        if (seg->buf.size() == 0) {
            continue;
        }
        int start_slice_index = off2slice(seg->offset);
        int end_slice_index = off2slice(seg->offset + seg->buf.size() - 1);
        set_block_segment(block, start_slice_index, end_slice_index, seg);
    }
    return Status::OK();
}

Status MemCache::read_block(const BlockKey& key, MemBlockItem* block, off_t offset, size_t size,
                            std::vector<BlockSegment>* segments) const {
    if (!block || size == 0) {
        return Status::OK();
    }

    _eviction_policy->touch(key);
    int start_slice_index = off2slice(offset);
    int end_slice_index = off2slice(offset + size - 1);
 
    BlockSegment* cur_segment = block->slices[start_slice_index];
    off_t segment_off = offset;
    for (int index = start_slice_index + 1; index <= end_slice_index; ++index) {
        if (block->slices[index] == cur_segment) {
            continue;
        }

        size_t length = slice_lower(index) - segment_off;
        if (cur_segment) {
            IOBuf buf;
            uint32_t pos = segment_off - cur_segment->offset;
            cur_segment->buf.append_to(&buf, length, pos);
            BlockSegment seg({ .offset = static_cast<uint32_t>(segment_off), .buf = buf});
            segments->emplace_back(std::move(seg));
        } 
        size -= length;
        cur_segment = block->slices[index];
        segment_off = slice_lower(index);
    }

    if (cur_segment) {
        IOBuf buf;
        uint32_t pos = segment_off - cur_segment->offset;
        cur_segment->buf.append_to(&buf, size, pos);
        BlockSegment seg({ .offset = static_cast<uint32_t>(segment_off), .buf = buf});
        segments->emplace_back(std::move(seg));
    } 
    return Status::OK();
}

MemBlockItem* MemCache::new_block_item(const BlockKey& key, BlockState state) const {
    return new MemBlockItem(state);
}

void MemCache::free_block_item(MemBlockItem* block) const {
    std::vector<BlockSegment*> segments;
    block->list_segments(&segments);
    for (auto seg : segments) {
        _space_manager->free_segment(seg);
    }
    memset(block->slices, 0, sizeof(BlockSegment*) * block_slice_count());
    delete block;
}

BlockSegment* MemCache::new_block_segment(off_t offset, const IOBuf& buf) const {
    auto segment = _space_manager->alloc_segment(buf.size());
    if (segment) {
        segment->offset = offset;
        segment->buf = buf;
    }
    return segment;
}

void MemCache::set_block_segment(MemBlockItem* block, int start_slice_index, int end_slice_index,
                                 BlockSegment* segment) const {
    for (int index = start_slice_index; index <= end_slice_index; ++index) {
        block->slices[index] = segment;
    }
}

void MemCache::free_block_segment(BlockSegment* segment) const {
    _space_manager->free_segment(segment);
}

void MemCache::evict_track(const BlockKey& key) const {
    _eviction_policy->add(key);
}

void MemCache::evict_untrack(const BlockKey& key) const {
    _eviction_policy->remove(key);
}

void MemCache::evict_for(const BlockKey& key, size_t count, std::vector<BlockKey>* evicted) const {
    _eviction_policy->evict_for(key, count, evicted);
}

} // namespace starrocks::starcache
