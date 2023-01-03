// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/mem_cache.h"

#include <limits.h>
#include "common/logging.h"
#include "star_cache/util.h"
#include "star_cache/cache_item.h"
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

Status MemCache::write_block(const BlockKey& key, MemBlockPtr block, const std::vector<BlockSegment*>& segments,
                             std::vector<BlockSegment*>* old_segments) const {
    for (auto& seg : segments) {
        if (seg->buf.size() == 0) {
            continue;
        }
        int start_slice_index = off2slice(seg->offset);
        int end_slice_index = off2slice(seg->offset + seg->buf.size() - 1);
        if (old_segments) {
            block->list_segments(start_slice_index, end_slice_index, old_segments);
        }
        set_block_segment(block, start_slice_index, end_slice_index, seg);
    }
    return Status::OK();
}

Status MemCache::read_block(const BlockKey& key, MemBlockPtr block, off_t offset, size_t size,
                            std::vector<BlockSegment>* segments) const {
    if (!block || size == 0) {
        return Status::OK();
    }

    auto handle = evict_touch(key);
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

CacheItemPtr MemCache::new_cache_item(const std::string& cache_key, uint32_t block_count, size_t size,
                                      uint64_t expire_time) {
    // We don't plus the disk block item and mem block item size here, as they may change during
    // the flushing and promotion.
    size_t item_size = sizeof(CacheItem) + cache_key.size() + sizeof(BlockItem) * block_count;
    if (!_space_manager->inc_mem(item_size)) {
        return nullptr;
    }

    CacheItemPtr cache_item(new CacheItem(cache_key, block_count, size, expire_time),
            [item_size, this](CacheItem* cache_item) {
                delete cache_item;
                _space_manager->dec_mem(item_size);
            });
    return cache_item;
}

MemBlockPtr MemCache::new_block_item(const BlockKey& key, BlockState state) const {
    MemBlockPtr block_item(new MemBlockItem(state),
            [this](MemBlockItem* block) {
                std::vector<BlockSegment*> segments;
                block->list_segments(&segments);
                free_block_segments(segments);
                memset(block->slices, 0, sizeof(BlockSegment*) * block_slice_count());
                delete block;
            });
    return block_item;
}

BlockSegment* MemCache::new_block_segment(off_t offset, const IOBuf& buf) const {
    if (!_space_manager->inc_mem(sizeof(BlockSegment) + buf.size())) {
        return nullptr;
    }
    BlockSegment* segment = new BlockSegment;
    segment->offset = offset;
    segment->buf = buf;
    return segment;
}

void MemCache::free_block_segment(BlockSegment* segment) const {
    size_t size = sizeof(BlockSegment) + segment->buf.size();
    delete segment;
    _space_manager->dec_mem(size);
}

void MemCache::free_block_segments(const std::vector<BlockSegment*> segments) const {
    for (auto seg : segments) {
        free_block_segment(seg); 
    }
}

void MemCache::set_block_segment(MemBlockPtr block, int start_slice_index, int end_slice_index,
                                 BlockSegment* segment) const {
    for (int index = start_slice_index; index <= end_slice_index; ++index) {
        block->slices[index] = segment;
    }
}

void MemCache::evict_track(const BlockKey& key) const {
    _eviction_policy->add(key);
}

void MemCache::evict_untrack(const BlockKey& key) const {
    _eviction_policy->remove(key);
}

EvictionPolicy<BlockKey>::HandlePtr MemCache::evict_touch(const BlockKey& key) const {
    return _eviction_policy->touch(key);
}

void MemCache::evict_for(const BlockKey& key, size_t count, std::vector<BlockKey>* evicted) const {
    _eviction_policy->evict_for(key, count, evicted);
}

void MemCache::evict(size_t count, std::vector<BlockKey>* evicted) const {
    _eviction_policy->evict(count, evicted);
}

} // namespace starrocks::starcache
