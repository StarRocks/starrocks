// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/disk_cache.h"

#include <fmt/format.h>
#include "gutil/strings/substitute.h"
#include "common/logging.h"
#include "star_cache/utils.h"
#include "star_cache/block_item.h"
#include "star_cache/lru_eviction_policy.h"
#include "star_cache/disk_space_manager.h"

namespace starrocks::starcache {

Status DiskCache::init(const DiskCacheOptions& options) {
    _space_manager = DiskSpaceManager::GetInstance();
    for (auto& dir : options.disk_dir_spaces) {
        RETURN_IF_ERROR(_space_manager->add_cache_dir(dir));
    }

    // TODO: This capacity is not accurate because we evict with cache object unit, but
    // their size are different.
    // size_t capacity = _space_manager->quota_bytes() / config::FLAGS_block_size * 2;

    // In fact, we can provide an big enough capacity because we need to hold all items and evict 
    // them manully.
    // The maximum of `int32_t` is big enought, so we choose it instead of maximum of `size_t`, to
    // avoid overflow in some cases.
    size_t capacity = std::numeric_limits<int32_t>::max();

    _eviction_policy = new LruEvictionPolicy<CacheId>(capacity);
    return Status::OK();
}

Status DiskCache::write_block(const CacheId& cache_id, DiskBlockPtr block, const BlockSegment& segment) const {
    if (UNLIKELY(segment.offset % config::FLAGS_slice_size != 0)) {
        return Status::InvalidArgument(
                strings::Substitute("offset must be aligned by slice size $0", config::FLAGS_slice_size));
    }

    if (config::FLAGS_enable_disk_checksum) {
        _update_block_checksum(block, segment);
    }
    auto st =  _space_manager->write_block({ block->dir_index, block->block_index }, segment.offset, segment.buf);
    return st;
}

Status DiskCache::read_block(const CacheId& cache_id, DiskBlockPtr block, BlockSegment* segment) const {
    auto handle = evict_touch(cache_id);
    if (UNLIKELY(segment->offset % config::FLAGS_slice_size != 0)) {
        DCHECK(false);
        return Status::InvalidArgument(
                strings::Substitute("offset must be aligned by slice size $0", config::FLAGS_slice_size));
    }

    Status st = _space_manager->read_block({ block->dir_index, block->block_index }, segment->offset,
                                          segment->size, &(segment->buf));
    RETURN_IF_ERROR(st);

    if (UNLIKELY(config::FLAGS_enable_disk_checksum && !_check_block_checksum(block, *segment))) {
        return Status::DataQualityError(
                strings::Substitute("fail to verify checksum for cache: $0", cache_id));
    }
    return Status::OK();
}

/*
Status DiskCache::writev_block(const CacheId& cache_id, DiskBlockPtr block, off_t offset_in_block,
                               const std::vector<IOBuf*>& bufv) const {
    if (UNLIKELY(offset_in_block % config::FLAGS_slice_size != 0)) {
        return Status::InvalidArgument(
                strings::Substitute("offset must be aligned by slice size $0", config::FLAGS_slice_size));
    }

    off_t off = offset_in_block;
    for (auto& buf : bufv) {
        _update_block_checksum(block, off, *buf);
        off += buf->size();
    }
    return _space_manager->writev_block({ block->dir_index, block->block_index }, offset_in_block, bufv);
}

Status DiskCache::readv_block(const CacheId& cache_id, DiskBlockPtr block, off_t offset_in_block,
                              const std::vector<size_t> sizev, std::vector<IOBuf*>* bufv) const {
    auto handle = evict_touch(cache_id);

    if (UNLIKELY(offset_in_block % config::FLAGS_slice_size != 0)) {
        return Status::InvalidArgument(
                strings::Substitute("offset must be aligned by slice size $0", config::FLAGS_slice_size));
    }

    Status st = _space_manager->readv_block({ block->dir_index, block->block_index }, offset_in_block, sizev, bufv);
    RETURN_IF_ERROR(st);
    if (config::FLAGS_enable_disk_checksum) {
        for (auto& buf : *bufv) {
            if (UNLIKELY(!_check_block_checksum(block, offset_in_block, *buf))) {
                return Status::DataQualityError(
                        strings::Substitute("fail to verify checksum for cache: $0", cache_id));
            }
        }
    }
    return Status::OK();
}
*/

void DiskCache::_update_block_checksum(DiskBlockPtr block, const BlockSegment& segment) const {
    const uint32_t slice_size = config::FLAGS_slice_size;
    for (off_t off = 0; off < segment.size; off += slice_size) {
        int index = off2slice(segment.offset + off);
        size_t size = std::min(slice_size, static_cast<uint32_t>(segment.size - off));
        IOBuf slice_buf;
        segment.buf.append_to(&slice_buf, size, off);
        block->checksums[index] = crc32(slice_buf);
        STAR_VLOG << "update checksum for block: " << block.get()
                  << ", slice index: " << index
                  << ", checksum: " << block->checksums[index]
                  << ", buf size: " << slice_buf.size()
                  << ", head char: " << slice_buf.to_string()[0]
                  << ", file block index: " << block->block_index;
    }
}

bool DiskCache::_check_block_checksum(DiskBlockPtr block, const BlockSegment& segment) const {
    const uint32_t slice_size = config::FLAGS_slice_size;
    for (off_t off = 0; off < segment.size; off += slice_size) {
        int index = off2slice(segment.offset + off);
        size_t size = std::min(slice_size, static_cast<uint32_t>(segment.size - off));
        IOBuf slice_buf;
        segment.buf.append_to(&slice_buf, size, off);
        if (block->checksums[index] != crc32(slice_buf)) {
            LOG(ERROR) << "fail to check checksum for block: " << block.get()
                       << ", slice index: " << index
                       << ", expected: " << block->checksums[index]
                       << ", real: " << crc32(slice_buf)
                       << ", buf size: " << slice_buf.size()
                       << ", head char: " << slice_buf.to_string()[0]
                       << ", file block index: " << block->block_index;
            return false;
        }
    }
    return true;
}

DiskBlockPtr DiskCache::new_block_item(const CacheId& cache_id) const {
    BlockId block_id;
    Status st = _space_manager->alloc_block(&block_id);
    if (!st.ok()) {
        LOG(ERROR) << "allocate block failed: " << st.get_error_msg();
        return nullptr;
    }
    DiskBlockPtr block_item(new DiskBlockItem(block_id.dir_index, block_id.block_index),
          [this](DiskBlockItem* block) {
              BlockId block_id = { .dir_index = block->dir_index, .block_index = block->block_index };
              Status st = _space_manager->free_block(block_id);
              LOG_IF(ERROR, !st.ok()) << "free block failed" << st.get_error_msg();
              delete block;
          });
    return block_item;
}

void DiskCache::evict_track(const CacheId& id, size_t size) const {
    _eviction_policy->add(id, size);
}

void DiskCache::evict_untrack(const CacheId& id) const {
    _eviction_policy->remove(id);
}

EvictionPolicy<CacheId>::HandlePtr DiskCache::evict_touch(const CacheId& id) const {
    return _eviction_policy->touch(id);
}

void DiskCache::evict_for(const CacheId& id, size_t count, std::vector<CacheId>* evicted) const {
    _eviction_policy->evict_for(id, count, evicted);
}

void DiskCache::evict(size_t count, std::vector<CacheId>* evicted) const {
    _eviction_policy->evict(count, evicted);
}

} // namespace starrocks::starcache
