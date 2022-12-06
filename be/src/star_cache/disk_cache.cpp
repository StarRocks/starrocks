// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/disk_cache.h"

#include <fmt/format.h>
#include "gutil/strings/substitute.h"
#include "common/logging.h"
#include "common/config.h"
#include "star_cache/util.h"
#include "star_cache/block_item.h"
#include "star_cache/lru_eviction_policy.h"
#include "star_cache/disk_space_manager.h"

namespace starrocks {

Status DiskCache::init(const DiskCacheOptions& options) {
    _space_manager = DiskSpaceManager::GetInstance();
    for (auto& dir : options.disk_dir_spaces) {
        RETURN_IF_ERROR(_space_manager->add_cache_dir(dir));
    }

    // TODO: This capacity is not accurate because we evict with cache object unit, but
    // their size are different.
    // In fact, we can provide an big enough capacity because we need to hold all items and evict 
    // them manully.
    size_t capacity = _space_manager->quota_bytes() / config::star_cache_block_size * 2;
    _eviction_policy = new LruEvictionPolicy<CacheId>(capacity);
    return Status::OK();
}

Status DiskCache::write_block(const CacheId& cache_id, DiskBlockItem* block, off_t offset_in_block,
                              const IOBuf& buf) const {
    if (UNLIKELY(offset_in_block % config::star_cache_slice_size != 0)) {
        return Status::InvalidArgument(
                strings::Substitute("offset must be aligned by slice size $0", config::star_cache_slice_size));
    }

    if (config::star_cache_checksum_enable) {
        _update_block_checksum(block, offset_in_block, buf);
    }
    return _space_manager->write_block({ block->dir_index, block->block_index }, offset_in_block, buf);
}

Status DiskCache::read_block(const CacheId& cache_id, DiskBlockItem* block, off_t offset_in_block, size_t size,
                             IOBuf* buf) const {
    _eviction_policy->touch(cache_id);
    if (UNLIKELY(offset_in_block % config::star_cache_slice_size != 0)) {
        return Status::InvalidArgument(
                strings::Substitute("offset must be aligned by slice size $0", config::star_cache_slice_size));
    }

    Status st = _space_manager->read_block({ block->dir_index, block->block_index }, offset_in_block, size, buf);
    RETURN_IF_ERROR(st);

    if (UNLIKELY(config::star_cache_checksum_enable && !_check_block_checksum(block, offset_in_block, *buf))) {
        return Status::DataQualityError(
                strings::Substitute("fail to verify checksum for cache: $0", cache_id));
    }
    return Status::OK();
}

Status DiskCache::writev_block(const CacheId& cache_id, DiskBlockItem* block, off_t offset_in_block,
                               const std::vector<IOBuf*>& bufv) const {
    _eviction_policy->touch(cache_id);
    if (UNLIKELY(offset_in_block % config::star_cache_slice_size != 0)) {
        return Status::InvalidArgument(
                strings::Substitute("offset must be aligned by slice size $0", config::star_cache_slice_size));
    }

    off_t off = offset_in_block;
    for (auto& buf : bufv) {
        _update_block_checksum(block, off, *buf);
        off += buf->size();
    }
    return _space_manager->writev_block({ block->dir_index, block->block_index }, offset_in_block, bufv);
}

Status DiskCache::readv_block(const CacheId& cache_id, DiskBlockItem* block, off_t offset_in_block,
                              const std::vector<size_t> sizev, std::vector<IOBuf*>* bufv) const {
    _eviction_policy->touch(cache_id);
    if (UNLIKELY(offset_in_block % config::star_cache_slice_size != 0)) {
        return Status::InvalidArgument(
                strings::Substitute("offset must be aligned by slice size $0", config::star_cache_slice_size));
    }

    Status st = _space_manager->readv_block({ block->dir_index, block->block_index }, offset_in_block, sizev, bufv);
    RETURN_IF_ERROR(st);
    if (config::star_cache_checksum_enable) {
        for (auto& buf : *bufv) {
            if (UNLIKELY(!_check_block_checksum(block, offset_in_block, *buf))) {
                return Status::DataQualityError(
                        strings::Substitute("fail to verify checksum for cache: $0", cache_id));
            }
        }
    }
    return Status::OK();
}

void DiskCache::_update_block_checksum(DiskBlockItem* block, off_t offset_in_block, const IOBuf& buf) const {
    const uint32_t slice_size = config::star_cache_slice_size;
    for (off_t off = 0; off < buf.size(); off += slice_size) {
        int index = off2slice(offset_in_block + off);
        IOBuf slice_buf;
        buf.append_to(&slice_buf, slice_size, off);
        block->checksums[index] = crc32(slice_buf);
    }
}

bool DiskCache::_check_block_checksum(DiskBlockItem* block, off_t offset_in_block, const IOBuf& buf) const {
    const uint32_t slice_size = config::star_cache_slice_size;
    for (off_t off = 0; off < buf.size(); off += slice_size) {
        int index = off2slice(offset_in_block + off);
        IOBuf slice_buf;
        buf.append_to(&slice_buf, slice_size, off);
        if (block->checksums[index] != crc32(slice_buf)) {
            return false;
        }
    }
    return true;
}

DiskBlockItem* DiskCache::new_block_item(const CacheId& cache_id) const {
    BlockId block_id;
    Status st = _space_manager->alloc_block(&block_id);
    if (!st.ok()) {
        LOG(ERROR) << "allocate block failed";
        return nullptr;
    }
    DiskBlockItem* block_item = new DiskBlockItem(block_id.dir_index, block_id.block_index);
    return block_item;
}

Status DiskCache::free_block_item(DiskBlockItem* block) {
    BlockId block_id = { .dir_index = block->dir_index, .block_index = block->block_index };
    return _space_manager->free_block(block_id);
}

void DiskCache::evict_track(const CacheId& id) const {
    _eviction_policy->add(id);
}

void DiskCache::evict_untrack(const CacheId& id) const {
    _eviction_policy->remove(id);
}

Status DiskCache::evict_for(const CacheId& id, size_t count, std::vector<CacheId>* evicted) const {
    _eviction_policy->evict_for(id, count, evicted);
    return Status::OK();
}

} // namespace starrocks
