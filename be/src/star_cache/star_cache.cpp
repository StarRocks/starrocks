// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "star_cache/star_cache.h"

#include "common/logging.h"
#include "util/time.h"
#include "star_cache/util.h"
#include "star_cache/hashtable_access_index.h"
#include "star_cache/size_based_admission_policy.h"
#include "star_cache/capacity_based_promotion_policy.h"

#include "star_cache/mem_space_manager.h"

namespace starrocks::starcache {

StarCache::StarCache() {
    _mem_cache = std::make_unique<MemCache>();
    _disk_cache = std::make_unique<DiskCache>();
}

StarCache::~StarCache() {
    delete _access_index;
    delete _admission_policy;
    delete _promotion_policy;
}

Status StarCache::init(const CacheOptions& options) {
    MemCacheOptions mc_options;
    mc_options.mem_quota_bytes = options.mem_quota_bytes;
    RETURN_IF_ERROR(_mem_cache->init(mc_options));

    DiskCacheOptions dc_options;
    dc_options.disk_dir_spaces = options.disk_dir_spaces;
    RETURN_IF_ERROR(_disk_cache->init(dc_options));

    _access_index = new HashTableAccessIndex;

    SizeBasedAdmissionPolicy::Config ac_config;
    ac_config.max_check_size = config::FLAGS_admission_max_check_size;
    ac_config.flush_probability = config::FLAGS_admission_flush_probability;
    ac_config.delete_probability = config::FLAGS_admission_delete_probability;
    _admission_policy = new SizeBasedAdmissionPolicy(ac_config);

    CapacityBasedPromotionPolicy::Config pm_config;
    pm_config.mem_cap_threshold = config::FLAGS_promotion_mem_threshold;
    _promotion_policy = new CapacityBasedPromotionPolicy(pm_config);

    return Status::OK();
}

Status StarCache::set(const std::string& cache_key, const IOBuf& buf, uint64_t ttl_seconds) {
    if (buf.empty()) {
        return Status::InternalError("cache value should not be empty");
    }

    CacheId cache_id = cachekey2id(cache_key);
    CacheItemPtr cache_item = _access_index->find(cache_id);
    if (cache_item) {
        // TODO: Replace the target data directly.
        _remove_cache_item(cache_id, cache_item);
    }

    size_t block_size = config::FLAGS_block_size;
    size_t block_count = (buf.size()  - 1) / block_size + 1;
    uint64_t expire_time = MonotonicSeconds() + ttl_seconds;
    cache_item = std::make_shared<CacheItem>(cache_key, block_count, buf.size(), expire_time);

    uint32_t start_block_index = 0;
    uint32_t end_block_index = (buf.size() - 1) / block_size;
    for (uint32_t index = start_block_index; index <= end_block_index; ++index) {
        BlockKey block_key = { .cache_id = cache_id, .block_index = index };
        off_t offset = index * block_size;
        IOBuf seg_buf;
        if (block_count > 1) {
            buf.append_to(&seg_buf, block_size, offset);
        } else {
            seg_buf = buf;
        }
        RETURN_IF_ERROR(_write_block(cache_item, block_key, seg_buf));
    }

    _access_index->insert(cache_id, cache_item);
    return Status::OK();
}

Status StarCache::_write_block(CacheItemPtr cache_item, const BlockKey& block_key, const IOBuf& buf) {
    BlockItem& block = cache_item->blocks[block_key.block_index];
    auto location = _promotion_policy->check_write(cache_item, block_key);

    if (location == BlockLocation::MEM) {
        // Copy the buffer content to avoid the buffer to be released by users
        void* data = nullptr;
        if (!config::FLAGS_enable_os_page_cache) {
            data = align_buf(buf);
        } else {
            data = malloc(buf.size());
            buf.copy_to(data);
        }
        IOBuf sbuf;
        sbuf.append_user_data(data, buf.size(), nullptr);

        // allocate segment and evict
        auto segment = _alloc_block_segment(block_key, 0, sbuf);
        if (!segment) {
            return Status::InternalError("allocate segment failed");
        }
        auto mem_block = _mem_cache->new_block_item(block_key, BlockState::DIRTY);
        Status st = _mem_cache->write_block(block_key, mem_block, { segment });
        if(!st.ok() || !_set_mem_block(cache_item, block_key.block_index, mem_block)) {
            _mem_cache->free_block_segment(segment);
            delete mem_block;
            return Status::InternalError("the block is released");
        }
        _mem_cache->evict_track(block_key);

    } else if (location == BlockLocation::DISK) {
        auto disk_block = block.disk_block_item;
        if (disk_block) {
            return _disk_cache->write_block(block_key.cache_id, disk_block, 0, buf);
        }
        // allocate block and evict
        disk_block = _alloc_disk_block(block_key);
        if (!disk_block) {
            return Status::InternalError("allocate disk block failed");
        }

        Status st = _disk_cache->write_block(block_key.cache_id, disk_block, 0, buf);
        if (!st.ok()) {
            _disk_cache->free_block_item(disk_block);
            return st;
        }
         if (!_set_disk_block(cache_item, block_key.block_index, disk_block)) {
            _disk_cache->free_block_item(disk_block);
            return Status::InternalError("the object is released");
        }
        // If the cache has been added to eviction component before, the `add` operation will do nothing.
        _disk_cache->evict_track(block_key.cache_id);

    } else {
        return Status::InternalError("write block is rejected for overload");
    }

    return Status::OK();
}

Status StarCache::get(const std::string& cache_key, IOBuf* buf) {
    auto cache_id = cachekey2id(cache_key);
    auto cache_item = _access_index->find(cache_id);
    if (!cache_item || cache_item->cache_key != cache_key) {
        return Status::NotFound("The target not found");
    }
    if (cache_item->is_released()) {
        return Status::InternalError("the object is released");
    }
    return _read_cache_item(cache_id, cache_item, 0, cache_item->size, buf);
}

Status StarCache::read(const std::string& cache_key, off_t offset, size_t size, IOBuf* buf) {
    auto cache_id = cachekey2id(cache_key);
    auto cache_item = _access_index->find(cache_id);
    if (!cache_item || cache_item->cache_key != cache_key) {
        return Status::NotFound("The target not found");
    }
    if (cache_item->is_released()) {
        return Status::InternalError("the object is released");
    }
    return _read_cache_item(cache_id, cache_item, offset, size, buf);
}

Status StarCache::_read_cache_item(const CacheId& cache_id, CacheItemPtr cache_item, off_t offset,
                                   size_t size, IOBuf* buf) {
    if (offset + size > cache_item->size) {
        size = cache_item->size - offset;
    }
    if (size == 0) {
        return Status::OK();
    }
    
    // Align read range by slices
    // TODO: We have no need to align read range if for segments from mem cache,
    // but as the operation is zero copy, so it seems doesn't matter.
    off_t lower = slice_lower(off2slice(offset));
    off_t upper = slice_upper(off2slice(offset + size - 1));
    size_t aligned_size = upper - lower + 1;
    int start_block_index = off2block(lower);
    int end_block_index = off2block(upper);

    for (uint32_t i = start_block_index; i <= end_block_index; ++i) {
        IOBuf block_buf;
        size_t to_read = std::min(static_cast<size_t>(config::FLAGS_block_size), aligned_size);
        RETURN_IF_ERROR(_read_block(cache_item, { cache_id, i }, 0, to_read, &block_buf));
        aligned_size -= to_read;
        buf->append(block_buf);
    }

    buf->pop_front(offset - lower);
    if (buf->size() > size) {
        buf->pop_back(buf->size() - size);
    }
    return Status::OK();
}

Status StarCache::_read_block(CacheItemPtr cache_item, const BlockKey& block_key, off_t offset, size_t size,
                              IOBuf* buf) {
    BlockItem& block = cache_item->blocks[block_key.block_index];
    std::vector<BlockSegment> segments;
    _mem_cache->read_block(block_key, block.mem_block_item, offset, size, &segments);
    std::vector<BlockSegment> disk_segments;
    off_t cursor = offset;
    for (auto& seg : segments) {
        if (seg.offset > cursor) {
            IOBuf block_buf;
            RETURN_IF_ERROR(_disk_cache->read_block(block_key.cache_id, block.disk_block_item, cursor,
                                                    seg.offset - cursor, &block_buf));
            buf->append(block_buf);
            disk_segments.emplace_back(cursor, block_buf);
        }
        buf->append(seg.buf);
        cursor = seg.offset + seg.buf.size();
    }
    if (buf->size() < size) {
        IOBuf block_buf;
        RETURN_IF_ERROR(_disk_cache->read_block(block_key.cache_id, block.disk_block_item, cursor,
                                                size - buf->size(), &block_buf));
        buf->append(block_buf);
        disk_segments.emplace_back(cursor, block_buf);
    }

    // TODO: The follow procedure can be done asynchronously
    if (!disk_segments.empty() && _promotion_policy->check_promote(cache_item, block_key)) {
        _promote_block_segments(block_key, &block, disk_segments);
    }
    return Status::OK();
}

Status StarCache::remove(const std::string& cache_key) { 
    auto cache_id = cachekey2id(cache_key);
    auto cache_item = _access_index->find(cache_id);
    if (!cache_item || cache_item->cache_key != cache_key) {
        return Status::NotFound("The target not found");
    }
    if (!cache_item->release()) {
        // The cache item has been released, return ok.
        return Status::OK();
    }
    _remove_cache_item(cache_id, cache_item);
    return Status::OK();
}

void StarCache::_remove_cache_item(const CacheId& cache_id, CacheItemPtr cache_item) {
    for (size_t i = 0; i < cache_item->block_count; ++i) {
        _set_disk_block(cache_item, i, nullptr);
        _set_mem_block(cache_item, i, nullptr);
    }
    _access_index->remove(cache_id);
}

Status StarCache::_flush_block(const BlockKey& block_key, BlockItem* block) {
    CacheItemPtr cache_item = _access_index->find(block_key.cache_id);
    DCHECK(cache_item);
    auto admission = _admission_policy->check_admission(cache_item, block_key);
    if (admission == BlockAdmission::SKIP) {
        _mem_cache->evict_track(block_key);
        return Status::OK();
    } else if (admission == BlockAdmission::DELETE) {
        _set_mem_block(cache_item, block_key.block_index, nullptr);
        return Status::OK();
    }

    LOG(INFO) << "[Gavin] flush block: " << block_key;
    // admission == AdmissionPolicy::FLUSH
    Status st;
    do {
        auto disk_block = block->disk_block_item;
        if (!disk_block) {
            disk_block = _alloc_disk_block(block_key);
            if (!disk_block) {
                st = Status::InternalError("allocate disk block failed");
                break;
            }
        }

        std::vector<BlockSegment*> segments;
        block->mem_block_item->list_segments(&segments);
        for (auto seg : segments) {
            st = _disk_cache->write_block(block_key.cache_id, disk_block, seg->offset, seg->buf);
            if (!st.ok()) {
                _disk_cache->free_block_item(disk_block);
                LOG(WARNING) << "flush block failed: " << st.message();
                break;
            }
        }
        if (!st.ok()) {
            break;
        }

        if (!block->disk_block_item && !_set_disk_block(cache_item, block_key.block_index, disk_block)) {
            _disk_cache->free_block_item(disk_block);
            st = Status::InternalError("the block is released");
        }
        
    } while (0);

    _set_mem_block(cache_item, block_key.block_index, nullptr);
    if (st.ok()) {
        // If the cache has been added to eviction component before, the `add` operation will do nothing.
        _disk_cache->evict_track(block_key.cache_id);
    }
    return st;
}

void StarCache::_promote_block_segments(const BlockKey& block_key, BlockItem* block,
                                        const std::vector<BlockSegment>& segments) {
    std::vector<BlockSegment*> mem_segments;
    for (auto& seg : segments) {
        BlockSegment* segment = _alloc_block_segment(block_key, seg.offset, seg.buf);
        if (segment) {
            mem_segments.push_back(segment);
        }
    }
    if (mem_segments.empty()) {
        LOG(WARNING) << "skip promoting segments as allocate failed, block: " << block_key;
        return;
    }

    auto& mem_block = block->mem_block_item;
    if (!mem_block) {
        mem_block = _mem_cache->new_block_item(block_key, BlockState::CLEAN);
    }
    DCHECK(mem_block);
    _mem_cache->write_block(block_key, mem_block, mem_segments);
    _mem_cache->evict_track(block_key);
}

void StarCache::_evict_for_mem_block(const BlockKey& block_key) {
    std::vector<BlockKey> evicted;
    _mem_cache->evict_for(block_key, config::FLAGS_mem_evict_batch, &evicted);
    if (evicted.empty()) {
        auto space_mgr = MemSpaceManager::GetInstance();
        LOG(WARNING) << "evict no blocks for block: " << block_key
                     << ", quota: " << space_mgr->quota_bytes()
                     << ", used: " << space_mgr->used_bytes();
    }
    for (auto& key : evicted) {
        BlockItem* block = _get_block(key);
        if (!block) {
            continue;
        }
        if (block->mem_block_item->state == BlockState::DIRTY || !block->disk_block_item) {
            _flush_block(key, block);
        } else {
            CacheItemPtr cache_item = _access_index->find(key.cache_id);
            DCHECK(cache_item);
            _set_mem_block(cache_item, key.block_index, nullptr);
        }
    }
}

void StarCache::_evict_for_disk_block(const CacheId& cache_id) {
    std::vector<CacheId> evicted;
    _disk_cache->evict_for(cache_id, config::FLAGS_disk_evict_batch, &evicted);
    for (auto& cache_id : evicted) {
        // When evicting a cache from disk, we clean the memory segments together if exist,
        // because it may only contains incomplete data. In other hand, if a cache item
        // becomes cold in disk cache, it should also be treated as cold data in memory.
        CacheItemPtr cache_item = _access_index->find(cache_id);
        if (!cache_item) {
            return;
        }
        if (cache_item->release()) {
            _remove_cache_item(cache_id, cache_item);
        }
    }
}

BlockSegment* StarCache::_alloc_block_segment(const BlockKey& block_key, off_t offset, const IOBuf& buf) {
    // allocate segment and evict
    BlockSegment* segment = nullptr;
    for (size_t i = 0 ; i < config::FLAGS_max_retry_when_allocate; ++i) {
        segment = _mem_cache->new_block_segment(offset, buf);
        if (segment) {
            break;
        }
        _evict_for_mem_block(block_key);
    }
    LOG_IF(ERROR, !segment) << "allocate segment failed too many times for block: " << block_key;
    return segment;
}

DiskBlockItem* StarCache::_alloc_disk_block(const BlockKey& block_key) {
    // allocate block and evict
    const CacheId& cache_id = block_key.cache_id;
    DiskBlockItem* disk_block = nullptr;
    for (size_t i = 0 ; i < config::FLAGS_max_retry_when_allocate; ++i) {
        disk_block = _disk_cache->new_block_item(cache_id);
        if (disk_block) {
            break;
        }
        _evict_for_disk_block(cache_id);
    }
    LOG_IF(ERROR, !disk_block) << "allocate disk block failed too many times for block: " << block_key;
    return disk_block;
}

bool StarCache::_set_mem_block(CacheItemPtr cache_item, uint32_t block_index, MemBlockItem* mem_block) {
    MemBlockItem* old_mem_block = nullptr;
    if (!cache_item->set_mem_block(block_index, mem_block, &old_mem_block)) {
        return false;
    }
    if (old_mem_block) {
        _mem_cache->free_block_item(old_mem_block);
    }
    return true;
}

bool StarCache::_set_disk_block(CacheItemPtr cache_item, uint32_t block_index, DiskBlockItem* disk_block) {
    DiskBlockItem* old_disk_block = nullptr;
    if (!cache_item->set_disk_block(block_index, disk_block, &old_disk_block)) {
        return false;
    }
    if (old_disk_block) {
        _disk_cache->free_block_item(old_disk_block);
    }
    return true;
}

BlockItem* StarCache::_get_block(const BlockKey& block_key) {
    auto cache_item = _access_index->find(block_key.cache_id);
    if (!cache_item) {
        return nullptr;
    }
    return &(cache_item->blocks[block_key.block_index]);
}

} // namespace starrocks::starcache
