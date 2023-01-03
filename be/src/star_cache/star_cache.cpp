// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "star_cache/star_cache.h"

#include "common/logging.h"
#include "util/time.h"
#include "star_cache/util.h"
#include "star_cache/hashtable_access_index.h"
#include "star_cache/size_based_admission_policy.h"
#include "star_cache/capacity_based_promotion_policy.h"
#include "star_cache/sharded_lock_manager.h"

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
    cache_item = _alloc_cache_item(cache_key, block_count, buf.size(), expire_time);

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
    auto wlck = block_unique_lock(block_key);
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
        Status st = _mem_cache->write_block(block_key, mem_block, { segment }, nullptr);
        if (!st.ok()) {
            _mem_cache->free_block_segment(segment);
            return st;
        }
        block.set_mem_block(mem_block, &wlck);
        _mem_cache->evict_track(block_key);
    } else if (location == BlockLocation::DISK) {
        // allocate block and evict
        auto disk_block = _alloc_disk_block(block_key);
        if (!disk_block) {
            return Status::InternalError("allocate disk block failed");
        }
        Status st = _disk_cache->write_block(block_key.cache_id, disk_block, 0, buf);
        RETURN_IF_ERROR(st);

        block.set_disk_block(disk_block, &wlck);
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
    auto wlck = block_unique_lock(block_key);
    auto rlck = block_shared_lock(block_key);
    auto mem_block = block.mem_block(&rlck);
    auto disk_block = block.disk_block(&rlck);
    std::vector<BlockSegment> segments;
    _mem_cache->read_block(block_key, mem_block, offset, size, &segments);
    std::vector<BlockSegment> disk_segments;
    off_t cursor = offset;

    wlck.lock();
    for (auto& seg : segments) {
        if (seg.offset > cursor) {
            IOBuf block_buf;
            RETURN_IF_ERROR(_disk_cache->read_block(block_key.cache_id, disk_block, cursor,
                                                    seg.offset - cursor, &block_buf));
            buf->append(block_buf);
            disk_segments.emplace_back(cursor, block_buf);
        }
        buf->append(seg.buf);
        cursor = seg.offset + seg.buf.size();
    }
    if (buf->size() < size) {
        IOBuf block_buf;
        RETURN_IF_ERROR(_disk_cache->read_block(block_key.cache_id, disk_block, cursor,
                                                size - buf->size(), &block_buf));
        buf->append(block_buf);
        disk_segments.emplace_back(cursor, block_buf);
    }
    wlck.unlock();

    // TODO: The follow procedure can be done asynchronously
    if (!disk_segments.empty() && _promotion_policy->check_promote(cache_item, block_key)) {
        _promote_block_segments(cache_item, block_key, disk_segments);
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
    for (uint32_t i = 0; i < cache_item->block_count; ++i) {
        auto& block = cache_item->blocks[i];
        auto wlck = block_unique_lock({ cache_id, i });
        block.set_disk_block(nullptr, &wlck);
        block.set_mem_block(nullptr, &wlck);
    }
    _access_index->remove(cache_id);
}

Status StarCache::_flush_block(CacheItemPtr cache_item, const BlockKey& block_key) {
    if (cache_item->is_released()) {
        LOG(INFO) << "The block to flush is released, key: " << block_key;
        return Status::OK();
    }

    auto& block = cache_item->blocks[block_key.block_index];
    auto admission = _admission_policy->check_admission(cache_item, block_key);
    if (admission == BlockAdmission::SKIP) {
        _mem_cache->evict_track(block_key);
        return Status::OK();
    } else if (admission == BlockAdmission::DELETE) {
        auto wlck = block_unique_lock(block_key);
        block.set_mem_block(nullptr, &wlck);
        return Status::OK();
    }

    // admission == AdmissionPolicy::FLUSH
    auto rlck = block_shared_lock(block_key);
    auto wlck = block_unique_lock(block_key);
    auto mem_block = block.mem_block(&rlck);
    Status st;

    bool new_block = false;
    do {
        auto disk_block = block.disk_block(&rlck);
        if (!disk_block) {
            disk_block = _alloc_disk_block(block_key);
            if (!disk_block) {
                st = Status::InternalError("allocate disk block failed");
                break;
            }
            new_block = true;
        }

        std::vector<BlockSegment*> segments;
        mem_block->list_segments(&segments);
        // For new disk block instance, we have no need to write block under lock
        // because the new disk block is invisible for other threads before `set_disk_block`.
        LOCK_IF(&wlck, !new_block);
        for (auto seg : segments) {
            st = _disk_cache->write_block(block_key.cache_id, disk_block, seg->offset, seg->buf);
            if (!st.ok()) {
                LOG(WARNING) << "flush block failed: " << st.message();
                break;
            }
        }
        if (!st.ok()) {
            UNLOCK_IF(&wlck, !new_block);
            break;
        }

        if (new_block) {
            block.set_disk_block(disk_block, &wlck); 
        } else {
            block.set_disk_block(disk_block);
            wlck.unlock();
        }
    } while (0);

    block.set_mem_block(nullptr, &wlck);
    if (st.ok()) {
        // If the cache has been added to eviction component before, the `add` operation will do nothing.
        _disk_cache->evict_track(block_key.cache_id);
    }
    return st;
}

void StarCache::_promote_block_segments(CacheItemPtr cache_item, const BlockKey& block_key,
                                        const std::vector<BlockSegment>& segments) {
    auto handle = _mem_cache->evict_touch(block_key);
    auto& block = cache_item->blocks[block_key.block_index];
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

    auto wlck = block_unique_lock(block_key);
    wlck.lock();

    auto mem_block = block.mem_block();
    // For new mem block instance, we have no need to write block under lock
    // because the new mem block is invisible for other threads before `set_mem_block`.
    if (!mem_block) {
        wlck.unlock();
        mem_block = _mem_cache->new_block_item(block_key, BlockState::CLEAN);
        DCHECK(mem_block);

        Status st = _mem_cache->write_block(block_key, mem_block, mem_segments, nullptr);
        if(!st.ok()) {
            _mem_cache->free_block_segments(mem_segments);
        }
        block.set_mem_block(mem_block, &wlck);
    } else {
        std::vector<BlockSegment*> old_segments;
        Status st = _mem_cache->write_block(block_key, mem_block, mem_segments, &old_segments);
        if(!st.ok()) {
            _mem_cache->free_block_segments(mem_segments);
        }
        block.set_mem_block(mem_block);
        wlck.unlock();
        _mem_cache->free_block_segments(old_segments);
    }
    if (!handle) {
        _mem_cache->evict_track(block_key);
    }
}

void StarCache::_evict_mem_block() {
    std::vector<BlockKey> evicted;
    _mem_cache->evict(config::FLAGS_mem_evict_batch, &evicted);
    if (evicted.empty()) {
        auto space_mgr = MemSpaceManager::GetInstance();
        LOG(WARNING) << "evict no blocks"
                     << ", quota: " << space_mgr->quota_bytes()
                     << ", used: " << space_mgr->used_bytes();
    }
    _process_evicted_mem_blocks(evicted);
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
    _process_evicted_mem_blocks(evicted);
}

void StarCache::_process_evicted_mem_blocks(const std::vector<BlockKey>& evicted) {
    for (auto& key : evicted) {
        auto cache_item = _access_index->find(key.cache_id);
        if (!cache_item) {
            LOG(WARNING) << "block does not exist when evict " << key;
            continue;
        }
        auto& block = cache_item->blocks[key.block_index];
        auto wlck = block_unique_lock(key);
        wlck.lock();
        auto mem_block = block.mem_block();
        if (!mem_block) {
            wlck.unlock();
            continue;
        }
        if (mem_block->state == BlockState::DIRTY || !block.disk_block()) {
            wlck.unlock();
            _flush_block(cache_item, key);
        } else {
            block.set_mem_block(nullptr);
            wlck.unlock();
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

CacheItemPtr StarCache::_alloc_cache_item(const std::string& cache_key, uint32_t block_count, size_t size,
                                          uint64_t expire_time) {
    // allocate cache item and evict
    CacheItemPtr cache_item(nullptr);
    for (size_t i = 0 ; i < config::FLAGS_max_retry_when_allocate; ++i) {
        cache_item = _mem_cache->new_cache_item(cache_key, block_count, size, expire_time);
        if (cache_item) {
            break;
        }
        _evict_mem_block();
    }
    LOG_IF(ERROR, !cache_item) << "allocate cache item failed too many times for cache: " << cache_key;
    return cache_item;
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

DiskBlockPtr StarCache::_alloc_disk_block(const BlockKey& block_key) {
    // allocate block and evict
    const CacheId& cache_id = block_key.cache_id;
    DiskBlockPtr disk_block(nullptr);
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

} // namespace starrocks::starcache
