// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "star_cache/star_cache.h"

#include "common/logging.h"
#include "util/time.h"
#include "star_cache/utils.h"
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

Status StarCache::set(const CacheKey& cache_key, const IOBuf& buf, uint64_t ttl_seconds) {
    STAR_VLOG << "set cache, cache_key: " << cache_key  << ", buf size: " << buf.size()
              << ", ttl: " << ttl_seconds;
    if (buf.empty()) {
        return Status::InternalError("cache value should not be empty");
    }

    CacheId cache_id = cachekey2id(cache_key);
    CacheItemPtr cache_item = _access_index->find(cache_id);
    if (cache_item) {
        // TODO: Replace the target data directly.
        STAR_VLOG << "remove old cache for update, cache_key: " << cache_key;
        _remove_cache_item(cache_id, cache_item);
    }

    size_t block_size = config::FLAGS_block_size;
    size_t block_count = (buf.size()  - 1) / block_size + 1;
    uint64_t expire_time = MonotonicSeconds() + ttl_seconds;
    cache_item = _alloc_cache_item(cache_key, buf.size(), expire_time);

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
    STAR_VLOG << "set cache, cache_key: " << cache_key  << " success";
    return Status::OK();
}

Status StarCache::_write_block(CacheItemPtr cache_item, const BlockKey& block_key, const IOBuf& buf) {
    BlockItem& block = cache_item->blocks[block_key.block_index];
    auto wlck = block_unique_lock(block_key);
    auto location = _promotion_policy->check_write(cache_item, block_key);

    STAR_VLOG << "write block to " << location << ", cache_key: " << cache_item->cache_key
              << ", block_key: " << block_key << ", buf size: " << buf.size();
    if (location == BlockLocation::MEM) {
        // Copy the buffer content to avoid the buffer to be released by users
        void* data = nullptr;
        size_t size = buf.size();
        if (!config::FLAGS_enable_os_page_cache) {
            // We allocate an aligned buffer here to avoid repeatedlly copying data to a new aligned buffer 
            // when flush to disk file in `O_DIRECT` mode.
            // data = align_buf_old(buf);
            size = align_buf(buf, &data);
        } else {
            data = malloc(buf.size());
            buf.copy_to(data);
        }
        IOBuf sbuf;
        sbuf.append_user_data(data, size, nullptr);

        // allocate segment and evict
        auto segment = _alloc_block_segment(block_key, 0, buf.size(), sbuf);
        if (!segment) {
            return Status::InternalError("allocate segment failed");
        }
        auto mem_block = _mem_cache->new_block_item(block_key, BlockState::DIRTY, true);
        RETURN_IF_ERROR(_mem_cache->write_block(block_key, mem_block, { segment }));
        block.set_mem_block(mem_block, &wlck);
        _mem_cache->evict_track(block_key, _mem_cache->block_size(cache_item, block_key.block_index));
    } else if (location == BlockLocation::DISK) {
        STAR_VLOG << "write block to disk, cache_key: " << cache_item->cache_key << ", block_key: " << block_key
                  << ", buf size: " << buf.size();
        // allocate block and evict
        auto disk_block = _alloc_disk_block(block_key);
        if (!disk_block) {
            return Status::InternalError("allocate disk block failed");
        }
        BlockSegment segment(0, buf.size(), buf);
        RETURN_IF_ERROR(_disk_cache->write_block(block_key.cache_id, disk_block, segment));

        block.set_disk_block(disk_block, &wlck);
        // If the cache has been added to eviction component before, the `add` operation will do nothing.
        _disk_cache->evict_track(block_key.cache_id, cache_item->size);

    } else {
        LOG(ERROR) << "write block is rejected for overload, cache_key: " << cache_item->cache_key
                   << ", block_key: " << block_key;
        return Status::InternalError("write block is rejected for overload");
    }

    return Status::OK();
}

Status StarCache::get(const CacheKey& cache_key, IOBuf* buf) {
    STAR_VLOG << "get cache, cache_key: " << cache_key;
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

Status StarCache::read(const CacheKey& cache_key, off_t offset, size_t size, IOBuf* buf) {
    STAR_VLOG << "read cache, cache_key: " << cache_key << ", offset: " << offset << ", size: " << size;
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
    if (upper >= cache_item->size) {
	    upper = cache_item->size - 1;
    }
    size_t aligned_size = upper - lower + 1;
    int start_block_index = off2block(lower);
    int end_block_index = off2block(upper);

    for (uint32_t i = start_block_index; i <= end_block_index; ++i) {
        IOBuf block_buf;
        off_t off_in_block = i == start_block_index ? lower - block_lower(i) : 0;
        size_t to_read = i == end_block_index ? upper - (block_lower(i)  + off_in_block) + 1 :
                                                config::FLAGS_block_size - off_in_block;
        //if (off_in_block + to_read > cache_item->size) {
        //    to_read = cache_item->size - off_in_block; 
        //}
        RETURN_IF_ERROR(_read_block(cache_item, { cache_id, i }, off_in_block, to_read, &block_buf));
        aligned_size -= to_read;
        buf->append(block_buf);
    }

    buf->pop_front(offset - lower);
    if (buf->size() > size) {
        buf->pop_back(buf->size() - size);
    }
    STAR_VLOG << "read cache success, cache_key: " << cache_item->cache_key
              << ", offset:" << offset << ", size: " << size << ", buf_size: " << buf->size();
    return Status::OK();
}

Status StarCache::_read_block(CacheItemPtr cache_item, const BlockKey& block_key, off_t offset, size_t size,
                              IOBuf* buf) {
    BlockItem& block = cache_item->blocks[block_key.block_index];
    std::vector<BlockSegment> segments;
    std::vector<BlockSegment> disk_segments;
    auto rlck = block_shared_lock(block_key);
    rlck.lock();
    auto mem_block = block.mem_block();
    auto disk_block = block.disk_block();
    _mem_cache->read_block(block_key, mem_block, offset, size, &segments);
    off_t cursor = offset;

    for (auto& seg : segments) {
        if (seg.offset > cursor) {
            BlockSegment bs(cursor, seg.offset - cursor);
            RETURN_IF_ERROR(_disk_cache->read_block(block_key.cache_id, disk_block, &bs));
            bs.buf.append_to(buf, bs.size, 0);
            disk_segments.emplace_back(std::move(bs));
        }
        buf->append(seg.buf);
        cursor = seg.offset + seg.buf.size();
    }
    if (disk_block && buf->size() < size) {
        IOBuf block_buf;
        BlockSegment bs(cursor, size - buf->size());
        RETURN_IF_ERROR(_disk_cache->read_block(block_key.cache_id, disk_block, &bs));
        bs.buf.append_to(buf, bs.size, 0);
        disk_segments.emplace_back(std::move(bs));
    }
    rlck.unlock();

    // TODO: The follow procedure can be done asynchronously
    if (!disk_segments.empty() && _promotion_policy->check_promote(cache_item, block_key)) {
        _promote_block_segments(cache_item, block_key, disk_segments);
    }
    return Status::OK();
}

Status StarCache::remove(const CacheKey& cache_key) { 
    STAR_VLOG << "remove cache, cache_key: " << cache_key;
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
    STAR_VLOG << "remove cache success, cache_key: " << cache_key;
    return Status::OK();
}

void StarCache::_remove_cache_item(const CacheId& cache_id, CacheItemPtr cache_item) {
    for (uint32_t i = 0; i < cache_item->block_count(); ++i) {
        auto& block = cache_item->blocks[i];
        auto wlck = block_unique_lock({ cache_id, i });
        block.set_disk_block(nullptr, &wlck);
        block.set_mem_block(nullptr, &wlck);
    }
    _access_index->remove(cache_id);
}

Status StarCache::_try_flush_block(CacheItemPtr cache_item, const BlockKey& block_key) {
    if (cache_item->is_released()) {
        LOG(INFO) << "The block to flush is released, key: " << block_key;
        return Status::OK();
    }

    auto admission = _admission_policy->check_admission(cache_item, block_key);
    STAR_VLOG << "try to flush block, cache_key: " << cache_item->cache_key
              << ", block_key" << block_key << ", admisson policy: " << admission;
    if (admission == BlockAdmission::FLUSH) {
        return _flush_block(cache_item, block_key);
    } else if (admission == BlockAdmission::SKIP) {
        _mem_cache->evict_track(block_key, cache_item->block_size(block_key.block_index));
    } else {    // admission == BlockAdmission::DELETE
        if (cache_item->block_count() == 1 && cache_item->release()) {
            _remove_cache_item(block_key.cache_id, cache_item);
        } else {
            auto& block = cache_item->blocks[block_key.block_index];
            auto wlck = block_unique_lock(block_key);
            block.set_mem_block(nullptr, &wlck);
        }
    }
    return Status::OK();
}

Status StarCache::_flush_block(CacheItemPtr cache_item, const BlockKey& block_key) {
    auto& block = cache_item->blocks[block_key.block_index];
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

        std::vector<BlockSegmentPtr> segments;
        mem_block->list_segments(&segments);
        // For new disk block instance, we have no need to write block under lock
        // because the new disk block is invisible for other threads before `set_disk_block`.
        LOCK_IF(&wlck, !new_block);
        for (auto seg : segments) {
            st = _disk_cache->write_block(block_key.cache_id, disk_block, *seg);
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
        _disk_cache->evict_track(block_key.cache_id, cache_item->size);
    }
    return st;
}

void StarCache::_promote_block_segments(CacheItemPtr cache_item, const BlockKey& block_key,
                                        const std::vector<BlockSegment>& segments) {
    STAR_VLOG << "promote block segment, cache_key: " << cache_item->cache_key
              << ", block_key: " << block_key << ", segments count: " << segments.size()
              << ", seg.off: " << segments[0].offset << ", seg.size: " << segments[0].size;
    auto handle = _mem_cache->evict_touch(block_key);
    auto& block = cache_item->blocks[block_key.block_index];
    std::vector<BlockSegmentPtr> mem_segments;
    mem_segments.reserve(segments.size());
    for (auto& seg : segments) {
        auto segment = _alloc_block_segment(block_key, seg.offset, seg.size, seg.buf);
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
    Status st;
    if (!mem_block) {
        wlck.unlock();
        mem_block = _mem_cache->new_block_item(block_key, BlockState::CLEAN, false);
        DCHECK(mem_block);
        st = _mem_cache->write_block(block_key, mem_block, mem_segments);
        if (st.ok()) {
            block.set_mem_block(mem_block, &wlck);
        }
    } else {
        st = _mem_cache->write_block(block_key, mem_block, mem_segments);
        wlck.unlock();
    }
    if (!st.ok()) {
        LOG(ERROR) << "fail to write memery block, cache_key: " << cache_item->cache_key
                  << ", block_key: " << block_key << ", segments count: " << segments.size();
        return;
    }
    if (!handle) {
        _mem_cache->evict_track(block_key, cache_item->block_size(block_key.block_index));
    }
}

void StarCache::_evict_mem_block(size_t size) {
    std::vector<BlockKey> evicted;
    _mem_cache->evict(size * config::FLAGS_mem_evict_times, &evicted);
    if (evicted.empty()) {
        auto space_mgr = MemSpaceManager::GetInstance();
        LOG(WARNING) << "evict no blocks"
                     << ", quota: " << space_mgr->quota_bytes()
                     << ", used: " << space_mgr->used_bytes();
    }
    _process_evicted_mem_blocks(evicted);
}

void StarCache::_evict_for_mem_block(const BlockKey& block_key, size_t size) {
    std::vector<BlockKey> evicted;
    _mem_cache->evict_for(block_key, size * config::FLAGS_mem_evict_times, &evicted);
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
        // TODO: Is it needed to store the cache item to eviction component, to avoid
        // the following `find`?
        auto cache_item = _access_index->find(key.cache_id);
        if (!cache_item) {
            LOG(WARNING) << "block does not exist when evict " << key;
            continue;
        }
        STAR_VLOG << "evict mem block, cache_key: " << cache_item->cache_key
                  << ", block_key: " << key;
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
            _try_flush_block(cache_item, key);
        } else {
            block.set_mem_block(nullptr);
            wlck.unlock();
        }
    }
}

void StarCache::_evict_for_disk_block(const CacheId& cache_id, size_t size) {
    std::vector<CacheId> evicted;
    _disk_cache->evict_for(cache_id, size * config::FLAGS_disk_evict_times, &evicted);
    for (auto& cache_id : evicted) {
        // When evicting a cache from disk, we clean the memory segments together if exist,
        // because it may only contains incomplete data. In other hand, if a cache item
        // becomes cold in disk cache, it should also be treated as cold data in memory.
        //
        // TODO: Is it needed to store the cache item to eviction component, to avoid
        // the following `find`?
        CacheItemPtr cache_item = _access_index->find(cache_id);
        if (!cache_item) {
            return;
        }
        STAR_VLOG << "evict disk cache, cache_key: " << cache_item->cache_key;
        if (cache_item->release()) {
            _remove_cache_item(cache_id, cache_item);
        }
    }
}

CacheItemPtr StarCache::_alloc_cache_item(const CacheKey& cache_key, size_t size, uint64_t expire_time) {
    // allocate cache item and evict
    CacheItemPtr cache_item(nullptr);
    for (size_t i = 0 ; i < config::FLAGS_max_retry_when_allocate; ++i) {
        cache_item = _mem_cache->new_cache_item(cache_key, size, expire_time, i != 0);
        if (cache_item) {
            break;
        }
        _evict_mem_block(size);
    }
    LOG_IF(ERROR, !cache_item) << "allocate cache item failed too many times for cache: " << cache_key;
    return cache_item;
}

BlockSegmentPtr StarCache::_alloc_block_segment(const BlockKey& block_key, off_t offset, uint32_t size,
                                              const IOBuf& buf) {
    // allocate segment and evict
    BlockSegmentPtr segment = nullptr;
    for (size_t i = 0 ; i < config::FLAGS_max_retry_when_allocate; ++i) {
        segment = _mem_cache->new_block_segment(offset, size, buf, i != 0);
        if (segment) {
            break;
        }
        _evict_for_mem_block(block_key, size);
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
        _evict_for_disk_block(cache_id, config::FLAGS_block_size);
    }
    LOG_IF(ERROR, !disk_block) << "allocate disk block failed too many times for block: " << block_key;
    return disk_block;
}

} // namespace starrocks::starcache
