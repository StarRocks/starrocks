// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "star_cache/star_cache.h"

#include "common/logging.h"
#include "common/config.h"
#include "util/time.h"
#include "star_cache/util.h"
#include "star_cache/hashtable_access_index.h"

namespace starrocks {

StarCache::StarCache() {
    _mem_cache = std::make_unique<MemCache>();
    _disk_cache = std::make_unique<DiskCache>();
}

StarCache::~StarCache() {
    delete _access_index;
}

Status StarCache::init(const CacheOptions& options) {
    MemCacheOptions mc_options;
    mc_options.mem_quota_bytes = options.mem_quota_bytes;
    RETURN_IF_ERROR(_mem_cache->init(mc_options));

    DiskCacheOptions dc_options;
    dc_options.disk_dir_spaces = options.disk_dir_spaces;
    RETURN_IF_ERROR(_disk_cache->init(dc_options));

    _access_index = new HashTableAccessIndex;
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
        _access_index->remove(cache_id);
    }

    size_t block_size = config::star_cache_block_size;
    size_t block_count = (buf.size()  - 1) / block_size + 1;
    uint64_t expire_time = MonotonicSeconds() + ttl_seconds;
    cache_item = std::make_shared<CacheItem>(cache_key, block_count, buf.size(), expire_time);

    uint32_t start_block_index = 0;
    uint32_t end_block_index = (buf.size() - 1) / block_size;
    for (uint32_t index = start_block_index; index <= end_block_index; ++index) {
        // TODO: To check the promotion policy and decide where to write
        BlockKey block_key = { .cache_id = cache_id, .block_index = index };
        MemBlockItem* block = _mem_cache->new_block_item(block_key, BlockState::DIRTY);

        off_t offset = index * block_size;
        IOBuf seg_buf;
        if (block_count > 1) {
            buf.append_to(&seg_buf, block_size, offset);
        } else {
            seg_buf = buf;
        }
        // allocate segment and evict
        BlockSegment* segment = _alloc_block_segment(block_key, 0, seg_buf);
        if (!segment) {
            delete block;
            return Status::InternalError("allocate segment failed");
        }
        RETURN_IF_ERROR(_mem_cache->write_block(block_key, block, { segment }));
        if(!cache_item->set_mem_block_item(cache_item->blocks[index], block)) {
            delete block;
            return Status::InternalError("the block is released");
        }
    }

    _access_index->insert(cache_id, cache_item);
    return Status::OK();
}

Status StarCache::get(const std::string& cache_key, IOBuf* buf) {
    auto cache_id = cachekey2id(cache_key);
    auto cache_item = _access_index->find(cache_id);
    if (!cache_item || cache_item->cache_key != cache_key) {
        return Status::NotFound("The target not found");
    }
    return _read_cache_item(cache_id, cache_item, 0, cache_item->size, buf);
}

Status StarCache::read(const std::string& cache_key, off_t offset, size_t size, IOBuf* buf) {
    auto cache_id = cachekey2id(cache_key);
    auto cache_item = _access_index->find(cache_id);
    if (!cache_item || cache_item->cache_key != cache_key) {
        return Status::NotFound("The target not found");
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
        size_t to_read = std::min(static_cast<size_t>(config::star_cache_block_size), aligned_size);
        RETURN_IF_ERROR(_read_block({ cache_id, i }, &cache_item->blocks[i], 0, to_read, &block_buf));
        aligned_size -= to_read;
        buf->append(block_buf);
    }

    buf->pop_front(offset - lower);
    if (buf->size() > size) {
        buf->pop_back(buf->size() - size);
    }
    return Status::OK();
}

void StarCache::_free_cache_item(const CacheId& cache_id) {
    _access_index->remove(cache_id);
}

Status StarCache::_read_block(const BlockKey& block_key, BlockItem* block, off_t offset, size_t size,
                              IOBuf* buf) {
    std::vector<BlockSegment> segments;
    _mem_cache->read_block(block_key, block->mem_block_item, offset, size, &segments);
    std::vector<BlockSegment> disk_segments;
    off_t cursor = offset;
    for (auto& seg : segments) {
        if (seg.offset > cursor) {
            IOBuf block_buf;
            RETURN_IF_ERROR(_disk_cache->read_block(block_key.cache_id, block->disk_block_item, cursor,
                                                    seg.offset - cursor, &block_buf));
            buf->append(block_buf);
            disk_segments.emplace_back(cursor, block_buf);
        }
        buf->append(seg.buf);
        cursor = seg.offset + seg.buf.size();
    }
    if (buf->size() < size) {
        IOBuf block_buf;
        RETURN_IF_ERROR(_disk_cache->read_block(block_key.cache_id, block->disk_block_item, cursor,
                                                size - buf->size(), &block_buf));
        buf->append(block_buf);
        disk_segments.emplace_back(cursor, block_buf);
    }

    // TODO: The followe procedure can be done asynchronously
    _promote_block_segments(block_key, block, disk_segments);
    return Status::OK();
}

Status StarCache::_flush_block(const BlockKey& block_key, BlockItem* block) {
    DiskBlockItem* disk_block = block->disk_block_item;;
    if (!disk_block) {
        disk_block = _alloc_disk_block(block_key);
        if (!disk_block) {
            return Status::InternalError("allocate disk block failed");
        }
        CacheItemPtr cache_item = _access_index->find(block_key.cache_id);
        DCHECK(cache_item);
        if (!cache_item->set_disk_block_item(*block, disk_block)) {
            _disk_cache->free_block_item(disk_block);
            return Status::InternalError("the block is released");
        }
    }
    std::vector<BlockSegment*> segments;
    block->mem_block_item->list_segments(&segments);
    for (auto seg : segments) {
        Status st = _disk_cache->write_block(block_key.cache_id, disk_block, seg->offset, seg->buf);
        if (!st.ok()) {
            LOG(WARNING) << "flush block failed: " << st.message();
            return st;
        }
    }
    return Status::OK();
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
    auto& mem_block = block->mem_block_item;
    if (!mem_block) {
        mem_block = _mem_cache->new_block_item(block_key, BlockState::CLEAN);
    }
    DCHECK(mem_block);
    _mem_cache->write_block(block_key, mem_block, mem_segments);
}

void StarCache::_evict_for_mem_block(const BlockKey& block_key) {
    std::vector<BlockKey> evicted;
    _mem_cache->evict_for(block_key, config::star_cache_mem_evict_batch, &evicted);
    for (auto& key : evicted) {
        BlockItem* block = _get_block(key);
        if (!block) {
            continue;
        }
        if (block->mem_block_item->state == BlockState::DIRTY || !block->disk_block_item) {
            _flush_block(key, block);
        }
        _mem_cache->free_block_item(block->mem_block_item);
        block->mem_block_item = nullptr;
    }
}

void StarCache::_evict_for_disk_block(const CacheId& cache_id) {
    std::vector<CacheId> evicted;
    _disk_cache->evict_for(cache_id, config::star_cache_disk_evict_batch, &evicted);
    for (auto& cache_id : evicted) {
        CacheItemPtr cache = _access_index->find(cache_id);
        if (!cache) {
            continue;
        }
        bool released = cache->release();
        _clean_disk_cache(cache);
        if (!released) {
            continue;
        }
        _free_cache_item(cache_id);
    }
}

void StarCache::_clean_disk_cache(CacheItemPtr cache) {
    for (size_t i = 0; i < cache->block_count; ++i) {
        auto disk_block = cache->blocks[i].disk_block_item;
        if (disk_block) {
            _disk_cache->free_block_item(disk_block);
        }
    }
}

BlockSegment* StarCache::_alloc_block_segment(const BlockKey& block_key, off_t offset, const IOBuf& buf) {
    // allocate segment and evict
    BlockSegment* segment = nullptr;
    for (size_t i = 0 ; i < config::star_cache_max_retry_when_allocate; ++i) {
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
    for (size_t i = 0 ; i < config::star_cache_max_retry_when_allocate; ++i) {
        disk_block = _disk_cache->new_block_item(cache_id);
        if (disk_block) {
            break;
        }
        _evict_for_disk_block(cache_id);
    }
    LOG_IF(ERROR, !disk_block) << "allocate disk block failed too many times for block: " << block_key;
    return disk_block;
}

BlockItem* StarCache::_get_block(const BlockKey& block_key) {
    auto cache_item = _access_index->find(block_key.cache_id);
    if (!cache_item) {
        return nullptr;
    }
    return &(cache_item->blocks[block_key.block_index]);
}

} // namespace starrocks
