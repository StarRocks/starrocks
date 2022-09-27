// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "io/cache_input_stream.h"

#include <fmt/format.h>

#include "block_cache/block_cache.h"
#include "util/hash_util.hpp"
#include "util/runtime_profile.h"

namespace starrocks::io {

CacheInputStream::CacheInputStream(const std::string& filename, std::shared_ptr<SeekableInputStream> stream)
        : _filename(filename), _stream(stream), _offset(0) {
    _size = _stream->get_size().value();
#ifdef WITH_BLOCK_CACHE
    // _cache_key = _filename;
    // use hash(filename) as cache key.
    _cache_key.resize(8);
    char* data = _cache_key.data();
    uint64_t hash_value = HashUtil::hash64(filename.data(), filename.size(), 0);
    memcpy(data, &hash_value, sizeof(hash_value));
    _buffer.reserve(BlockCache::instance()->block_size());
#endif
}

#ifdef WITH_BLOCK_CACHE
StatusOr<int64_t> CacheInputStream::read(void* out, int64_t count) {
    BlockCache* cache = BlockCache::instance();
    const int64_t BLOCK_SIZE = cache->block_size();
    int64_t end = _offset + count;
    int64_t start_block_id = _offset / BLOCK_SIZE;
    int64_t end_block_id = (end - 1) / BLOCK_SIZE;

    char* p = static_cast<char*>(out);
    for (int64_t i = start_block_id; i <= end_block_id; i++) {
        int64_t off = i * BLOCK_SIZE;
        int64_t size = std::min(BLOCK_SIZE, end - off);
        int64_t load_size = std::min(BLOCK_SIZE, _size - off);

        // VLOG_FILE << "[CacheInputStream] offset = " << _offset << ", end = " << end << ", block_id = " << i
        //           << ", off = " << off << ", size = " << size << " , load_size = " << load_size;

        StatusOr<size_t> res;
        char* src = nullptr;
        // try read data from cache.
        {
            SCOPED_RAW_TIMER(&_stats.read_cache_ns);
            res = cache->read_cache(_cache_key, off, load_size, _buffer.data());
            if (res.ok()) {
                src = _buffer.data();
                _stats.read_cache_count += 1;
            }
            // TODO: Replace the above with a safe zero copy interface
            //st = cache->read_cache_zero_copy(_cache_key, off, load_size, (const char**)&src);
        }
        // if not found, read from stream and write back to cache.
        if (res.status().is_not_found()) {
            RETURN_IF_ERROR(_stream->read_at_fully(off, _buffer.data(), load_size));
            {
                SCOPED_RAW_TIMER(&_stats.write_cache_ns);
                Status r = cache->write_cache(_cache_key, off, load_size, _buffer.data());
                if (r.ok()) {
                    _stats.write_cache_count += 1;
                    _stats.write_cache_bytes += load_size;
                } else {
                    LOG(WARNING) << "write block cache failed, errmsg: " << r.get_error_msg();
                }
            }
            src = _buffer.data();
        } else if (!res.ok()) {
            return res;
        } else {
            _stats.read_cache_bytes += size;
        }
        // handle data alignment for first block
        if (i == start_block_id) {
            int64_t shift = _offset - start_block_id * BLOCK_SIZE;
            DCHECK(size > shift);
            src += shift;
            size -= shift;
        }
        memcpy(p, src, size);
        p += size;
    }
    return count;
}
#else
StatusOr<int64_t> CacheInputStream::read(void* out, int64_t count) {
    int64_t load_size = std::min(count, _size - _offset);
    RETURN_IF_ERROR(_stream->read_at_fully(_offset, out, load_size));
    return load_size;
}
#endif

Status CacheInputStream::seek(int64_t offset) {
    if (offset < 0) return Status::InvalidArgument(fmt::format("Invalid offset {}", offset));
    _offset = offset;
    return Status::OK();
}

StatusOr<int64_t> CacheInputStream::position() {
    return _offset;
}

StatusOr<int64_t> CacheInputStream::get_size() {
    return _stream->get_size();
}

} // namespace starrocks::io
