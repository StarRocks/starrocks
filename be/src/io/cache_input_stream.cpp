// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "io/cache_input_stream.h"

#include <fmt/format.h>

#include "cache/block_cache.h"
#include "util/hash_util.hpp"
#include "util/runtime_profile.h"

namespace starrocks::io {

void CacheInputStream::make_cache_key(uint64_t offset) {
    char* data = _cache_key.data();
    memcpy(data + 8, &offset, sizeof(offset));
}

CacheInputStream::CacheInputStream(const std::string& filename, std::shared_ptr<SeekableInputStream> stream)
        : _filename(filename), _stream(stream), _offset(0) {
    _cache_key.resize(16);
    char* data = _cache_key.data();
    uint64_t hash_value = HashUtil::hash64(filename.data(), filename.size(), 0);
    memcpy(data, &hash_value, sizeof(hash_value));
    _buffer.reserve(BLOCK_SIZE);
}

StatusOr<int64_t> CacheInputStream::read(void* out, int64_t count) {
    BlockCache* cache = BlockCache::instance();
    int64_t end = _offset + count;
    int64_t start_block_id = _offset / BLOCK_SIZE;
    int64_t end_block_id = (end - 1) / BLOCK_SIZE;

    char* p = static_cast<char*>(out);
    for (int64_t i = start_block_id; i <= end_block_id; i++) {
        int64_t off = i * BLOCK_SIZE;
        int64_t size = std::min(BLOCK_SIZE, end - off);
        make_cache_key(off);

        VLOG_FILE << "[CacheInputStream] offset = " << _offset << ", end = " << end << ", block_id = " << i
                  << ", off = " << off << ", size = " << size;

        StatusOr<size_t> st;

        {
            SCOPED_RAW_TIMER(&_stats.read_cache_ns);
            _stats.read_cache_count += 1;
            st = cache->read_cache(_cache_key, off, size, _buffer.data());
        }
        if (st.status().is_not_found()) {
            RETURN_IF_ERROR(_stream->read_at_fully(off, _buffer.data(), size));
            {
                SCOPED_RAW_TIMER(&_stats.write_cache_ns);
                _stats.write_cache_count += 1;
                _stats.write_cache_bytes += size;
                RETURN_IF_ERROR(cache->write_cache(_cache_key, off, size, _buffer.data()));
            }
        } else if (!st.ok()) {
            return st;
        } else {
            _stats.read_cache_bytes += size;
        }

        const char* src = _buffer.data();
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
