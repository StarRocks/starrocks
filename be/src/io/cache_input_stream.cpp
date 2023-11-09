// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "io/cache_input_stream.h"

#include <fmt/format.h>

#include <utility>

#include "block_cache/block_cache.h"
#include "util/hash_util.hpp"
#include "util/runtime_profile.h"
#include "util/stack_util.h"

namespace starrocks::io {

CacheInputStream::CacheInputStream(std::shared_ptr<SeekableInputStream> stream, const std::string& filename,
                                   size_t size, int64_t modification_time)
        : SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership),
          _filename(filename),
          _stream(stream),
          _offset(0),
          _size(size) {
#ifdef WITH_BLOCK_CACHE
    // _cache_key = _filename;
    // use hash(filename) as cache key.
    _cache_key.resize(12);
    char* data = _cache_key.data();
    uint64_t hash_value = HashUtil::hash64(filename.data(), filename.size(), 0);
    memcpy(data, &hash_value, sizeof(hash_value));
    // The modification time is more appropriate to indicate the different file versions.
    // While some data source, such as Hudi, have no modification time because their files
    // cannot be overwritten. So, if the modification time is unsupported, we use file size instead.
    // Usually the last modification timestamp has 41 bits, to reduce memory usage, we ignore the tail 9
    // bytes and choose the high 32 bits to represent the second timestamp.
    if (modification_time > 0) {
        uint32_t mtime_s = (modification_time >> 9) & 0x00000000FFFFFFFF;
        memcpy(data + 8, &mtime_s, sizeof(mtime_s));
    } else {
        uint32_t file_size = _size;
        memcpy(data + 8, &file_size, sizeof(file_size));
    }
    _buffer.reserve(BlockCache::instance()->block_size());
#endif
}

#ifdef WITH_BLOCK_CACHE
Status CacheInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    BlockCache* cache = BlockCache::instance();
    count = std::min(_size - offset, count);
    if (count < 0) {
        return Status::EndOfFile("");
    }
    const int64_t BLOCK_SIZE = cache->block_size();
    char* p = static_cast<char*>(out);
    char* pe = p + count;

    auto read_one_block = [&](size_t offset, size_t size) {
        StatusOr<size_t> res;

        DCHECK(size <= BLOCK_SIZE);
        {
            SCOPED_RAW_TIMER(&_stats.read_cache_ns);
            res = cache->read_cache(_cache_key, offset, size, p);
            if (res.ok()) {
                _stats.read_cache_count += 1;
                _stats.read_cache_bytes += size;
                p += size;
                return Status::OK();
            }
        }
        if (!res.status().is_not_found()) Status::NotFound("");
        DCHECK(res.status().is_not_found());

        int64_t block_id = offset / BLOCK_SIZE;
        int64_t block_offset = block_id * BLOCK_SIZE;
        int64_t shift = offset - block_offset;

        char* src = nullptr;
        bool can_zero_copy = false;
        if ((p + BLOCK_SIZE <= pe) && (shift == 0)) {
            can_zero_copy = true;
            src = p;
        } else {
            src = _buffer.data();
        }

        // if not found, read from stream and write back to cache.
        int64_t load_size = std::min(BLOCK_SIZE, _size - block_offset);
        RETURN_IF_ERROR(_stream->read_at_fully(block_offset, src, load_size));

        if (_enable_populate_cache) {
            SCOPED_RAW_TIMER(&_stats.write_cache_ns);
            Status r = cache->write_cache(_cache_key, block_offset, load_size, src);
            if (r.ok()) {
                _stats.write_cache_count += 1;
                _stats.write_cache_bytes += load_size;
            } else {
                _stats.write_cache_fail_count += 1;
                _stats.write_cache_fail_bytes += load_size;
                LOG(WARNING) << "write block cache failed, errmsg: " << r.get_error_msg();
                // Failed to write cache, but we can keep processing query.
            }
        }

        if (!can_zero_copy) {
            memcpy(p, src + shift, size);
        }
        p += size;
        return Status::OK();
    };

    int64_t end_offset = offset + count;
    int64_t start_block_id = offset / BLOCK_SIZE;
    int64_t end_block_id = (end_offset - 1) / BLOCK_SIZE;
    for (int64_t i = start_block_id; i <= end_block_id; i++) {
        size_t off = std::max(offset, i * BLOCK_SIZE);
        size_t end = std::min((i + 1) * BLOCK_SIZE, end_offset);
        size_t size = end - off;
        Status st = read_one_block(off, size);
        if (!st.ok()) return st;
        offset += size;
    }
    DCHECK(p == pe);
    return Status::OK();
}
#else
Status CacheInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    int64_t load_size = std::min(count, _size - offset);
    RETURN_IF_ERROR(_stream->read_at_fully(offset, out, load_size));
    return Status::OK();
}
#endif

StatusOr<int64_t> CacheInputStream::read(void* data, int64_t count) {
    count = std::min(_size - _offset, count);
    RETURN_IF_ERROR(read_at_fully(_offset, data, count));
    _offset += count;
    return count;
}

Status CacheInputStream::seek(int64_t offset) {
    if (offset < 0 || offset >= _size) return Status::InvalidArgument(fmt::format("Invalid offset {}", offset));
    _offset = offset;
    _stream->seek(offset);
    return Status::OK();
}

StatusOr<int64_t> CacheInputStream::position() {
    return _offset;
}

StatusOr<int64_t> CacheInputStream::get_size() {
    return _size;
}

int64_t CacheInputStream::get_align_size() const {
#ifdef WITH_BLOCK_CACHE
    BlockCache* cache = BlockCache::instance();
    return cache->block_size();
#else
    return 0;
#endif
}

StatusOr<std::string_view> CacheInputStream::peek(int64_t count) {
    // if app level uses zero copy read, it does bypass the cache layer.
    // so here we have to fill cache manually.
    ASSIGN_OR_RETURN(auto s, _stream->peek(count));
    if (_enable_populate_cache) {
        _populate_cache_from_zero_copy_buffer(s.data(), _offset, count);
    }
    return s;
}

#ifdef WITH_BLOCK_CACHE
void CacheInputStream::_populate_cache_from_zero_copy_buffer(const char* p, int64_t offset, int64_t count) {
    BlockCache* cache = BlockCache::instance();
    const int64_t BLOCK_SIZE = cache->block_size();
    int64_t begin = offset / BLOCK_SIZE * BLOCK_SIZE;
    int64_t end = std::min((offset + count + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE, _size);
    p -= (offset - begin);
    auto f = [&](const char* buf, size_t offset, size_t size) {
        StatusOr<size_t> res;
        // to support nullptr read.
        res = cache->read_cache(_cache_key, offset, size, nullptr);
        if (res.ok()) {
            return;
        }
        {
            SCOPED_RAW_TIMER(&_stats.write_cache_ns);
            Status r = cache->write_cache(_cache_key, offset, size, buf);
            if (r.ok()) {
                _stats.write_cache_count += 1;
                _stats.write_cache_bytes += size;
            } else {
                _stats.write_cache_fail_count += 1;
                _stats.write_cache_fail_bytes += size;
                LOG(WARNING) << "write block cache failed, errmsg: " << r.get_error_msg();
            }
        }
    };

    while (begin < end) {
        size_t size = std::min(BLOCK_SIZE, end - begin);
        f(p, begin, size);
        begin += size;
        p += size;
    }
    return;
}
#else
void CacheInputStream::_populate_cache_from_zero_copy_buffer(const char* p, int64_t offset, int64_t size) {
    return;
}
#endif

} // namespace starrocks::io
