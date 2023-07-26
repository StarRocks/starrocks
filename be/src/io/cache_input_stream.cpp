// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "io/cache_input_stream.h"

#include <fmt/format.h>

#include <utility>

#include "block_cache/block_cache.h"
#include "gutil/strings/fastmem.h"
#include "util/hash_util.hpp"
#include "util/runtime_profile.h"
#include "util/stack_util.h"

namespace starrocks::io {

<<<<<<< HEAD
CacheInputStream::CacheInputStream(std::shared_ptr<SeekableInputStream> stream, const std::string& filename,
                                   size_t size)
=======
CacheInputStream::CacheInputStream(const std::shared_ptr<SeekableInputStream>& stream, const std::string& filename,
                                   size_t size, int64_t modification_time)
>>>>>>> c45aff1aa5 ([Enhancement] Encode file modification time to data cache key. (#27755))
        : SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership),
          _filename(filename),
          _stream(stream),
          _offset(0),
          _size(size) {
    // _cache_key = _filename;
    // use hash(filename) as cache key.
    _cache_key.resize(12);
    char* data = _cache_key.data();
    uint64_t hash_value = HashUtil::hash64(filename.data(), filename.size(), 0);
    memcpy(data, &hash_value, sizeof(hash_value));
    // The modification time is more appropriate to indicate the different file versions.
    // While some data source, such as Hudi, have no modification time because their files
    // cannot be overwritten. So, if the modification time is unsupported, we use file size instead.
    // Also, to reduce memory usage, we only use the high four bytes to represent the second timestamp.
    if (modification_time > 0) {
        int32_t mtime_s = (modification_time >> 32) & 0x00000000FFFFFFFF;
        memcpy(data + 8, &mtime_s, sizeof(mtime_s));
    } else {
        int32_t file_size = _size;
        memcpy(data + 8, &file_size, sizeof(file_size));
    }
    _buffer.reserve(BlockCache::instance()->block_size());
}

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
        if (!res.status().is_not_found()) return res.status();
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
            // memcpy(p, src + shift, size);
            strings::memcpy_inlined(p, src + shift, size);
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

StatusOr<int64_t> CacheInputStream::read(void* data, int64_t count) {
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
    BlockCache* cache = BlockCache::instance();
    return cache->block_size();
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

void CacheInputStream::_populate_cache_from_zero_copy_buffer(const char* p, int64_t offset, int64_t count) {
    BlockCache* cache = BlockCache::instance();
    const int64_t BLOCK_SIZE = cache->block_size();
    int64_t begin = offset / BLOCK_SIZE * BLOCK_SIZE;
    int64_t end = std::min((offset + count + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE, _size);
    p -= (offset - begin);
    auto f = [&](const char* buf, size_t offset, size_t size) {
        SCOPED_RAW_TIMER(&_stats.write_cache_ns);
        Status r = cache->write_cache(_cache_key, offset, size, buf, 0, false);
        if (r.ok()) {
            _stats.write_cache_count += 1;
            _stats.write_cache_bytes += size;
        } else if (!r.is_already_exist()) {
            _stats.write_cache_fail_count += 1;
            _stats.write_cache_fail_bytes += size;
            LOG(WARNING) << "write block cache failed, errmsg: " << r.get_error_msg();
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

} // namespace starrocks::io
