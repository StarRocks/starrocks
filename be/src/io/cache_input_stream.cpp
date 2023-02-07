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

CacheInputStream::CacheInputStream(const std::string& filename, std::shared_ptr<SeekableInputStream> stream)
        : _filename(filename), _stream(std::move(stream)), _offset(0) {
    _size = _stream->get_size().value();
#ifdef WITH_BLOCK_CACHE
    // _cache_key = _filename;
    // use hash(filename) as cache key.
    _cache_key.resize(16);
    char* data = _cache_key.data();
    uint64_t hash_value = HashUtil::hash64(filename.data(), filename.size(), 0);
    memcpy(data, &hash_value, sizeof(hash_value));
    int64_t file_size = _size;
    memcpy(data + 8, &file_size, sizeof(file_size));
    _buffer.reserve(BlockCache::instance()->block_size());
#endif
}

#ifdef WITH_BLOCK_CACHE
StatusOr<int64_t> CacheInputStream::read(void* out, int64_t count) {
    BlockCache* cache = BlockCache::instance();
    count = std::min(_size - _offset, count);
    const int64_t BLOCK_SIZE = cache->block_size();
    char* p = static_cast<char*>(out);
    char* pe = p + count;

    auto read_one_block = [&](size_t offset, size_t size) {
        StatusOr<size_t> res;

        DCHECK(size <= BLOCK_SIZE);
        {
            SCOPED_RAW_TIMER(&_stats.read_cache_ns);
            res = cache->read_cache(_cache_key, _offset, size, p);
            if (res.ok()) {
                _stats.read_cache_count += 1;
                _stats.read_cache_bytes += size;
                p += size;
                _offset += size;
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
                LOG(WARNING) << "write block cache failed, errmsg: " << r.get_error_msg();
                // Failed to write cache, but we can keep processing query.
            }
        }

        if (!can_zero_copy) {
            // memcpy(p, src + shift, size);
            strings::memcpy_inlined(p, src + shift, size);
        }
        p += size;
        _offset += size;
        return Status::OK();
    };

    int64_t end_offset = _offset + count;
    int64_t start_block_id = _offset / BLOCK_SIZE;
    int64_t end_block_id = (end_offset - 1) / BLOCK_SIZE;
    // VLOG_FILE << "[XXX] CacheInputStream read _offset = " << _offset << ", count = " << count;
    for (int64_t i = start_block_id; i <= end_block_id; i++) {
        size_t off = std::max(_offset, i * BLOCK_SIZE);
        size_t end = std::min((i + 1) * BLOCK_SIZE, end_offset);
        size_t size = end - off;
        // VLOG_FILE << "[XXX] Cache inputStream read_one_block. off = " << off << ", size = " << size;
        Status st = read_one_block(off, size);
        if (!st.ok()) return st;
    }
    DCHECK(p == pe);
    return count;
}
#else
StatusOr<int64_t> CacheInputStream::read(void* out, int64_t count) {
    int64_t load_size = std::min(count, _size - _offset);
    RETURN_IF_ERROR(_stream->read_at_fully(_offset, out, load_size));
    _offset += load_size;
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
