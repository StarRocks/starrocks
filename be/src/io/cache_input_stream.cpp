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

// We use the `SharedBufferedInputStream` in `CacheInputStream` directly, because the we depend some functions of
// `SharedBufferedInputStream`.
// In fact, although the parameter is `SeekableInputStream` before, we only use `CacheInputStream` when using
// `SharedBufferedInputStream`. Also, if we don't set the io range for `SharedBufferedInputStream`, it will
// act as the old `DefaultInputStream`.
CacheInputStream::CacheInputStream(const std::shared_ptr<SharedBufferedInputStream>& stream,
                                   const std::string& filename, size_t size, int64_t modification_time)
        : SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership),
          _filename(filename),
          _sb_stream(stream),
          _offset(0),
          _size(size) {
    // _cache_key = _filename;
    // use hash(filename) as cache key.
    _block_size = BlockCache::instance()->block_size();
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
    _buffer.reserve(_block_size);
}

Status CacheInputStream::_read_block(int64_t offset, int64_t size, char* out, bool can_zero_copy) {
    DCHECK(size <= _block_size);
    int64_t block_id = offset / _block_size;

    // check block map
    auto iter = _block_map.find(block_id);
    if (iter != _block_map.end()) {
        auto& block = iter->second;
        block.buffer.copy_to(out, size, offset - block.offset);
        _stats.read_block_buffer_bytes += size;
        _stats.read_block_buffer_count += 1;
        return Status::OK();
    }

    // check shared buffer
    int64_t block_offset = block_id * _block_size;
    int64_t load_size = std::min(_block_size, _size - block_offset);
    int64_t shift = offset - block_offset;

    SharedBufferedInputStream::SharedBuffer* sb = nullptr;
    auto ret = _sb_stream->find_shared_buffer(offset, size);
    if (ret.ok()) {
        sb = ret.value();
        if (sb->buffer.capacity() > 0) {
            strings::memcpy_inlined(out, sb->buffer.data() + offset - sb->offset, size);
            _populate_cache_from_zero_copy_buffer((const char*)sb->buffer.data() + block_offset - sb->offset,
                                                  block_offset, load_size);
            return Status::OK();
        }
    }

    // read cache
    BlockCache* cache = BlockCache::instance();
    Status res;
    {
        SCOPED_RAW_TIMER(&_stats.read_cache_ns);
        BlockBuffer block;
        res = cache->read_cache(_cache_key, block_offset, load_size, &block.buffer);
        if (res.ok()) {
            block.buffer.copy_to(out, size, shift);
            block.offset = block_offset;
            _block_map[block_id] = block;
            _stats.read_cache_count += 1;
            _stats.read_cache_bytes += load_size;
            return Status::OK();
        }
    }
    if (!res.is_not_found()) return res;
    DCHECK(res.is_not_found());

    // read remote
    char* src = nullptr;
    if (sb) {
        // Duplicate the block ranges to avoid saving the same data both in cache and shared buffer.
        _deduplicate_shared_buffer(sb);
        const uint8_t* buffer = nullptr;
        RETURN_IF_ERROR(_sb_stream->get_bytes(&buffer, block_offset, load_size));
        strings::memcpy_inlined(out, buffer + shift, size);
        src = (char*)buffer;
    } else {
        if (!can_zero_copy || (shift != 0)) {
            can_zero_copy = false;
            src = _buffer.data();
        } else {
            src = out;
        }

        // if not found, read from stream and write back to cache.
        RETURN_IF_ERROR(_sb_stream->read_at_fully(block_offset, src, load_size));
        if (!can_zero_copy) {
            strings::memcpy_inlined(out, src + shift, size);
        }
    }

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
    return Status::OK();
}

void CacheInputStream::_deduplicate_shared_buffer(SharedBufferedInputStream::SharedBuffer* sb) {
    int64_t end_offset = sb->offset + sb->size;
    int64_t start_block_id = sb->offset / _block_size;
    int64_t end_block_id = (end_offset - 1) / _block_size;
    while (start_block_id < end_block_id) {
        if (_block_map.find(start_block_id) == _block_map.end()) {
            break;
        }
        ++start_block_id;
    }
    while (start_block_id < end_block_id) {
        if (_block_map.find(end_block_id) == _block_map.end()) {
            break;
        }
        --end_block_id;
    }
    // It is impossible that all block exists in block_map because we check block map before
    // reading remote storage.
    for (int64_t i = start_block_id; i <= end_block_id; ++i) {
        _block_map.erase(i);
    }

    sb->offset = std::max(start_block_id * _block_size, sb->offset);
    int64_t end = std::min((end_block_id + 1) * _block_size, end_offset);
    sb->size = end - sb->offset;
}

Status CacheInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    BlockCache* cache = BlockCache::instance();
    count = std::min(_size - offset, count);
    if (count < 0) {
        return Status::EndOfFile("");
    }
    const int64_t _block_size = cache->block_size();
    char* p = static_cast<char*>(out);
    char* pe = p + count;

    int64_t end_offset = offset + count;
    int64_t start_block_id = offset / _block_size;
    int64_t end_block_id = (end_offset - 1) / _block_size;
    bool can_zero_copy = p + _block_size < pe;
    for (int64_t i = start_block_id; i <= end_block_id; i++) {
        size_t off = std::max(offset, i * _block_size);
        size_t end = std::min((i + 1) * _block_size, end_offset);
        size_t size = end - off;
        Status st = _read_block(off, size, p, can_zero_copy);
        if (!st.ok()) return st;
        offset += size;
        p += size;
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
    _sb_stream->seek(offset);
    return Status::OK();
}

StatusOr<int64_t> CacheInputStream::position() {
    return _offset;
}

StatusOr<int64_t> CacheInputStream::get_size() {
    return _size;
}

int64_t CacheInputStream::get_align_size() const {
    return _block_size;
}

StatusOr<std::string_view> CacheInputStream::peek(int64_t count) {
    // if app level uses zero copy read, it does bypass the cache layer.
    // so here we have to fill cache manually.
    ASSIGN_OR_RETURN(auto s, _sb_stream->peek(count));
    if (_enable_populate_cache) {
        _populate_cache_from_zero_copy_buffer(s.data(), _offset, count);
    }
    return s;
}

void CacheInputStream::_populate_cache_from_zero_copy_buffer(const char* p, int64_t offset, int64_t count) {
    BlockCache* cache = BlockCache::instance();
    int64_t begin = offset / _block_size * _block_size;
    int64_t end = std::min((offset + count + _block_size - 1) / _block_size * _block_size, _size);
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
        size_t size = std::min(_block_size, end - begin);
        f(p, begin, size);
        begin += size;
        p += size;
    }
    return;
}

} // namespace starrocks::io
