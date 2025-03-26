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
    _cache = BlockCache::instance();
    _block_size = _cache->block_size();

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
    // default _buffer size is 4MB = (16 * 256KB)
    _buffer_size = 16 * _block_size;
    _buffer.reserve(_buffer_size);
}

CacheInputStream::~CacheInputStream() = default;

Status CacheInputStream::_read_block_from_local(const int64_t offset, const int64_t size, char* out) {
    if (UNLIKELY(size == 0)) {
        return Status::OK();
    }
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

    SharedBufferPtr sb = nullptr;
    {
        // try to find data from shared buffer
        auto ret = _sb_stream->find_shared_buffer(offset, size);
        if (ret.ok()) {
            sb = ret.value();
            if (sb->buffer.capacity() > 0) {
                strings::memcpy_inlined(out, sb->buffer.data() + offset - sb->offset, size);
                if (_enable_populate_cache) {
                    _populate_to_cache((const char*)sb->buffer.data() + block_offset - sb->offset, block_offset,
                                       load_size, sb);
                }
                return Status::OK();
            }
        }
    }

    // read cache
    Status res;
    int64_t read_cache_ns = 0;
    BlockBuffer block;
    ReadCacheOptions options;
    size_t read_size = 0;
    {
        options.use_adaptor = _enable_cache_io_adaptor;
        SCOPED_RAW_TIMER(&read_cache_ns);
        if (_enable_block_buffer) {
            res = _cache->read(_cache_key, block_offset, load_size, &block.buffer, &options);
            read_size = load_size;
        } else {
            StatusOr<size_t> r = _cache->read(_cache_key, offset, size, out, &options);
            res = r.status();
            read_size = size;
        }
    }

    // ok() or is_resource_busy() means block is already cached
    if (res.ok() || res.is_resource_busy()) {
        _already_populated_blocks.emplace(block_id);
    }

    if (res.ok()) {
        if (_enable_block_buffer) {
            block.buffer.copy_to(out, size, shift);
            block.offset = block_offset;
            _block_map[block_id] = block;
        }
        _stats.read_cache_bytes += read_size;
        _stats.read_cache_count += 1;
        _stats.read_mem_cache_bytes += options.stats.read_mem_bytes;
        _stats.read_disk_cache_bytes += options.stats.read_disk_bytes;
        _stats.read_cache_ns += read_cache_ns;
        if (_enable_cache_io_adaptor) {
            _cache->record_read_cache(read_size, read_cache_ns / 1000);
        }
    } else if (res.is_resource_busy()) {
        _stats.skip_read_cache_count += 1;
        _stats.skip_read_cache_bytes += read_size;
    }

    return res;
}

Status CacheInputStream::_read_blocks_from_remote(const int64_t offset, const int64_t size, char* out) {
    const int64_t start_block_id = offset / _block_size;
    const int64_t end_block_id = (offset + size - 1) / _block_size;

    // We will load range=[read_start_offset, read_end_offset) from remote
    const int64_t block_start_offset = start_block_id * _block_size;
    const int64_t block_end_offset = std::min(end_block_id * _block_size + _block_size, _size);

    // cursors for `out`
    int64_t out_offset_cursor = offset;
    int64_t out_remain_size = size;
    char* out_pointer_cursor = out;

    for (int64_t read_offset_cursor = block_start_offset; read_offset_cursor < block_end_offset;) {
        // Everytime read at most one buffer size
        const int64_t read_size = std::min(_buffer_size, block_end_offset - read_offset_cursor);
        char* src = nullptr;

        // check [read_offset_cursor, read_size) is already in SharedBuffer
        // If existed, we can use zero copy to avoid copy data from SharedBuffer to _buffer
        SharedBufferPtr sb = nullptr;
        int64_t read_remote_ns = 0;
        int64_t previous_remote_bytes = _sb_stream->shared_io_bytes() + _sb_stream->direct_io_bytes();
        {
            SCOPED_RAW_TIMER(&read_remote_ns);
            auto ret = _sb_stream->find_shared_buffer(read_offset_cursor, read_size);
            if (ret.ok()) {
                sb = ret.value();
                const uint8_t* buffer = nullptr;
                RETURN_IF_ERROR(_sb_stream->get_bytes(&buffer, read_offset_cursor, read_size, sb));
                src = (char*)buffer;
            } else {
                RETURN_IF_ERROR(_sb_stream->read_at_fully(read_offset_cursor, _buffer.data(), read_size));
                src = _buffer.data();
            }
        }

        if (sb) {
            // Duplicate the block ranges to avoid saving the same data both in block_map and shared buffer.
            _deduplicate_shared_buffer(sb);
        }

        if (_enable_cache_io_adaptor) {
            int64_t delta_remote_bytes =
                    _sb_stream->shared_io_bytes() + _sb_stream->direct_io_bytes() - previous_remote_bytes;
            if (delta_remote_bytes > 0) {
                _cache->record_read_remote(_block_size,
                                           _calculate_remote_latency_per_block(delta_remote_bytes, read_remote_ns));
            }
        }

        // write _buffer's data into `out`
        const int64_t shift = out_offset_cursor - read_offset_cursor;
        const int64_t out_size = std::min(read_size - shift, out_remain_size);
        if (out_size > 0) {
            strings::memcpy_inlined(out_pointer_cursor, src + shift, out_size);

            // cursor for `out`
            out_offset_cursor += out_size;
            out_pointer_cursor += out_size;
            out_remain_size -= out_size;
        }

        if (_enable_populate_cache) {
            _populate_to_cache(src, read_offset_cursor, read_size, sb);
        }

        read_offset_cursor += read_size;
    }
    DCHECK_EQ(0, out_remain_size);
    DCHECK_EQ(offset + size, out_offset_cursor);
    DCHECK_EQ(out + size, out_pointer_cursor);
    return Status::OK();
}

void CacheInputStream::_deduplicate_shared_buffer(const SharedBufferPtr& sb) {
    if (sb->size == 0 || _block_map.empty()) {
        return;
    }
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

    if (sb->buffer.capacity() == 0) {
        // shared buffer is empty, don't need to deduplicate block map
        sb->offset = std::max(start_block_id * _block_size, sb->offset);
        int64_t end = std::min((end_block_id + 1) * _block_size, end_offset);
        sb->size = end - sb->offset;
    } else {
        // It is impossible that all block exists in block_map because we check block map before
        // reading remote storage.
        for (int64_t i = start_block_id; i <= end_block_id; ++i) {
            _block_map.erase(i);
        }
    }
}

struct ReadFromRemoteIORange {
    ReadFromRemoteIORange(const int64_t offset, char* write_pointer, const int64_t size)
            : offset(offset), write_pointer(write_pointer), size(size) {}
    const int64_t offset;
    char* write_pointer;
    const int64_t size;
};

Status CacheInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    const int64_t origin_offset = offset;
    count = std::min(_size - offset, count);
    if (count < 0) {
        return Status::EndOfFile("");
    }
    const int64_t end_offset = offset + count;

    char* p = static_cast<char*>(out);
    char* pe = p + count;

    const int64_t start_block_id = offset / _block_size;
    const int64_t end_block_id = (end_offset - 1) / _block_size;

    std::vector<ReadFromRemoteIORange> need_read_from_remote{};

    for (int64_t i = start_block_id; i <= end_block_id; i++) {
        size_t off = std::max(offset, i * _block_size);
        size_t end = std::min((i + 1) * _block_size, end_offset);
        size_t size = end - off;
        Status st = _read_block_from_local(off, size, p);
        if (st.is_not_found() || st.is_resource_busy()) {
            // Not found block from local or disk is busy, we need to load it from remote
            need_read_from_remote.emplace_back(off, p, size);
        } else if (!st.ok()) {
            return st;
        }
        offset += size;
        p += size;
    }
    DCHECK(p == pe);

    if (need_read_from_remote.size() == 0) {
        return Status::OK();
    }

    // Merged multiple continous io range into one big io range
    std::vector<ReadFromRemoteIORange> merged_need_read_from_remote{};
    auto do_merge = [&](const size_t from, const size_t to) {
        // merge range = [from, to]
        ReadFromRemoteIORange& from_io_range = need_read_from_remote[from];
        ReadFromRemoteIORange& to_io_range = need_read_from_remote[to];
        int64_t start_offset = from_io_range.offset;
        int64_t merged_size = to_io_range.offset + to_io_range.size - from_io_range.offset;
        // check write pointer is continous
        DCHECK(from_io_range.write_pointer + merged_size == to_io_range.write_pointer + to_io_range.size);
        merged_need_read_from_remote.emplace_back(start_offset, from_io_range.write_pointer, merged_size);
    };

    size_t unmerge = 0;
    for (size_t i = 1; i < need_read_from_remote.size(); i++) {
        const auto& prev = need_read_from_remote[i - 1];
        const auto& now = need_read_from_remote[i];
        size_t prev_end = prev.offset + prev.size;
        size_t now_start = now.offset;
        if (now_start != prev_end) {
            // offset not matched, start to merge from [unmerge, i - 1]
            do_merge(unmerge, i - 1);
            unmerge = i;
        }
    }
    do_merge(unmerge, need_read_from_remote.size() - 1);

    // Don't need it anymore
    need_read_from_remote.clear();

    for (const auto& io_range : merged_need_read_from_remote) {
        DCHECK(io_range.offset >= origin_offset);
        DCHECK(io_range.offset + io_range.size <= origin_offset + count);
        RETURN_IF_ERROR(_read_blocks_from_remote(io_range.offset, io_range.size, io_range.write_pointer));
    }

    return Status::OK();
}

StatusOr<int64_t> CacheInputStream::read(void* data, int64_t count) {
    count = std::min(_size - _offset, count);
    RETURN_IF_ERROR(read_at_fully(_offset, data, count));
    _offset += count;
    return count;
}

Status CacheInputStream::seek(int64_t offset) {
    if (offset < 0 || offset >= _size) return Status::InvalidArgument(fmt::format("Invalid offset {}", offset));
    _offset = offset;
    return _sb_stream->seek(offset);
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
    SharedBufferPtr sb;
    ASSIGN_OR_RETURN(auto s, _sb_stream->peek_shared_buffer(count, &sb));
    if (_enable_populate_cache) {
        _populate_to_cache(s.data(), _offset, count, sb);
    }
    return s;
}

void CacheInputStream::_populate_to_cache(const char* p, int64_t offset, int64_t count, const SharedBufferPtr& sb) {
    int64_t begin = offset / _block_size * _block_size;
    int64_t end = std::min((offset + count + _block_size - 1) / _block_size * _block_size, _size);
    p -= (offset - begin);
    auto f = [sb, this](const char* buf, size_t off, size_t size) {
        DCHECK(off % _block_size == 0);
        if (_already_populated_blocks.contains(off / _block_size)) {
            // Already populate in CacheInputStream's lifecycle, ignore this time
            return;
        }

        SCOPED_RAW_TIMER(&_stats.write_cache_ns);
        WriteCacheOptions options;
        options.async = _enable_async_populate_mode;
        options.evict_probability = _datacache_evict_probability;
        options.priority = _priority;
        options.ttl_seconds = _ttl_seconds;
        options.frequency = _frequency;
        if (options.async && sb) {
            auto cb = [sb](int code, const std::string& msg) {
                // We only need to keep the shared buffer pointer
                LOG_IF(WARNING, code != 0 && code != EEXIST) << "write block cache failed, errmsg: " << msg;
            };
            options.callback = cb;
            options.allow_zero_copy = true;
        }
        Status r = _cache->write(_cache_key, off, size, buf, &options);
        if (r.ok() || r.is_already_exist()) {
            _already_populated_blocks.emplace(off / _block_size);
        }

        if (r.ok()) {
            _stats.write_cache_count += 1;
            _stats.write_cache_bytes += size;
            _stats.write_mem_cache_bytes += options.stats.write_mem_bytes;
            _stats.write_disk_cache_bytes += options.stats.write_disk_bytes;
        } else if (!_can_ignore_populate_error(r)) {
            _stats.write_cache_fail_count += 1;
            _stats.write_cache_fail_bytes += size;
            LOG(WARNING) << "write block cache failed, errmsg: " << r.message();
        } else if (r.is_already_exist() || r.is_resource_busy()) {
            _stats.skip_write_cache_count += 1;
            _stats.skip_write_cache_bytes += size;
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

bool CacheInputStream::_can_ignore_populate_error(const Status& status) const {
    if (status.is_already_exist() || status.is_resource_busy() || status.is_mem_limit_exceeded() ||
        status.is_capacity_limit_exceeded()) {
        return true;
    }
    return false;
}

int64_t CacheInputStream::_calculate_remote_latency_per_block(int64_t io_bytes, int64_t read_time_ns) {
    int64_t latency_us_per_block = read_time_ns / 1000;
    // We try to estimate the average latency for accessing one block.
    // However, there is not a linear ratio between the read bytes and the read latency. For example,
    // the latency of accessing 1M bytes is usually less than 4 times that of accessing 256K bytes.
    // It makes the accurate estimation difficult.
    // So, we just use an approximate ratio to optimize the estimation. The value 2 is only an empirical value,
    // which may not be entirely accurate, but in most cases it can reflect this computational relationship.
    // If the total `io_bytes` between `[_block_size / 2, _block_size * 2]`, we treat their average latency for
    // accessing one block are same as the total `io_time`. In other cases, we will calculate the that latency
    // by their linear scale with the approximate ratio.
    static const int64_t approximate_ratio = 2;
    if (io_bytes > approximate_ratio * _block_size) {
        latency_us_per_block =
                std::min(latency_us_per_block, latency_us_per_block * _block_size / io_bytes * approximate_ratio);
    } else if (io_bytes * approximate_ratio < _block_size) {
        latency_us_per_block =
                std::max(latency_us_per_block, latency_us_per_block * _block_size / io_bytes / approximate_ratio);
    }
    return latency_us_per_block;
}

} // namespace starrocks::io
