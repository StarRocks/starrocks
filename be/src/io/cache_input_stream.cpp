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
    // default _buffer size is 4MB(16 * 256KB)
    _buffer_size = 16 * _block_size;
    _buffer.reserve(_buffer_size);
}

CacheInputStream::~CacheInputStream() {
    int64_t io_bytes = _sb_stream->shared_io_bytes();
    if (io_bytes > 0) {
        int64_t latency_us_per_block = (_sb_stream->shared_io_timer() / 1000 * _block_size / io_bytes);
        _cache->record_read_remote(io_bytes, latency_us_per_block);
    }
}

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

    SharedBufferedInputStream::SharedBuffer* sb = nullptr;
    if (_enable_block_buffer) {
        auto ret = _sb_stream->find_shared_buffer(offset, size);
        if (ret.ok()) {
            sb = ret.value();
            if (sb->buffer.capacity() > 0) {
                strings::memcpy_inlined(out, sb->buffer.data() + offset - sb->offset, size);
                if (_enable_populate_cache) {
                    _populate_cache_from_zero_copy_buffer((const char*)sb->buffer.data() + block_offset - sb->offset,
                                                          block_offset, load_size);
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
        SCOPED_RAW_TIMER(&read_cache_ns);
        if (_enable_block_buffer) {
            res = _cache->read_buffer(_cache_key, block_offset, load_size, &block.buffer, &options);
            read_size = load_size;
        } else {
            StatusOr<size_t> r = _cache->read_buffer(_cache_key, offset, size, out, &options);
            res = r.status();
            read_size = size;
        }
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
        _cache->record_read_cache(read_size, read_cache_ns / 1000);
        return Status::OK();
    } else if (res.is_resource_busy()) {
        _stats.skip_read_cache_count += 1;
        _stats.skip_read_cache_bytes += read_size;
    }
    if (!res.is_not_found() && !res.is_resource_busy()) return res;

    if (sb) {
        // Duplicate the block ranges to avoid saving the same data both in cache and shared buffer.
        _deduplicate_shared_buffer(sb);
    }

    return Status::NotFound("Not Found");
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
        // Everytime read at least one buffer size
        const int64_t read_size = std::min(_buffer_size, block_end_offset - read_offset_cursor);
        RETURN_IF_ERROR(_sb_stream->read_at_fully(read_offset_cursor, _buffer.data(), read_size));

        // write _buffer's data into `out`
        const int64_t shift = out_offset_cursor - read_offset_cursor;
        const int64_t out_size = std::min(read_size - shift, out_remain_size);
        if (out_size > 0) {
            strings::memcpy_inlined(out_pointer_cursor, _buffer.data() + shift, out_size);

            // cursor for `out`
            out_offset_cursor += out_size;
            out_pointer_cursor += out_size;
            out_remain_size -= out_size;
        }

        if (_enable_populate_cache) {
            RETURN_IF_ERROR(_populate_to_cache(read_offset_cursor, read_size, _buffer.data()));
        }

        read_offset_cursor += read_size;
    }
    DCHECK_EQ(0, out_remain_size);
    DCHECK_EQ(offset + size, out_offset_cursor);
    DCHECK_EQ(out + size, out_pointer_cursor);

    return Status::OK();
}

Status CacheInputStream::_populate_to_cache(const int64_t offset, const int64_t size, char* src) {
    SCOPED_RAW_TIMER(&_stats.write_cache_ns);
    const int64_t write_end_offset = offset + size;
    char* src_cursor = src;

    for (int64_t write_offset_cursor = offset; write_offset_cursor < write_end_offset;) {
        DCHECK(write_offset_cursor % _block_size == 0);
        WriteCacheOptions options{};
        const int64_t write_size = std::min(_block_size, write_end_offset - write_offset_cursor);
        Status r = _cache->write_buffer(_cache_key, write_offset_cursor, write_size, src_cursor, &options);

        src_cursor += write_size;
        write_offset_cursor += write_size;

        if (r.ok()) {
            _stats.write_cache_count += 1;
            _stats.write_cache_bytes += write_size;
            _stats.write_mem_cache_bytes += options.stats.write_mem_bytes;
            _stats.write_disk_cache_bytes += options.stats.write_disk_bytes;
        } else if (!r.is_already_exist()) {
            _stats.write_cache_fail_count += 1;
            _stats.write_cache_fail_bytes += write_size;
            LOG(WARNING) << "write block cache failed, errmsg: " << r.get_error_msg();
            // Failed to write cache, but we can keep processing query.
        }
    }
    return Status::OK();
}

void CacheInputStream::_deduplicate_shared_buffer(SharedBufferedInputStream::SharedBuffer* sb) {
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
    // It is impossible that all block exists in block_map because we check block map before
    // reading remote storage.
    for (int64_t i = start_block_id; i <= end_block_id; ++i) {
        _block_map.erase(i);
    }

    sb->offset = std::max(start_block_id * _block_size, sb->offset);
    int64_t end = std::min((end_block_id + 1) * _block_size, end_offset);
    sb->size = end - sb->offset;
}

struct ReadFromRemoteIORange {
    ReadFromRemoteIORange(const int64_t offset, char* write_pointer, const int64_t size)
            : offset(offset), write_pointer(write_pointer), size(size) {}
    const int64_t offset;
    char* write_pointer;
    const int64_t size;
};

Status CacheInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    const BlockCache* cache = BlockCache::instance();
    const int64_t origin_offset = offset;
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

    std::vector<ReadFromRemoteIORange> need_read_from_remote{};

    // The number of blocks we ignored
    // For example [block1] [block2] [block3] [block4]
    // If [block1] not existed in cache, we will not check [block2] is existed in cache, just go to check [block3],
    // then [block1] and [block2] will from remote directly.
    int64_t _remain_ignore_block_nums = 0;
    for (int64_t i = start_block_id; i <= end_block_id; i++) {
        size_t off = std::max(offset, i * _block_size);
        size_t end = std::min((i + 1) * _block_size, end_offset);
        DCHECK(off >= origin_offset);
        DCHECK(end <= origin_offset + count);
        size_t size = end - off;
        Status st;
        if (_remain_ignore_block_nums > 0) {
            st = Status::NotFound("Ingore this block");
            _remain_ignore_block_nums--;
        } else {
            DCHECK_EQ(0, _remain_ignore_block_nums);
            st = _read_block_from_local(off, size, p);
        }
        if (st.is_not_found()) {
            // Not found block from local
            need_read_from_remote.emplace_back(off, p, size);
            if (_remain_ignore_block_nums == 0) {
                _remain_ignore_block_nums = _ignore_block_nums;
            }
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
        WriteCacheOptions options;
        options.overwrite = false;
        Status r = cache->write_buffer(_cache_key, offset, size, buf, &options);
        if (r.ok()) {
            _stats.write_cache_count += 1;
            _stats.write_cache_bytes += size;
            _stats.write_mem_cache_bytes += options.stats.write_mem_bytes;
            _stats.write_disk_cache_bytes += options.stats.write_disk_bytes;
        } else if (r.is_cancelled()) {
            _stats.skip_write_cache_count += 1;
            _stats.skip_write_cache_bytes += size;
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
