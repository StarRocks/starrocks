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

#include "formats/parquet/page_reader.h"

#include "common/config.h"
#include "exec/hdfs_scanner.h"
#include "formats/parquet/column_reader.h"
#include "gutil/strings/substitute.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

// Reference for:
// https://github.com/apache/arrow/blob/7ebc88c8fae62ed97bc30865c845c8061132af7e/cpp/src/parquet/column_reader.h#L54-L57
static constexpr size_t kDefaultPageHeaderSize = 16 * 1024;
// 16MB is borrowed from Arrow
static constexpr size_t kMaxPageHeaderSize = 16 * 1024 * 1024;

PageReader::PageReader(io::SeekableInputStream* stream, uint64_t start_offset, uint64_t length, uint64_t num_values,
                       const ColumnReaderOptions& opts)
    : _stream(stream), _finish_offset(start_offset + length), _num_values_total(num_values), _opts(opts) {
        if (opts.use_file_pagecache) {
            _cache = BlockCache::instance();
            _init_pagecache_key();
        }
    }

void PageReader::_init_pagecache_key() {
    auto& filename = _opts.file->filename();
    _pagecache_key.resize(22);
    char* data = _pagecache_key.data();
    const std::string page_suffix = "pg";
    uint64_t hash_value = HashUtil::hash64(filename.data(), filename.size(), 0);
    memcpy(data, &hash_value, sizeof(hash_value));
    memcpy(data + 8, page_suffix.data(), page_suffix.length());
    // The modification time is more appropriate to indicate the different file versions.
    // While some data source, such as Hudi, have no modification time because their files
    // cannot be overwritten. So, if the modification time is unsupported, we use file size instead.
    // Also, to reduce memory usage, we only use the high four bytes to represent the second timestamp.
    if (_opts.file_mtime > 0) {
        uint32_t mtime_s = (_opts.file_mtime >> 9) & 0x00000000FFFFFFFF;
        memcpy(data + 10, &mtime_s, sizeof(mtime_s));
    } else {
        uint32_t size = _opts.file_size;
        memcpy(data + 10, &size, sizeof(size));
    }
}

void PageReader::_update_pagecache_key(uint64_t offset) {
    memcpy(_pagecache_key.data() + 14, &offset, sizeof(offset));
}

static void empty_deleter(void*) {}

Status PageReader::next_header() {
    if (_offset != _next_header_pos) {
        return Status::InternalError(
                strings::Substitute("Try to parse parquet column header in wrong position, offset=$0 vs expect=$1",
                                    _offset, _next_header_pos));
    }

    DCHECK(_num_values_read <= _num_values_total);
    if (_num_values_read >= _num_values_total) {
        LOG_IF(WARNING, _num_values_read > _num_values_total)
                << "Read more values than expected, read=" << _num_values_read << ", expect=" << _num_values_total;
        return Status::EndOfFile("");
    }

    size_t allowed_page_size = kDefaultPageHeaderSize;
    size_t remaining = _finish_offset - _offset;
    uint32_t header_length = 0;
    if (_cache) {
        _update_pagecache_key(_offset);
    }

    RETURN_IF_ERROR(_stream->seek(_offset));

    do {
        allowed_page_size = std::min(std::min(allowed_page_size, remaining), kMaxPageHeaderSize);

        std::vector<uint8_t> page_buffer;
        uint8_t* page_buf = nullptr;

        // prefer peek data instead to read data.
        bool peek_mode = false;
        IOBuffer buffer;
        RETURN_IF_ERROR(_get_header(allowed_page_size, &page_buf, &peek_mode, &buffer));

        header_length = allowed_page_size;
        auto st = deserialize_thrift_msg(page_buf, &header_length, TProtocolType::COMPACT, &_cur_header);
        if (st.ok()) {
            if (_local_page_buffer.offset == 0) {
                // read from cache
                size_t page_size = buffer.size() - header_length;
                _local_page_buffer.page.resize(page_size);
                buffer.copy_to(_local_page_buffer.page.data(), page_size, header_length);
                _local_page_buffer.offset = header_length;
            } else if (_remote_page_buffer.offset == 0) {
                // read from remote
                _remote_page_buffer.offset = header_length;
            }
            if (peek_mode) {
                _opts.stats->bytes_read += header_length;
            }
            break;
        }

        if (UNLIKELY((allowed_page_size >= kMaxPageHeaderSize) || (_offset + allowed_page_size) >= _finish_offset)) {
            // Notice, here (_offset + allowed_page_size) >= _finish_offset
            // is using '>=' just to prevent loop infinitely.
            return Status::Corruption(
                    strings::Substitute("Failed to decode parquet page header, page header's size is out of range.  "
                                        "allowed_page_size=$0, max_page_size=$1, offset=$2, finish_offset=$3",
                                        allowed_page_size, kMaxPageHeaderSize, _offset, _finish_offset));
        }

        allowed_page_size *= 2;
    } while (true);
    DCHECK(header_length > 0);
    _offset += header_length;
    _header_length = header_length;
    _next_header_pos = _offset + _cur_header.compressed_page_size;
    _num_values_read += _cur_header.data_page_header.num_values;
    return Status::OK();
}

Status PageReader::_get_header(size_t allowed_page_size, uint8_t** page_buf, bool* peek_mode, IOBuffer* buffer) {
    if (_cache) {
        _local_page_buffer.reset();
        _remote_page_buffer.reset();
    }

    if (!_cache) {
        _buffer.reserve(allowed_page_size);
        RETURN_IF_ERROR(_stream->read_at_fully(_offset, _buffer.data(), allowed_page_size));
        *page_buf = _buffer.data();
        auto st = _stream->peek(allowed_page_size);
        if (st.ok()) {
            _opts.stats->bytes_read -= allowed_page_size;
            *peek_mode = true;
        }
        return Status::OK();
    }

    Status res;
    {
        SCOPED_RAW_TIMER(&_opts.stats->pagecache_read_ns);
        res = _cache->read_buffer(_pagecache_key, buffer, &_read_options);   
    }
    if (res.ok()) {
        _local_page_buffer.header.resize(allowed_page_size);
        _local_page_buffer.offset = 0;
        *page_buf = _local_page_buffer.header.data();
        buffer->copy_to(*page_buf, allowed_page_size, 0);
        _opts.stats->pagecache_read_bytes += buffer->size();
        _opts.stats->pagecache_read_count++;;
        return res;
    }

    _remote_page_buffer.header.resize(allowed_page_size);
    auto st = _stream->peek(allowed_page_size);
    if (st.ok() && st.value().size() == allowed_page_size) {
        *peek_mode = true;
        memcpy(_remote_page_buffer.header.data(), st.value().data(), allowed_page_size);
    } else {
        RETURN_IF_ERROR(_stream->read_at_fully(_offset, _remote_page_buffer.header.data(), allowed_page_size));
        st = _stream->peek(allowed_page_size);
        if (st.ok()) {
            _opts.stats->bytes_read -= allowed_page_size;
            *peek_mode = true;
        }
    }
    _remote_page_buffer.offset = 0;
    *page_buf = _remote_page_buffer.header.data();
    return Status::OK();
}

Status PageReader::set_page_buffer(Slice* slice) {
    if (!_cache) {
        return Status::NotFound("cache instance is not exist");
    }
    DCHECK(_remote_page_buffer.offset > 0);
    IOBuffer header_buffer;
    IOBuffer page_buffer;
    header_buffer.append_user_data(_remote_page_buffer.header.data(), _remote_page_buffer.offset, empty_deleter);
    page_buffer.append_user_data(slice->mutable_data(), slice->get_size(), empty_deleter);
    IOBuffer buffer;
    buffer.append(header_buffer);
    buffer.append(page_buffer);

    Status res;
    {
        SCOPED_RAW_TIMER(&_opts.stats->pagecache_write_ns);
        res = _cache->write_buffer(_pagecache_key, buffer, &_write_options);   
    }
    if (res.ok()) {
        _opts.stats->pagecache_write_bytes += buffer.size();
        _opts.stats->pagecache_write_count++;
    } else {
        _opts.stats->pagecache_write_fail_count++;
        LOG(WARNING) << "write page buffer failed, reason: " << res.message();
    }

    return Status::OK();
}

Status PageReader::get_page_buffer(size_t size, Slice* slice) {
    if (_offset >= _next_header_pos) {
        return Status::InternalError("get page buffer in exceed offset");
    }

    if (!_cache || _local_page_buffer.offset <= 0) {
        return Status::NotFound("the page buffer not found");
    }

    if (size != _local_page_buffer.page.size()) {
        // Should never be happen
        DCHECK(false);
        return Status::NotFound("the page buffer not found");
    }

    *slice = Slice(_local_page_buffer.page.data(), size);
    uint64_t distance = _next_header_pos - _offset;
    _offset = _next_header_pos;
    _stream->skip(distance);
    return Status::OK();
}

Status PageReader::read_bytes(void* buffer, size_t size) {
    if (_offset + size > _next_header_pos) {
        return Status::InternalError("Size to read exceed page size");
    }
    RETURN_IF_ERROR(_stream->read_at_fully(_offset, buffer, size));
    _offset += size;
    return Status::OK();
}

Status PageReader::skip_bytes(size_t size) {
    if (_offset + size > _next_header_pos) {
        return Status::InternalError("Size to skip exceed page size");
    }
    _offset += size;
    _stream->skip(size);
    return Status::OK();
}

StatusOr<std::string_view> PageReader::peek(size_t size) {
    if (_offset + size > _next_header_pos) {
        return Status::InternalError("Size to read exceed page size");
    }
    _stream->seek(_offset);
    ASSIGN_OR_RETURN(auto ret, _stream->peek(size));
    return ret;
}

} // namespace starrocks::parquet
