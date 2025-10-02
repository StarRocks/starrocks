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

#include <glog/logging.h>

#include <memory>
#include <ostream>
#include <vector>

#include "cache/datacache.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/status.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/utils.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "util/raw_container.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

// Reference for:
// https://github.com/apache/arrow/blob/7ebc88c8fae62ed97bc30865c845c8061132af7e/cpp/src/parquet/column_reader.h#L54-L57
static constexpr size_t kDefaultPageHeaderSize = 16 * 1024;
// 16MB is borrowed from Arrow
static constexpr size_t kMaxPageHeaderSize = 16 * 1024 * 1024;

PageReader::PageReader(io::SeekableInputStream* stream, uint64_t start_offset, uint64_t length, uint64_t num_values,
                       const ColumnReaderOptions& opts, const tparquet::CompressionCodec::type codec)
        : _stream(stream),
          _finish_offset(start_offset + length),
          _num_values_total(num_values),
          _opts(opts),
          _codec(codec) {
    if (_opts.use_file_pagecache) {
        _cache = DataCache::GetInstance()->page_cache();
        _init_page_cache_key();
    }
    _compressed_buf = std::make_unique<std::vector<uint8_t>>();
    _uncompressed_buf = std::make_unique<std::vector<uint8_t>>();
}

Status PageReader::next_page() {
    if (_opts.use_file_pagecache) {
        _cache_buf = nullptr;
        _hit_cache = false;
        _skip_page_cache = false;
    }
    return seek_to_offset(_next_header_pos);
}

Status PageReader::_deal_page_with_cache() {
    std::string& page_cache_key = _current_page_cache_key();
    PageCacheHandle cache_handle;
    bool ret = _cache->lookup(page_cache_key, &cache_handle);
    if (ret) {
        _hit_cache = true;
        _opts.stats->page_cache_read_counter += 1;
        // TODO: This is an ugly implementation. The _cache_buf is used both as a const pointer
        //  retrieved from the cache and as a temporary mutable pointer before insertion into the cache.
        //  Therefore, I must use const_cast here, which will be optimized later.
        _cache_buf =
                const_cast<std::vector<uint8_t>*>(reinterpret_cast<const std::vector<uint8_t>*>(cache_handle.data()));
        _page_handle = PageHandle(std::move(cache_handle));
        _header_length = _cache_buf->size();
        auto st = deserialize_thrift_msg(_cache_buf->data(), &_header_length, TProtocolType::COMPACT, &_cur_header);
        DCHECK(st.ok());
        _next_header_pos = _offset + _header_length + _data_length();
        RETURN_IF_ERROR(_skip_bytes(_header_length + _data_length()));
    } else {
        auto cache_buf = std::make_unique<std::vector<uint8_t>>();
        _cache_buf = cache_buf.get();
        RETURN_IF_ERROR(_read_and_deserialize_header(true));
        if (config::enable_adjustment_page_cache_skip && !_cache_decompressed_data()) {
            _skip_page_cache = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(_read_and_decompress_internal(true));
        MemCacheWriteOptions opts;
        auto st = _cache->insert(page_cache_key, _cache_buf, opts, &cache_handle);
        if (st.ok()) {
            _page_handle = PageHandle(std::move(cache_handle));
            _opts.stats->page_cache_write_counter += 1;
        } else {
            _page_handle = PageHandle(_cache_buf);
        }
        cache_buf.release();
    }

    return Status::OK();
}

Status PageReader::_read_and_deserialize_header(bool need_fill_cache) {
    size_t allowed_page_size = kDefaultPageHeaderSize;
    size_t remaining = _finish_offset - _offset;
    _header_length = 0;

    RETURN_IF_ERROR(_stream->seek(_offset));
    BufferPtr tmp_page_buffer;
    std::vector<uint8_t>* page_buffer;
    if (need_fill_cache) {
        DCHECK(_cache_buf);
        page_buffer = _cache_buf;
    } else {
        tmp_page_buffer = std::make_unique<std::vector<uint8_t>>();
        page_buffer = tmp_page_buffer.get();
    }

    do {
        allowed_page_size = std::min(std::min(allowed_page_size, remaining), kMaxPageHeaderSize);
        const uint8_t* page_buf = page_buffer->data();

        // prefer peek data instead to read data.
        bool peek_mode = false;
        {
            auto st = _stream->peek(allowed_page_size);
            if (!need_fill_cache && st.ok() && st.value().size() == allowed_page_size) {
                page_buf = (const uint8_t*)st.value().data();
                peek_mode = true;
            } else {
                TRY_CATCH_BAD_ALLOC(raw::stl_vector_resize_uninitialized(page_buffer, allowed_page_size));
                RETURN_IF_ERROR(_stream->read_at_fully(_offset, page_buffer->data(), allowed_page_size));
                page_buf = page_buffer->data();
                auto st = _stream->peek(allowed_page_size);
                if (st.ok()) {
                    _opts.stats->bytes_read -= allowed_page_size;
                    peek_mode = true;
                }
            }
        }

        _header_length = allowed_page_size;
        auto st = deserialize_thrift_msg(page_buf, &_header_length, TProtocolType::COMPACT, &_cur_header);

        if (st.ok()) {
            DCHECK(_header_length > 0);
            page_buffer->resize(_header_length);
            _next_header_pos = _offset + _header_length + _data_length();
            RETURN_IF_ERROR(_skip_bytes(_header_length));
            if (peek_mode) {
                _opts.stats->bytes_read += _header_length;
            }
            _opts.stats->request_bytes_read += _header_length;
            _opts.stats->request_bytes_read_uncompressed += _header_length;
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
    return Status::OK();
}

Status PageReader::next_header() {
    if (_offset != _next_header_pos) {
        return Status::InternalError(
                strings::Substitute("Try to parse parquet column header in wrong position, offset=$0 vs expect=$1",
                                    _offset, _next_header_pos));
    }

    DCHECK(_num_values_read <= _num_values_total);
    if (_num_values_read >= _num_values_total || _next_read_page_idx >= _page_num) {
        LOG_IF(WARNING, _num_values_read > _num_values_total)
                << "Read more values than expected, read=" << _num_values_read << ", expect=" << _num_values_total;
        return Status::EndOfFile("");
    }

    if (_opts.use_file_pagecache) {
        RETURN_IF_ERROR(_deal_page_with_cache());
    } else {
        RETURN_IF_ERROR(_read_and_deserialize_header(false));
    }

    if (_cur_header.type == tparquet::PageType::DATA_PAGE) {
        _num_values_read += _cur_header.data_page_header.num_values;
        _next_read_page_idx++;
    } else if (_cur_header.type == tparquet::PageType::DATA_PAGE_V2) {
        _num_values_read += _cur_header.data_page_header_v2.num_values;
        _next_read_page_idx++;
    }
    return Status::OK();
}

Status PageReader::_read_bytes(void* buffer, size_t size) {
    if (_offset + size > _next_header_pos) {
        return Status::InternalError("Size to read exceed page size");
    }
    RETURN_IF_ERROR(_stream->read_at_fully(_offset, buffer, size));
    _offset += size;
    return Status::OK();
}

Status PageReader::_skip_bytes(size_t size) {
    if (UNLIKELY(_offset + size > _next_header_pos)) {
        return Status::InternalError("Size to skip exceed page size");
    }
    _offset += size;
    RETURN_IF_ERROR(_stream->skip(size));
    return Status::OK();
}

StatusOr<std::string_view> PageReader::_peek(size_t size) {
    if (_offset + size > _next_header_pos) {
        return Status::InternalError("Size to read exceed page size");
    }
    RETURN_IF_ERROR(_stream->seek(_offset));
    ASSIGN_OR_RETURN(auto ret, _stream->peek(size));
    return ret;
}

void PageReader::_init_page_cache_key() {
    auto& filename = _opts.file->filename();
    std::string key =
            ParquetUtils::get_file_cache_key(CacheType::PAGE, filename, _opts.modification_time, _opts.file_size);
    _page_cache_key.resize(22);
    char* data = _page_cache_key.data();
    memcpy(data, key.data(), key.size());
}

std::string& PageReader::_current_page_cache_key() {
    memcpy(_page_cache_key.data() + 14, &_offset, sizeof(_offset));
    return _page_cache_key;
}

StatusOr<Slice> PageReader::read_and_decompress_page_data() {
    _opts.stats->page_read_counter += 1;
    if (!_opts.use_file_pagecache || _skip_page_cache) {
        RETURN_IF_ERROR(_read_and_decompress_internal(false));
        return _uncompressed_data;
    } else {
        if (_cache_decompressed_data()) {
            if (_hit_cache) {
                _opts.stats->page_cache_read_decompressed_counter += 1;
            }
            _uncompressed_data = Slice(_cache_buf->data() + _header_length, _cache_buf->size() - _header_length);
        } else {
            if (_hit_cache) {
                _opts.stats->page_cache_read_compressed_counter += 1;
            }
            Slice input = Slice(_cache_buf->data() + _header_length, _cache_buf->size() - _header_length);
            TRY_CATCH_BAD_ALLOC(
                    raw::stl_vector_resize_uninitialized(_uncompressed_buf.get(), _cur_header.uncompressed_page_size));
            _uncompressed_data = Slice(_uncompressed_buf->data(), _cur_header.uncompressed_page_size);
            RETURN_IF_ERROR(_decompress_page(input, &_uncompressed_data));
        }
        return _uncompressed_data;
    }
}

bool PageReader::_cache_decompressed_data() {
    return _codec == tparquet::CompressionCodec::UNCOMPRESSED ||
           _cur_header.uncompressed_page_size <=
                   config::parquet_page_cache_decompress_threshold * _cur_header.compressed_page_size;
}

Status PageReader::_decompress_page(starrocks::Slice& input, starrocks::Slice* output) {
    if (_compress_codec == nullptr) {
        auto compress_type = ParquetUtils::convert_compression_codec(_codec);
        RETURN_IF_ERROR(get_block_compression_codec(compress_type, &_compress_codec));
    }
    if (_cur_header.type == tparquet::PageType::DATA_PAGE_V2) {
        auto uncompressed_size = output->size;
        auto* mark_pointer = output->data;
        uint32_t bytes_level_size = _cur_header.data_page_header_v2.definition_levels_byte_length +
                                    _cur_header.data_page_header_v2.repetition_levels_byte_length;
        memcpy(output->data, input.data, bytes_level_size);
        input.remove_prefix(bytes_level_size);
        output->remove_prefix(bytes_level_size);
        RETURN_IF_ERROR(_compress_codec->decompress(input, output));
        *output = Slice(mark_pointer, uncompressed_size);
    } else {
        RETURN_IF_ERROR(_compress_codec->decompress(input, output));
    }
    return Status::OK();
}

Status PageReader::_read_and_decompress_internal(bool need_fill_cache) {
    bool is_compressed = true;
    if (_cur_header.type == tparquet::PageType::DATA_PAGE_V2) {
        const auto& page_header = _cur_header.data_page_header_v2;
        if (page_header.__isset.is_compressed) {
            is_compressed = page_header.is_compressed;
        }
    }

    // ARROW-17100: [C++][Parquet] Fix backwards compatibility for ParquetV2 data pages written prior to 3.0.0 per ARROW-10353 #13665
    // https://github.com/apache/arrow/pull/13665/files
    // Prior to Arrow 3.0.0, is_compressed was always set to false in column headers,
    // even if compression was used. See ARROW-17100.
    bool always_compressed = (_opts.file_meta_data->writer_version().IsAlwaysCompressed());
    is_compressed |= always_compressed;

    is_compressed = is_compressed && (_codec != tparquet::CompressionCodec::UNCOMPRESSED);

    RETURN_IF_ERROR(CurrentThread::mem_tracker()->check_mem_limit("read and decompress page"));

    size_t uncompressed_size = _cur_header.uncompressed_page_size;
    // based on parquet.thrift Line 571~575, for DATA_PAGE_V2, even when is_compressed is set as false,
    // data length is decided by compressed_page_size
    size_t read_size = _data_length();
    _opts.stats->request_bytes_read += read_size;
    _opts.stats->request_bytes_read_uncompressed += uncompressed_size;

    // check if we can zero copy read.
    Slice read_data;
    DCHECK(_next_header_pos - _offset == read_size);
    auto ret = _peek(read_size);
    if (!need_fill_cache && ret.ok() && ret.value().size() == read_size) {
        _opts.stats->bytes_read += read_size;
        // peek dos not advance offset.
        RETURN_IF_ERROR(_skip_bytes(read_size));
        read_data = Slice(ret.value().data(), read_size);
    } else {
        std::vector<uint8_t>& read_buffer = is_compressed ? *_compressed_buf : *_uncompressed_buf;
        if (!need_fill_cache || (is_compressed && _cache_decompressed_data())) {
            TRY_CATCH_BAD_ALLOC(read_buffer.reserve(read_size));
            read_data = Slice(read_buffer.data(), read_size);
        } else {
            auto original_size = _cache_buf->size();
            TRY_CATCH_BAD_ALLOC(raw::stl_vector_resize_uninitialized(_cache_buf, original_size + read_size));
            read_data = Slice(_cache_buf->data() + original_size, read_size);
        }
        RETURN_IF_ERROR(_read_bytes(read_data.data, read_data.size));
    }

    // if it's compressed, we have to uncompress page
    // otherwise we just assign slice.
    if (is_compressed) {
        if (!need_fill_cache) {
            TRY_CATCH_BAD_ALLOC(raw::stl_vector_resize_uninitialized(_uncompressed_buf.get(), uncompressed_size));
            _uncompressed_data = Slice(_uncompressed_buf->data(), uncompressed_size);
            return _decompress_page(read_data, &_uncompressed_data);
        } else if (_cache_decompressed_data()) {
            auto original_size = _cache_buf->size();
            TRY_CATCH_BAD_ALLOC(raw::stl_vector_resize_uninitialized(_cache_buf, uncompressed_size + original_size));
            _uncompressed_data = Slice(_cache_buf->data() + original_size, uncompressed_size);
            return _decompress_page(read_data, &_uncompressed_data);
        }
        // if we cache compressed data, we can decompress it later.
    } else {
        _uncompressed_data = read_data;
    }
    return Status::OK();
}

} // namespace starrocks::parquet
