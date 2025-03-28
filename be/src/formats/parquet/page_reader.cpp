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

#include <algorithm>
#include <memory>
#include <ostream>
#include <vector>

#include "cache/object_cache/object_cache.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "exec/hdfs_scanner.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/utils.h"
#include "gutil/strings/substitute.h"
#include "io/compressed_input_stream.h"
#include "runtime/exec_env.h"
#include "util/compression/stream_compression.h"
#include "util/lazy_slice.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

// Reference for:
// https://github.com/apache/arrow/blob/7ebc88c8fae62ed97bc30865c845c8061132af7e/cpp/src/parquet/column_reader.h#L54-L57
static constexpr size_t kDefaultPageHeaderSize = 16 * 1024;
// 16MB is borrowed from Arrow
static constexpr size_t kMaxPageHeaderSize = 16 * 1024 * 1024;

PageReader::PageReader(uint64_t start_offset, uint64_t length, uint64_t num_values,
                       const ColumnReaderOptions& opts, const tparquet::CompressionCodec::type codec)
        : _stream(opts.file->stream()), _finish_offset(start_offset + length), _num_values_total(num_values), _opts(opts), _codec(codec) {
    if (_opts.use_file_pagecache) {
        _cache = CacheEnv::GetInstance()->external_table_page_cache();
        _init_page_cache_key();
    }
    _compressed_buf = std::make_shared<std::vector<uint8_t>>();
    _uncompressed_buf = std::make_shared<std::vector<uint8_t>>();
}

Status PageReader::next_page() {
    if (_opts.use_file_pagecache) {
        _uncompressed_buf.reset();
    }
    return seek_to_offset(_next_header_pos);
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

    size_t allowed_page_size = kDefaultPageHeaderSize;
    size_t remaining = _finish_offset - _offset;
    uint32_t header_length = 0;

    RETURN_IF_ERROR(_stream->seek(_offset));

    do {
        allowed_page_size = std::min(std::min(allowed_page_size, remaining), kMaxPageHeaderSize);

        std::vector<uint8_t> page_buffer;
        const uint8_t* page_buf = nullptr;

        // prefer peek data instead to read data.
        bool peek_mode = false;
        {
            auto st = _stream->peek(allowed_page_size);
            if (st.ok() && st.value().size() == allowed_page_size) {
                page_buf = (const uint8_t*)st.value().data();
                peek_mode = true;
            } else {
                page_buffer.reserve(allowed_page_size);
                RETURN_IF_ERROR(_stream->read_at_fully(_offset, page_buffer.data(), allowed_page_size));
                page_buf = page_buffer.data();
                auto st = _stream->peek(allowed_page_size);
                if (st.ok()) {
                    _opts.stats->bytes_read -= allowed_page_size;
                    peek_mode = true;
                }
            }
        }

        header_length = allowed_page_size;
        auto st = deserialize_thrift_msg(page_buf, &header_length, TProtocolType::COMPACT, &_cur_header);

        if (st.ok()) {
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
    _next_header_pos = _offset + _cur_header.compressed_page_size;
    if (_cur_header.type == tparquet::PageType::DATA_PAGE) {
        _num_values_read += _cur_header.data_page_header.num_values;
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
    if (!_opts.use_file_pagecache) {
        return _read_and_decompress_internal(false);
    }

    std::string& page_cache_key = _current_page_cache_key();
    ObjectCacheHandle* cache_handle = nullptr;
    Status st = _cache->lookup(page_cache_key, &cache_handle);
    if (st.ok()) {
        _uncompressed_buf = *(static_cast<const BufferPtr*>(_cache->value(cache_handle)));
        _cache->release(cache_handle);
        size_t data_length =
                _codec != tparquet::CompressionCodec::UNCOMPRESSED ? _cur_header.compressed_page_size :
                                                                   _cur_header.uncompressed_page_size;
        RETURN_IF_ERROR(_skip_bytes(data_length));
        DCHECK_EQ(_uncompressed_buf->size(), _cur_header.uncompressed_page_size);
    } else {
        _uncompressed_buf = std::make_shared<std::vector<uint8_t>>();
        ASSIGN_OR_RETURN([[maybe_unused]] auto ret, _read_and_decompress_internal(true))
        BufferPtr* capture = new BufferPtr(_uncompressed_buf);
        Status st = Status::InternalError("write footer cache failed");
        int64_t page_cache_size = sizeof(BufferPtr) + _uncompressed_buf->size();
        DeferOp op([&st, this, capture, page_cache_size, &cache_handle]() {
            if (st.ok()) {
                _cache->release(cache_handle);
            } else {
                delete capture;
            }
        });
        auto deleter = [](const CacheKey& key, void* value) { delete (BufferPtr*)value; };
        ObjectCacheWriteOptions options;
        // TODO, deal with it.
        options.evict_probability = 0;
        st = _cache->insert(page_cache_key, capture, page_cache_size, page_cache_size, deleter, &cache_handle,
                            &options);
    }

    return Slice(_uncompressed_buf->data(), _uncompressed_buf->size());
}

StatusOr<Slice> PageReader::_read_and_decompress_internal(bool need_fill_buf) {
    RETURN_IF_ERROR(_stream->seek(_offset));
    bool is_compressed = _codec != tparquet::CompressionCodec::UNCOMPRESSED;
    // TODO deal with uncompressed data
    if (!is_compressed || (_cur_header.type == tparquet::PageType::DATA_PAGE_V2 && _cur_header.data_page_header_v2.__isset.is_compressed && !_cur_header.data_page_header_v2.is_compressed)) {
        return Status::NotSupported("to be done");
    }

    size_t filled = 0;
    if (_cur_header.type == tparquet::PageType::DATA_PAGE_V2) {
        filled = _cur_header.data_page_header_v2.definition_levels_byte_length + _cur_header.data_page_header_v2.repetition_levels_byte_length;
        RETURN_IF_ERROR(_stream->read(_uncompressed_buf->data(), filled));
    }

    auto compress_type = ParquetUtils::convert_compression_codec(_codec);
    std::unique_ptr<StreamCompression> dec;
    RETURN_IF_ERROR(StreamCompression::create_decompressor(compress_type, &dec));
    auto compressed_input_stream =
            std::make_unique<io::CompressedInputStream>(_stream, std::shared_ptr<StreamCompression>(dec.release()));
    auto decompress_filler = std::make_unique<DecompressFiller>(std::move(compressed_input_stream));

    size_t uncompressed_size = _cur_header.uncompressed_page_size;
    raw::stl_vector_resize_uninitialized(_uncompressed_buf.get(), uncompressed_size);
    auto lazy_slice = std::make_unique<LazySlice>(reinterpret_cast<char *>(_uncompressed_buf->data()), uncompressed_size, filled, std::move(decompress_filler));
    RETURN_IF_ERROR(lazy_slice->try_to_trigger_fill(filled, uncompressed_size));
    auto uncompressed_data = Slice(_uncompressed_buf->data(), uncompressed_size);
    return uncompressed_data;
}

} // namespace starrocks::parquet
