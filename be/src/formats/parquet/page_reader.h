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

#pragma once

#include <stddef.h>

#include <cstdint>
#include <string_view>

#include "base/string/slice.h"
#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/parquet_types.h"
#include "io/seekable_input_stream.h"
#include "storage/rowset/page_handle.h"

namespace starrocks {
class ObjectCache;
class StoragePageCache;
class BlockCompressionCodec;
} // namespace starrocks

namespace starrocks::parquet {

struct ColumnReaderOptions;

// Used to parse page header of column chunk. This class don't parse page's type.
class PageReader {
public:
    PageReader(io::SeekableInputStream* stream, size_t start, size_t length, size_t num_values,
               const ColumnReaderOptions& opts, const tparquet::CompressionCodec::type codec);

    ~PageReader() = default;

    // Try to parse header starts from current _offset. Caller should assure that
    // _offset locates at the start of page header. If _offset doesn't locate the
    // start of header, this function will return error.
    // return Stats::OK if parse page header success.
    Status next_header();

    const tparquet::PageHeader* current_header() const { return &_cur_header; }

    // seek to read position, this position must be a start of a page header.
    Status seek_to_offset(uint64_t offset) {
        _offset = offset;
        _next_header_pos = offset;
        return _stream->seek(offset);
    }

    Status next_page();

    bool is_last_page() { return _num_values_read >= _num_values_total || _next_read_page_idx >= _page_num; }

    void set_page_num(size_t page_num) { _page_num = page_num; }

    void set_next_read_page_idx(size_t cur_page_idx) { _next_read_page_idx = cur_page_idx; }

    StatusOr<Slice> read_and_decompress_page_data();

private:
    // Must call this function after next_header called. The total read size
    // after one next_header can not exceed the page's compressed_page_size.
    Status _read_bytes(void* buffer, size_t size);

    Status _skip_bytes(size_t size);

    StatusOr<std::string_view> _peek(size_t size);

    int32_t _data_length() {
        return _codec != tparquet::CompressionCodec::UNCOMPRESSED ? _cur_header.compressed_page_size
                                                                  : _cur_header.uncompressed_page_size;
    }

    void _init_page_cache_key();
    std::string& _current_page_cache_key();
    Status _deal_page_with_cache();
    Status _read_and_deserialize_header(bool need_fill_cache);
    Status _read_and_decompress_internal(bool need_fill_cache);
    bool _cache_decompressed_data();
    Status _decompress_page(Slice& input, Slice* output);

    io::SeekableInputStream* const _stream;
    tparquet::PageHeader _cur_header;
    uint32_t _header_length;

    uint64_t _offset = 0;
    uint64_t _next_header_pos = 0;
    const uint64_t _finish_offset = 0;

    uint64_t _num_values_read = 0;
    const uint64_t _num_values_total = 0;
    const ColumnReaderOptions& _opts;

    size_t _page_num = 0xffffffff;
    size_t _next_read_page_idx = 0;

    StoragePageCache* _cache = nullptr;
    std::string _page_cache_key;

    const tparquet::CompressionCodec::type _codec;
    const BlockCompressionCodec* _compress_codec = nullptr;

    using BufferPtr = std::unique_ptr<std::vector<uint8_t>>;
    BufferPtr _compressed_buf;
    BufferPtr _uncompressed_buf;
    std::vector<uint8_t>* _cache_buf = nullptr;
    PageHandle _page_handle;
    bool _hit_cache = false;
    bool _skip_page_cache = false;

    Slice _uncompressed_data;
};

} // namespace starrocks::parquet
