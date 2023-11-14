// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstdint>

#include "common/status.h"
#include "gen_cpp/parquet_types.h"
#include "io/seekable_input_stream.h"

namespace starrocks::parquet {

// Used to parse page header of column chunk. This class don't parse page's type.
class PageReader {
public:
    PageReader(io::SeekableInputStream* stream, size_t start, size_t length, size_t num_values);

    ~PageReader() = default;

    // Try to parse header starts from current _offset. Caller should assure that
    // _offset locates at the start of page header. If _offset doesn't locate the
    // start of header, this function will return error.
    // return Stats::OK if parse page header success.
    Status next_header();

    const tparquet::PageHeader* current_header() const { return &_cur_header; }

    // Must call this function ater next_header called. The total read size
    // after one next_header can not exceede the page's compressed_page_size.
    Status read_bytes(void* buffer, size_t size);

    StatusOr<std::string_view> peek(size_t size);

    Status skip_bytes(size_t size);

    // seek to read position, this position must be a start of a page header.
    void seek_to_offset(uint64_t offset) {
        _offset = offset;
        _next_header_pos = offset;
        _stream->seek(offset);
    }

    uint64_t get_offset() const { return _offset; }

private:
    io::SeekableInputStream* const _stream;
    tparquet::PageHeader _cur_header;

    uint64_t _offset = 0;
    uint64_t _next_header_pos = 0;
    const uint64_t _finish_offset = 0;

    uint64_t _num_values_read = 0;
    const uint64_t _num_values_total = 0;
};

} // namespace starrocks::parquet
