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

#include <cstdint>

#include "common/status.h"
#include "gen_cpp/parquet_types.h"
#include "util/buffered_stream.h"

namespace starrocks {

class RandomAccessFile;

}
namespace starrocks::parquet {

// Used to parse page header of column chunk. This class don't parse page's type.
class PageReader {
public:
    PageReader(IBufferedInputStream* stream, size_t start, size_t length);
    ~PageReader() = default;

    // Try to parse header starts from current _offset. Caller should assure that
    // _offset locates at the start of page header. If _offset doesn't locate the
    // start of header, this function will return error.
    // return Stats::OK if parse page header success.
    Status next_header();

    //
    const tparquet::PageHeader* current_header() const { return &_cur_header; }

    // Must call this function ater next_header called. The total read size
    // after one next_header can not exceede the page's compressed_page_size.
    Status read_bytes(const uint8_t** buffer, size_t size);

    Status skip_bytes(size_t size);

    // seek to read position, this position must be a start of a page header.
    void seek_to_offset(uint64_t offset) {
        _stream->seek_to(offset);
        _offset = offset;
        _next_header_pos = offset;
    }

    uint64_t get_offset() const { return _offset; }

private:
    IBufferedInputStream* _stream;
    tparquet::PageHeader _cur_header;

    uint64_t _offset = 0;
    uint64_t _next_header_pos = 0;

    uint64_t _finish_offset = 0;
};

} // namespace starrocks::parquet
