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

#include "formats/orc/orc_input_stream.h"

#include <set>

#include "cctz/civil_time.h"
#include "exprs/cast_expr.h"
#include "formats/orc/orc_mapping.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "simd/simd.h"
#include "util/timezone_utils.h"

namespace starrocks {

ORCHdfsFileStream::ORCHdfsFileStream(RandomAccessFile* file, uint64_t length, io::SharedBufferedInputStream* sb_stream)
        : _file(file), _length(length), _cache_buffer(0), _cache_offset(0), _sb_stream(sb_stream) {}

void ORCHdfsFileStream::prepareCache(PrepareCacheScope scope, uint64_t offset, uint64_t length) {
    size_t cache_max_size = config::orc_file_cache_max_size;
    if (scope == PrepareCacheScope::READ_FULL_ROW_INDEX) {
        cache_max_size = config::orc_row_index_cache_max_size;
    }

    if (length > cache_max_size) return;
    if (canUseCacheBuffer(offset, length)) return;
    _cache_buffer.resize(length);
    _cache_offset = offset;
    doRead(_cache_buffer.data(), length, offset);
}

bool ORCHdfsFileStream::canUseCacheBuffer(uint64_t offset, uint64_t length) {
    if ((_cache_buffer.size() != 0) && (offset >= _cache_offset) &&
        ((offset + length) <= (_cache_offset + _cache_buffer.size()))) {
        return true;
    }
    return false;
}

void ORCHdfsFileStream::read(void* buf, uint64_t length, uint64_t offset) {
    if (canUseCacheBuffer(offset, length)) {
        size_t idx = offset - _cache_offset;
        memcpy(buf, _cache_buffer.data() + idx, length);
    } else {
        doRead(buf, length, offset);
    }
}

const std::string& ORCHdfsFileStream::getName() const {
    return _file->filename();
}

void ORCHdfsFileStream::doRead(void* buf, uint64_t length, uint64_t offset) {
    if (buf == nullptr) {
        throw orc::ParseError("Buffer is null");
    }
    Status status = _file->read_at_fully(offset, buf, length);
    if (!status.ok()) {
        auto msg = strings::Substitute("Failed to read $0: $1", _file->filename(), status.to_string());
        throw orc::ParseError(msg);
    }
}

void ORCHdfsFileStream::clearIORanges() {
    if (!_sb_stream) return;
    _sb_stream->release();
}

void ORCHdfsFileStream::setIORanges(std::vector<IORange>& io_ranges) {
    if (!_sb_stream) return;
    std::vector<io::SharedBufferedInputStream::IORange> bs_io_ranges;
    bs_io_ranges.reserve(io_ranges.size());
    for (const auto& r : io_ranges) {
        bs_io_ranges.emplace_back(io::SharedBufferedInputStream::IORange{.offset = static_cast<int64_t>(r.offset),
                                                                         .size = static_cast<int64_t>(r.size)});
    }
    Status st = _sb_stream->set_io_ranges(bs_io_ranges);
    if (!st.ok()) {
        auto msg = strings::Substitute("Failed to setIORanges $0: $1", _file->filename(), st.to_string());
        throw orc::ParseError(msg);
    }
}

} // namespace starrocks
