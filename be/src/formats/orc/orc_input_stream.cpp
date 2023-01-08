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

#include <glog/logging.h>

#include <exception>
#include <set>
#include <unordered_map>
#include <utility>

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "exprs/cast_expr.h"
#include "exprs/literal.h"
#include "formats/orc/fill_function.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_mapping.h"
#include "fs/fs.h"
#include "gen_cpp/orc_proto.pb.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "simd/simd.h"
#include "types/logical_type.h"
#include "util/timezone_utils.h"

namespace starrocks {

ORCHdfsFileStream::ORCHdfsFileStream(RandomAccessFile* file, uint64_t length)
        : _file(file), _length(length), _cache_buffer(0), _cache_offset(0), _buffer_stream(_file) {
    SharedBufferedInputStream::CoalesceOptions options = {.max_dist_size = config::io_coalesce_read_max_distance_size,
                                                          .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    _buffer_stream.set_coalesce_options(options);
}

void ORCHdfsFileStream::prepareCache(PrepareCacheScope scope, uint64_t offset, uint64_t length) {
    const size_t cache_max_size = config::orc_file_cache_max_size;
    if (length > cache_max_size) return;
    if (canUseCacheBuffer(offset, length)) return;

    // If this stripe is small, probably other stripes are also small
    // we combine those reads into one, and try to read several stripes in one shot.
    if (scope == PrepareCacheScope::READ_FULL_STRIPE) {
        length = std::min(_length - offset, cache_max_size);
    }

    _cache_buffer.resize(length);
    _cache_offset = offset;
    doRead(_cache_buffer.data(), length, offset, true);
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
        doRead(buf, length, offset, false);
    }
}

const std::string& ORCHdfsFileStream::getName() const {
    return _file->filename();
}

void ORCHdfsFileStream::doRead(void* buf, uint64_t length, uint64_t offset, bool direct) {
    if (buf == nullptr) {
        throw orc::ParseError("Buffer is null");
    }
    Status status;
    if (direct || !_buffer_stream_enabled) {
        status = _file->read_at_fully(offset, buf, length);
    } else {
        const uint8_t* ptr = nullptr;
        size_t nbytes = length;
        status = _buffer_stream.get_bytes(&ptr, offset, &nbytes, false);
        DCHECK_EQ(nbytes, length);
        if (status.ok()) {
            memcpy(buf, ptr, length);
        }
    }
    if (!status.ok()) {
        auto msg = strings::Substitute("Failed to read $0: $1", _file->filename(), status.to_string());
        throw orc::ParseError(msg);
    }
}

void ORCHdfsFileStream::clearIORanges() {
    _buffer_stream_enabled = false;
    _buffer_stream.release();
}

void ORCHdfsFileStream::setIORanges(std::vector<IORange>& io_ranges) {
    _buffer_stream_enabled = true;
    std::vector<SharedBufferedInputStream::IORange> bs_io_ranges;
    bs_io_ranges.reserve(io_ranges.size());
    for (const auto& r : io_ranges) {
        bs_io_ranges.emplace_back(SharedBufferedInputStream::IORange{.offset = static_cast<int64_t>(r.offset),
                                                                     .size = static_cast<int64_t>(r.size)});
    }
    Status st = _buffer_stream.set_io_ranges(bs_io_ranges);
    if (!st.ok()) {
        auto msg = strings::Substitute("Failed to setIORanges $0: $1", _file->filename(), st.to_string());
        throw orc::ParseError(msg);
    }
}

} // namespace starrocks
