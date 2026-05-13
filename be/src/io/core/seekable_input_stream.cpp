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

#include "io/core/seekable_input_stream.h"

#include <fmt/format.h>

#include "base/container/raw_container.h"

namespace starrocks::io {

// Out-of-line so gcov can attribute coverage to a concrete `.cpp` line.
IoStatsSnapshot SeekableInputStreamWrapper::get_io_stats_snapshot() const {
    return _impl->get_io_stats_snapshot();
}

StatusOr<int64_t> SeekableInputStream::read_at(int64_t offset, void* data, int64_t count) {
    RETURN_IF_ERROR(seek(offset));
    return read(data, count);
}

Status SeekableInputStream::read_at_fully(int64_t offset, void* data, int64_t count) {
    RETURN_IF_ERROR(seek(offset));
    return read_fully(data, count);
}

Status SeekableInputStream::skip(int64_t count) {
    ASSIGN_OR_RETURN(auto pos, position());
    return seek(pos + count);
}

void SeekableInputStream::set_size(int64_t count) {}

StatusOr<std::string> SeekableInputStream::read_all() {
    ASSIGN_OR_RETURN(auto size, get_size());
    std::string ret;
    raw::stl_string_resize_uninitialized(&ret, size);
    RETURN_IF_ERROR(read_at_fully(0, ret.data(), ret.size()));
    return std::move(ret);
}

std::string SeekableInputStream::page_cache_key(int64_t stream_offset) const {
    // Use the virtual filename() rather than the _filename member: many subclasses (e.g.
    // RandomAccessFile, MemoryFileInputStream, S3InputStream) override filename() to return a
    // value held in their own member while leaving _filename unset, so reading _filename here
    // would return an empty string and silently collapse the keys of every file to the same
    // (empty, offset) prefix.
    const std::string& fname = filename();
    std::string key;
    key.reserve(fname.size() + sizeof(stream_offset));
    key.append(fname);
    key.append(reinterpret_cast<const char*>(&stream_offset), sizeof(stream_offset));
    return key;
}

} // namespace starrocks::io
