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

#include "fs/bundle_file.h"

#include "fs/fs.h"
#include "fs/fs_util.h"

namespace starrocks {

Status BundleSeekableInputStream::init() {
    // Initialize the stream.
    RETURN_IF_ERROR(_stream->seek(_offset));
    return Status::OK();
}

Status BundleSeekableInputStream::seek(int64_t position) {
    return _stream->seek(_offset + position);
}

StatusOr<int64_t> BundleSeekableInputStream::position() {
    ASSIGN_OR_RETURN(auto pos, _stream->position());
    return pos - _offset;
}

StatusOr<int64_t> BundleSeekableInputStream::read_at(int64_t offset, void* out, int64_t count) {
    return _stream->read_at(_offset + offset, out, count);
}

Status BundleSeekableInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    return _stream->read_at_fully(_offset + offset, out, count);
}

StatusOr<int64_t> BundleSeekableInputStream::get_size() {
    return _size;
}

Status BundleSeekableInputStream::skip(int64_t count) {
    return _stream->skip(count);
}

StatusOr<std::string> BundleSeekableInputStream::read_all() {
    std::string result;
    result.resize(_size);
    RETURN_IF_ERROR(_stream->read_at_fully(_offset, result.data(), _size));
    return std::move(result);
}

Status BundleSeekableInputStream::touch_cache(int64_t offset, size_t length) {
    // Touch the cache for the underlying stream.
    return _stream->touch_cache(_offset + offset, length);
}

StatusOr<std::unique_ptr<io::NumericStatistics>> BundleSeekableInputStream::get_numeric_statistics() {
    return _stream->get_numeric_statistics();
}

const std::string& BundleSeekableInputStream::filename() const {
    return _stream->filename();
}

StatusOr<int64_t> BundleSeekableInputStream::read(void* data, int64_t count) {
    return _stream->read(data, count);
}

} // namespace starrocks