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

#include "io/array_input_stream.h"

#include <fmt/format.h>

namespace starrocks::io {

StatusOr<int64_t> ArrayInputStream::read(void* data, int64_t count) {
    if (count < 0) {
        return Status::InvalidArgument("negative count");
    }
    if (_offset >= _size) {
        return 0;
    }
    int64_t n = std::min(count, _size - _offset);
    memcpy(data, reinterpret_cast<const char*>(_data) + _offset, n);
    _offset += n;
    return n;
}

Status ArrayInputStream::seek(int64_t offset) {
    if (offset < 0) return Status::InvalidArgument(fmt::format("Invalid offset {}", offset));
    _offset = offset;
    return Status::OK();
}

StatusOr<std::string_view> ArrayInputStream::peek(int64_t nbytes) {
    int64_t n = std::max<int64_t>(std::min(_size - _offset, nbytes), 0);
    return std::string_view(reinterpret_cast<const char*>(_data) + _offset, n);
}

} // namespace starrocks::io
