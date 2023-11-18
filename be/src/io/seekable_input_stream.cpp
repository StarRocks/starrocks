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

#include "io/seekable_input_stream.h"

#include <fmt/format.h>

#include "util/raw_container.h"

namespace starrocks::io {

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

} // namespace starrocks::io
