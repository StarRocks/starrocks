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

#include "block_cache/io_buffer.h"
#include "gutil/strings/fastmem.h"

namespace starrocks {

size_t IOBuffer::copy_to(void* data, ssize_t size, size_t pos) const {
    size_t bytes_to_copy = size > 0 ? size : _buf.size();
    size_t bytes_copied = 0;
    size_t skip = pos;
    for (size_t i = 0; i < _buf.backing_block_num(); ++i) {
        auto sp = _buf.backing_block(i);
        if (!sp.empty()) {
            if (sp.size() <= skip) {
                skip -= sp.size();
                continue;
            }
            size_t to_cp = std::min(sp.size(), bytes_to_copy - bytes_copied);
            strings::memcpy_inlined((char*)data + bytes_copied, sp.data() + skip, to_cp);
            bytes_copied += to_cp;
            if (bytes_copied >= size) {
                break;
            }
        }
    }
    return bytes_copied;
}

} // namespace starrocks
