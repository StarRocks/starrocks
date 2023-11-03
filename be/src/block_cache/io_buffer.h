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

#include <butil/iobuf.h>

#include "starcache/obj_handle.h"

namespace starrocks {

class IOBuffer {
public:
    using IOBuf = butil::IOBuf;

    IOBuffer() = default;
    explicit IOBuffer(const IOBuffer& iobuf) { _buf = iobuf._buf; }
    explicit IOBuffer(const IOBuf& buf) { _buf = buf; }
    ~IOBuffer() = default;

    void append(const IOBuffer& other) { _buf.append(other._buf); }

    int append_user_data(void* data, size_t size, void (*deleter)(void*)) {
        return _buf.append_user_data(data, size, deleter);
    }

    size_t pop_back(size_t n) { return _buf.pop_back(n); }

    size_t copy_to(void* data, ssize_t size = -1, size_t pos = 0) const;

    size_t size() const { return _buf.size(); }

    bool empty() const { return _buf.empty(); }

    IOBuf& raw_buf() { return _buf; }

    const IOBuf& const_raw_buf() const { return _buf; }

private:
    butil::IOBuf _buf;
};

// We use the `starcache::ObjectHandle` directly because implementing a new one seems unnecessary.
// Importing the starcache headers here is not graceful, but the `cachelib` doesn't support
// object cache and we'll deprecate it for some performance reasons. Now there is no need to
// pay too much attention to the compatibility and upper-level abstraction of the cachelib interface.
using CacheHandle = starcache::ObjectHandle;

} // namespace starrocks
