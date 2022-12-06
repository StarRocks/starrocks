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

#include "common/statusor.h"
#include "io/writable.h"
#include "util/slice.h"

namespace starrocks::io {

// OutputStream is the superclass of all classes representing an output stream of
// bytes. An output stream accepts output bytes and sends them to some sink.
class OutputStream : public Writable {
public:
    using Buffer = Slice;
    using Position = uint8_t*;

    ~OutputStream() override = default;

    // Skips a number of bytes, leaving the bytes unmodified
    // in the underlying buffer. This is mainly useful with `get_direct_buffer()`.
    // Note of caution, the skipped bytes may contain uninitialized data.
    // The caller must make sure that the skipped bytes are properly initialized,
    // otherwise you might leak bytes from your heap.
    virtual Status skip(int64_t count) = 0;

    // Return the pointer and size of the unwritten part of the OutputStream's
    // underlying buffer, but does not advance the stream's current position.
    // If the caller writes any data to this buffer, it should then call `skip()`
    // to skip over the consumed bytes.
    virtual StatusOr<Buffer> get_direct_buffer() = 0;

    // If there are at least |size| bytes available in the current buffer,
    // returns a pointer directly into the buffer and advances over these
    // bytes.
    // The caller may then write directly into this buffer rather than go
    // through OutputStream. If there are not enough bytes available,
    // returns NULL. The return pointer is invalidated as soon as any other
    // non-const method of OutputStream is called.
    virtual StatusOr<Position> get_direct_buffer_and_advance(int64_t size) = 0;
};

} // namespace starrocks::io
