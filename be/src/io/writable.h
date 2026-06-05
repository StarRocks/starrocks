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

#include "common/status.h"

namespace starrocks::io {

class Writable {
public:
    virtual ~Writable() = default;

    // Write the given data to the stream
    //
    // This method always processes the bytes in full. Depending on the
    // semantics of the stream, the data may be written out immediately,
    // held in a buffer, or written asynchronously. In the case where
    // the stream buffers the data, it will be copied.
    virtual Status write(const void* data, int64_t size) = 0;

    virtual bool allows_aliasing() const = 0;

    // Write a given chunk of data to the output.
    // Some output streams may implement this in a way that avoids copying.
    // For example, if we have an asynchronized Writable, the chunk of data
    // passed by `write_aliased()` can be passed to the background writer
    // thread directly without copying, while the chunk of data passed by
    // `write()` need to be copied into an internal buffer first, and then
    // the internal buffer will be passed to the background writer thread.
    //
    // Check allows_aliasing() before calling write_aliased(). It will fall
    // back to copying if write_aliased() is called on a stream that does not
    // allow aliasing.
    // NOTE: It is caller's responsibility to ensure that the chunk of memory
    // remains live until all of the data has been consumed from the stream.
    virtual Status write_aliased(const void* data, int64_t size) = 0;

    // Flushes the output stream and releasing the underlying resource.
    // Any further access (including another call to close()) to the stream results in undefined behavior.
    virtual Status close() = 0;
};

} // namespace starrocks::io
