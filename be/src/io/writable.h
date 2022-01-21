// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
    // Check allows_aliasing() before calling write_aliased(). It will fall
    // back to copying if write_aliased() is called on a stream that does not
    // allow aliasing.
    // NOTE: It is caller's responsibility to ensure that the chunk of memory
    // remains live until all of the data has been consumed from the stream.
    virtual Status write_aliased(const void* data, int64_t size) = 0;

    // Flush buffered bytes, if any
    virtual Status flush() = 0;
};

} // namespace starrocks::io
