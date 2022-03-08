// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "io/input_stream.h"

namespace starrocks::io {

class SeekableInputStream : public InputStream {
public:
    ~SeekableInputStream() override = default;

    // Repositions the offset of the InputStream to the argument |offset|
    // according to the directive |whence| as follows:
    // - SEEK_SET: The offset is set to offset bytes.
    // - SEEK_CUR: The offset is set to its current location plus offset
    //   bytes.
    // - SEEK_END: The offset is set to the size of the file plus offset
    //   bytes.
    // Returns the resulting offset location as measured in bytes from
    // the beginning of the InputStream
    virtual StatusOr<int64_t> seek(int64_t offset, int whence) = 0;

    // Returns the current offset location as measured in bytes from
    // the beginning of the InputStream
    virtual StatusOr<int64_t> position() = 0;

    // Read up to |count| bytes data from given position |offset|.
    //
    // This method will NOT change the internal offset.
    //
    // Return the number of bytes read, or error.
    virtual StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) = 0;

    // Read exactly the number of |count| bytes data from given position |offset|.
    // This method does not return the number of bytes read because either
    // (1) the entire |count| bytes is read
    // (2) the end of the stream is reached
    // If the eof is reached, an IO error is returned, leave the content of
    // |data| buffer unspecified.
    //
    // This method will NOT change the internal offset.
    virtual Status read_at_fully(int64_t offset, void* out, int64_t count);

    // Return the total file size in bytes, or error.
    virtual StatusOr<int64_t> get_size() = 0;
};

} // namespace starrocks::io
