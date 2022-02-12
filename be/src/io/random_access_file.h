// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "io/seekable_input_stream.h"

namespace starrocks::io {

class RandomAccessFile : public SeekableInputStream {
public:
    ~RandomAccessFile() override = default;

    // Read up to |count| bytes data from given position |offset|.
    // Return the number of bytes read, or error.
    virtual StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) = 0;

    // Read exactly the number of |count| bytes data from given position |offset|.
    // This method does not return the number of bytes read because either
    // (1) the entire |count| bytes is read
    // (2) the end of the stream is reached
    // If the eof is reached, an IO error is returned, leave the content of
    // |data| buffer unspecified.
    virtual Status read_at_fully(int64_t offset, void* out, int64_t count);

    // Return the total file size in bytes, or error.
    virtual StatusOr<int64_t> get_size() = 0;
};

} // namespace starrocks::io
