// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "io/seekable_input_stream.h"

namespace starrocks::io {

class RandomAccessFile : public SeekableInputStream {
public:
    ~RandomAccessFile() override = default;

    // Read at most |count| bytes data from given position |offset|.
    // Return the number of bytes read, or error.
    virtual StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) = 0;

    // Return the total file size in bytes, or error.
    virtual StatusOr<int64_t> get_size() = 0;
};

} // namespace starrocks::io
