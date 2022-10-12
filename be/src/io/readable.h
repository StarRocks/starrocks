// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/statusor.h"

namespace starrocks::io {

// A Readable is a source of bytes. Bytes from a Readable are made available
// to callers of the read method via a byte array.
class Readable {
public:
    virtual ~Readable() = default;

    // Read up to |count| bytes into |data|.
    // Returns the number of bytes read, 0 means EOF.
    virtual StatusOr<int64_t> read(void* data, int64_t count) = 0;

    // Read exactly |count| bytes into |data|.
    // This method does not return the number of bytes read because either
    // (1) the entire |count| bytes is read
    // (2) the end of the stream is reached
    // If the eof is reached, an IO error is returned, leave the content of
    // |data| buffer unspecified.
    virtual Status read_fully(void* data, int64_t count);
};

} // namespace starrocks::io
