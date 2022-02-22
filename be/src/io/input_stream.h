// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string_view>

#include "io/readable.h"

namespace starrocks::io {

// InputStream is the superclass of all classes representing an input stream of bytes.
class InputStream : public Readable {
public:
    ~InputStream() override = default;

    // Skips a number of bytes.
    // This is guaranteed to be no slower that reading the same data, but may be faster.
    // If end of stream is reached, skipping will stop at the end of the stream, and skip
    // will return OK.
    // Returns error if an underlying read error occurs.
    virtual Status skip(int64_t count) = 0;

    // Returns true if InputStream support `peak()`
    virtual bool allows_peak() const { return false; }

    // Return zero-copy string_view to upcoming bytes.
    //
    // Do not modify the stream position. The view becomes invalid after
    // any operation on the stream. May trigger buffering if the requested
    // size is larger than the number of buffered bytes.
    //
    // May return NotSupported on streams that don't support it.
    //
    virtual StatusOr<std::string_view> peak(int64_t nbytes) { return Status::NotSupported("InputStream::peak"); }
};

} // namespace starrocks::io
