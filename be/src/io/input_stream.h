// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#pragma once

#include <string_view>

#include "io/readable.h"

namespace starrocks::io {

class InputStream : public Readable {
public:
    ~InputStream() override = default();

    // Skips a number of bytes.
    // Returns error if an underlying read error occurs.
    virtual Status skip(int64_t count) = 0;

    // Returns true if InputStream support `peak()`
    virtual bool allows_peak() const = 0;

    // Return zero-copy string_view to upcoming bytes.
    //
    // Do not modify the stream position. The view becomes invalid after
    // any operation on the stream. May trigger buffering if the requested
    // size is larger than the number of buffered bytes.
    //
    // May return NotSupported on streams that don't support it.
    //
    virtual StatusOr<std::string_view> peak(int64_t nbytes) = 0;
};

} // namespace starrocks::io
