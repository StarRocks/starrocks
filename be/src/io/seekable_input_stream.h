// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#pragma once

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
    virtual int64_t position() = 0;
};

} // namespace starrocks::io
