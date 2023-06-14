// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "io/seekable_input_stream.h"

namespace starrocks::io {

// A RandomAccessFile backed by an in-memory array of bytes.
class ArrayInputStream : public SeekableInputStream {
public:
    explicit ArrayInputStream() : _data(nullptr), _size(0), _offset(0) {}
    // The input array must outlive the stream.
    explicit ArrayInputStream(const void* data, int64_t size) : _data(data), _size(size), _offset(0) {}

    ~ArrayInputStream() override = default;

    ArrayInputStream(const ArrayInputStream&) = delete;
    ArrayInputStream(ArrayInputStream&&) = delete;
    void operator=(const ArrayInputStream&) = delete;
    void operator=(ArrayInputStream&&) = delete;

    void reset(const void* data, int64_t size) {
        _data = data;
        _size = size;
        _offset = 0;
    }

    StatusOr<int64_t> read(void* data, int64_t count) override;

    StatusOr<std::string_view> peek(int64_t nbytes) override;

    StatusOr<int64_t> get_size() override { return _size; }

    StatusOr<int64_t> position() override { return _offset; }

    Status seek(int64_t offset) override;

private:
    const void* _data;
    int64_t _size;
    int64_t _offset;
};

} // namespace starrocks::io
