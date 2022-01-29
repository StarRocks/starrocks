// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "io/random_access_file.h"

namespace starrocks::io {

// A RandomAccessFile backed by an in-memory array of bytes.
class ArrayRandomAccessFile : public RandomAccessFile {
public:
    // The input array must outlive the stream.
    explicit ArrayRandomAccessFile(const void* data, int64_t size) : _data(data), _size(size), _offset(0) {}

    ~ArrayRandomAccessFile() override = default;

    ArrayRandomAccessFile(const ArrayRandomAccessFile&) = delete;
    ArrayRandomAccessFile(ArrayRandomAccessFile&&) = delete;
    void operator=(const ArrayRandomAccessFile&) = delete;
    void operator=(ArrayRandomAccessFile&&) = delete;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t count) override;

    StatusOr<int64_t> seek(int64_t offset, int whence) override;

    Status skip(int64_t count) override;

    bool allows_peak() const override { return true; }

    StatusOr<std::string_view> peak(int64_t nbytes) override;

    StatusOr<int64_t> get_size() override { return _size; }

    StatusOr<int64_t> position() override { return _offset; }

private:
    const void* _data;
    int64_t _size;
    int64_t _offset;
};

} // namespace starrocks::io
