// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "io/random_access_file.h"

namespace starrocks::io {

// A RandomAccessFile which reads from a file descriptor.
class FdRandomAccessFile : public RandomAccessFile {
public:
    explicit FdRandomAccessFile(int fd);

    ~FdRandomAccessFile() override;

    FdRandomAccessFile(const FdRandomAccessFile&) = delete;
    FdRandomAccessFile(FdRandomAccessFile&&) = delete;
    void operator=(const FdRandomAccessFile&) = delete;
    void operator=(FdRandomAccessFile&&) = delete;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t count) override;

    StatusOr<int64_t> seek(int64_t offset, int whence) override;

    Status skip(int64_t count) override;

    bool allows_peak() const override { return false; }

    StatusOr<std::string_view> peak(int64_t nbytes) override;

    StatusOr<int64_t> get_size() override;

    StatusOr<int64_t> position() override;

    // closes the underlying file.
    //
    // Returns error if an error occurs during the process;
    // use get_errno() to examine the error. Even if an error occurs,
    // the file descriptor is closed when this returns.
    Status close();

    // By default, the file descriptor is not closed when the stream is destroyed.
    //
    // Call set_close_on_delete(true) to change that. WARNING: This leaves no way for
    // the caller to detect if close() fails. If detecting close() errors is important
    // to you, you should arrange to close the descriptor yourself.
    void set_close_on_delete(bool value) { _close_on_delete = value; }

    // If an I/O error has occurred on this file descriptor, this is the errno from that error.
    //
    // Otherwise, this is zero.
    int get_errno() const { return _errno; }

private:
    int _fd;
    int _errno;
    bool _close_on_delete;
    bool _is_closed;
};

} // namespace starrocks::io
