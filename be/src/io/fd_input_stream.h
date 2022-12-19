// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "io/seekable_input_stream.h"

namespace starrocks::io {

// A RandomAccessFile which reads from a file descriptor.
class FdInputStream : public SeekableInputStream {
public:
    explicit FdInputStream(int fd);

    ~FdInputStream() override;

    FdInputStream(const FdInputStream&) = delete;
    FdInputStream(FdInputStream&&) = delete;
    void operator=(const FdInputStream&) = delete;
    void operator=(FdInputStream&&) = delete;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    StatusOr<int64_t> get_size() override;

    StatusOr<int64_t> position() override { return _offset; }

    Status seek(int64_t offset) override;

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
    int64_t _offset;
    bool _close_on_delete;
    bool _is_closed;
};

} // namespace starrocks::io
