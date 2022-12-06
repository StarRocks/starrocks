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

#include <utility>

#include "io/output_stream.h"

namespace starrocks::io {

class FdOutputStream : public OutputStream {
public:
    // The file descriptor |fd| will be closed on `close()` or destruction.
    explicit FdOutputStream(int fd);

    ~FdOutputStream() override;

    // Disallow copy and assignment
    FdOutputStream(const FdOutputStream&) = delete;
    void operator=(const FdOutputStream&) = delete;
    // Disallow move c'tor and move assignment
    FdOutputStream(FdOutputStream&&) = delete;
    void operator=(FdOutputStream&&) = delete;

    int fd() const { return _fd; }

    Status write(const void* data, int64_t count) override;

    bool allows_aliasing() const override { return false; }

    Status write_aliased(const void* data, int64_t size) override { return write(data, size); }

    Status skip(int64_t count) override;

    StatusOr<Buffer> get_direct_buffer() override { return Buffer(); }

    StatusOr<Position> get_direct_buffer_and_advance(int64_t /*size*/) override { return nullptr; }

    Status close() override;

    // By default, all modified buffer cache pages for the file referred to by the file descriptor
    // will NOT be flushed to the disk device.
    //
    // Calling `set_fdatasync_on_close(true)` to change that.
    //
    // NOTE: `set_sync_file_on_close(true)` does not necessarily ensure that the entry in the directory
    // containing the file has also reached disk.
    void set_fdatasync_on_close(bool value) { _sync_file_on_close = value; }

    // Call set_sync_directory_on_close() and pass the path of the directory of the file referenced
    // by the file descriptor to flush the directory entries on close.
    void set_sync_directory_on_close(std::string dir) { _sync_dir = std::move(dir); }

private:
    Status do_sync_if_needed();

    int _fd;
    bool _sync_file_on_close;
    bool _closed;
    std::string _sync_dir;
};

} // namespace starrocks::io
