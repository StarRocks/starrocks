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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/status.h"

namespace starrocks {

class RandomAccessFile;

class IBufferedInputStream {
public:
    virtual Status get_bytes(const uint8_t** buffer, size_t offset, size_t* nbytes, bool peek) = 0;
    virtual void seek_to(uint64_t offset) = 0;
    virtual void skip(uint64_t nbytes) = 0;
    virtual ~IBufferedInputStream() = default;
};

class DefaultBufferedInputStream : public IBufferedInputStream {
public:
    DefaultBufferedInputStream(RandomAccessFile* file, uint64_t offset, uint64_t length);

    ~DefaultBufferedInputStream() override = default;

    void seek_to(uint64_t offset) override {
        uint64_t current_file_offset = tell();
        if (offset < current_file_offset) {
            // TODO(zc): To reuse already read data in some case, however it is not a common case
            _buf_position = 0;
            _buf_written = 0;
            _file_offset = offset;
        } else {
            skip(offset - current_file_offset);
        }
    }

    void skip(uint64_t nbytes) override {
        if (_buf_position + nbytes < _buf_written) {
            _buf_position += nbytes;
        } else {
            _file_offset = tell() + nbytes;
            _buf_position = 0;
            _buf_written = 0;
        }
    }

    uint64_t tell() const { return _file_offset - num_remaining(); }

    Status get_bytes(const uint8_t** buffer, size_t* nbytes, bool peek);

    void reserve(size_t nbytes);

    Status get_bytes(const uint8_t** buffer, size_t offset, size_t* nbytes, bool peek) override;

private:
    Status _read_data();
    size_t num_remaining() const { return _buf_written - _buf_position; }
    size_t left_capactiy() const { return _buf_capacity - _buf_written; }

private:
    std::unique_ptr<uint8_t[]> _buf;
    RandomAccessFile* _file;
    uint64_t _end_offset = 0;

    size_t _buf_capacity = 0;
    size_t _buf_position = 0;
    size_t _buf_written = 0;

    uint64_t _file_offset = 0;
};

class SharedBufferedInputStream : public IBufferedInputStream {
public:
    struct IORange {
        int64_t offset;
        int64_t size;
        bool operator<(const IORange& x) const { return offset < x.offset; }
    };
    struct CoalesceOptions {
        static constexpr int64_t MB = 1024 * 1024;
        int64_t max_dist_size = 1 * MB;
        int64_t max_buffer_size = 8 * MB;
    };

    SharedBufferedInputStream(RandomAccessFile* file);

    ~SharedBufferedInputStream() override = default;

    Status set_io_ranges(const std::vector<IORange>& ranges);
    void release_to_offset(int64_t offset);

    void seek_to(uint64_t offset) override {}
    void skip(uint64_t nbytes) override {}
    Status get_bytes(const uint8_t** buffer, size_t offset, size_t* nbytes, bool peek) override;
    void release();
    void set_coalesce_options(const CoalesceOptions& options) { _options = options; }

private:
    struct SharedBuffer {
    public:
        int64_t offset;
        int64_t size;
        int64_t ref_count;
        std::vector<uint8_t> buffer;
    };
    RandomAccessFile* _file;
    std::map<int64_t, SharedBuffer> _map;
    CoalesceOptions _options;
};

} // namespace starrocks
