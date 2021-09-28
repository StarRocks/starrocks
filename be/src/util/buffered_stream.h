// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/status.h"

namespace starrocks {

class RandomAccessFile;

class BufferedInputStream {
public:
    BufferedInputStream(RandomAccessFile* file, uint64_t offset, uint64_t length);
    ~BufferedInputStream() = default;

    void seek_to(uint64_t offset) {
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

    void skip(uint64_t nbytes) {
        if (_buf_position + nbytes < _buf_written) {
            _buf_position += nbytes;
        } else {
            _file_offset = tell() + nbytes;
            _buf_position = 0;
            _buf_written = 0;
        }
    }

    uint64_t tell() const { return _file_offset - num_remaining(); }

    Status get_bytes(const uint8_t** buffer, size_t* nbytes, bool peek = false);

private:
    void _reserve(size_t nbytes);
    Status _read_data();
    size_t num_remaining() const { return _buf_written - _buf_position; }
    size_t left_capactiy() const { return _buf_capacity - _buf_written; }

private:
    std::unique_ptr<uint8_t[]> _buf;
    RandomAccessFile* _file;
    uint64_t _offset = 0;
    uint64_t _end_offset = 0;

    size_t _buf_capacity = 0;
    size_t _buf_position = 0;
    size_t _buf_written = 0;

    uint64_t _file_offset = 0;
};

} // namespace starrocks
