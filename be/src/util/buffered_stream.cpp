// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "util/buffered_stream.h"

#include "common/config.h"
#include "env/env.h"
#include "util/bit_util.h"

namespace starrocks {

BufferedInputStream::BufferedInputStream(RandomAccessFile* file, uint64_t offset, uint64_t length)
        : _file(file), _offset(offset), _end_offset(offset + length) {
    _reserve(config::buffer_stream_reserve_size);
}

Status BufferedInputStream::get_bytes(const uint8_t** buffer, size_t* nbytes, bool peek) {
    if (*nbytes <= num_remaining()) {
        *buffer = _buf.get() + _buf_position;
        if (!peek) {
            _buf_position += *nbytes;
        }
        return Status::OK();
    }

    _reserve(*nbytes);
    RETURN_IF_ERROR(_read_data());

    size_t max_get = std::min(*nbytes, num_remaining());
    *buffer = _buf.get() + _buf_position;
    *nbytes = max_get;
    if (!peek) {
        _buf_position += max_get;
    }
    return Status::OK();
}

void BufferedInputStream::_reserve(size_t nbytes) {
    if (nbytes <= _buf_capacity - _buf_position) {
        return;
    }

    if (nbytes > _buf_capacity) {
        size_t new_capacity = BitUtil::next_power_of_two(nbytes);
        std::unique_ptr<uint8_t[]> new_buf(new uint8_t[new_capacity]);
        if (num_remaining() > 0) {
            memcpy(new_buf.get(), _buf.get() + _buf_position, num_remaining());
        }
        _buf = std::move(new_buf);
        _buf_capacity = new_capacity;
    } else {
        if (num_remaining() > 0 && _buf_position > 0) {
            memmove(_buf.get(), _buf.get() + _buf_position, num_remaining());
        }
    }

    _buf_written -= _buf_position;
    _buf_position = 0;
}

Status BufferedInputStream::_read_data() {
    size_t bytes_read = std::min(left_capactiy(), _end_offset - _file_offset);
    Slice slice(_buf.get() + _buf_written, bytes_read);
    auto st = _file->read(_file_offset, &slice);
    if (!st.ok() && !st.is_end_of_file()) {
        return st;
    }

    _file_offset += slice.size;
    _buf_written += slice.size;
    return Status::OK();
}

} // namespace starrocks
