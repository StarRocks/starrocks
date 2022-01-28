// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "io/array_random_access_file.h"

namespace starrocks::io {

StatusOr<int64_t> ArrayRandomAccessFile::read(void* data, int64_t count) {
    if (count < 0) {
        return Status::InvalidArgument("negative count");
    }
    int64_t n = std::min(count, _size - _offset);
    memcpy(data, reinterpret_cast<const char*>(_data) + _offset, n);
    _offset += n;
    return n;
}

StatusOr<int64_t> ArrayRandomAccessFile::read_at(int64_t offset, void* data, int64_t count) {
    if (count < 0) {
        return Status::InvalidArgument("negative count");
    }
    if (offset > _size || offset < 0) {
        return Status::InvalidArgument("invalid offset");
    }
    int64_t n = std::min(_size - offset, count);
    memcpy(data, reinterpret_cast<const char*>(_data) + offset, n);
    return n;
}

StatusOr<int64_t> ArrayRandomAccessFile::seek(int64_t offset, int whence) {
    int64_t pos = 0;
    if (whence == SEEK_SET) {
        pos = offset;
    } else if (whence == SEEK_CUR) {
        pos = _offset + offset;
    } else if (whence == SEEK_END) {
        pos = _size + offset;
    } else {
        return Status::InvalidArgument("invalid whence");
    }
    if (pos < 0 || pos > _size) {
        return Status::InvalidArgument("offset out of bounds");
    }
    _offset = pos;
    return _offset;
}

Status ArrayRandomAccessFile::skip(int64_t count) {
    if (UNLIKELY(count > std::numeric_limits<int64_t>::max() - _offset)) {
        _offset = _size;
        return Status::OK();
    }
    _offset = std::min(_size, _offset + count);
    if (UNLIKELY(_offset < 0)) {
        _offset = 0;
    }
    return Status::OK();
}

StatusOr<std::string_view> ArrayRandomAccessFile::peak(int64_t nbytes) {
    int64_t n = std::min(_size - _offset, nbytes);
    return std::string_view(reinterpret_cast<const char*>(_data) + _offset, n);
}

} // namespace starrocks::io
