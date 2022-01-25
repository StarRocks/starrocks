// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "io/fd_random_access_file.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/logging.h"
#include "gutil/macros.h"
#include "io/io_error.h"

namespace starrocks::io {

#define CHECK_IS_CLOSED(is_closed)                                                       \
    do {                                                                                 \
        if (UNLIKELY(is_closed)) return Status::InternalError("file has been close()d"); \
    } while (0)

FdRandomAccessFile::FdRandomAccessFile(int fd) : _fd(fd), _errno(0), _close_on_delete(false), _is_closed(false) {}

FdRandomAccessFile::~FdRandomAccessFile() {
    if (_close_on_delete) {
        auto st = close();
        LOG_IF(ERROR, !st.ok()) << "close() failed: " << st;
    }
}

Status FdRandomAccessFile::close() {
    CHECK_IS_CLOSED(_is_closed);
    int res;
    RETRY_ON_EINTR(res, ::close(_fd));
    if (res != 0) {
        _errno = errno;
        return io_error("close", _errno);
    }
    return Status::OK();
}

StatusOr<int64_t> FdRandomAccessFile::read(void* data, int64_t count) {
    CHECK_IS_CLOSED(_is_closed);
    ssize_t res;
    RETRY_ON_EINTR(res, ::read(_fd, static_cast<char*>(data), count));
    if (UNLIKELY(res < 0)) {
        _errno = errno;
        return io_error("read", _errno);
    }
    return res;
}

StatusOr<int64_t> FdRandomAccessFile::read_at(int64_t offset, void* data, int64_t count) {
    CHECK_IS_CLOSED(_is_closed);
    ssize_t res;
    RETRY_ON_EINTR(res, ::pread(_fd, static_cast<char*>(data), count, offset));
    if (UNLIKELY(res < 0)) {
        _errno = errno;
        return io_error("pread", _errno);
    }
    return res;
}

StatusOr<int64_t> FdRandomAccessFile::seek(int64_t offset, int whence) {
    CHECK_IS_CLOSED(_is_closed);
    off_t res = ::lseek(_fd, offset, whence);
    if (res < 0) {
        _errno = errno;
        return io_error("lseek", _errno);
    }
    return res;
}

Status FdRandomAccessFile::skip(int64_t count) {
    CHECK_IS_CLOSED(_is_closed);
    off_t res = lseek(_fd, count, SEEK_CUR);
    if (res < 0) {
        _errno = errno;
        return io_error("lseek", _errno);
    }
    return Status::OK();
}

StatusOr<std::string_view> FdRandomAccessFile::peak(int64_t nbytes) {
    return Status::NotSupported("FdRandomAccessFile::peak()");
}

StatusOr<int64_t> FdRandomAccessFile::get_size() {
    CHECK_IS_CLOSED(_is_closed);
    struct stat st;
    auto res = ::fstat(_fd, &st);
    if (res != 0) {
        _errno = errno;
        return io_error("fstat", _errno);
    }
    return st.st_size;
}

StatusOr<int64_t> FdRandomAccessFile::position() {
    CHECK_IS_CLOSED(_is_closed);
    return seek(0, SEEK_CUR);
}

#undef CHECK_IS_CLOSED
} // namespace starrocks::io
