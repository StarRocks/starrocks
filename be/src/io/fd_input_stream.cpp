// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "io/fd_input_stream.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/logging.h"
#include "gutil/macros.h"
#include "io/io_error.h"
#include "io_profiler.h"
#include "util/stopwatch.hpp"

namespace starrocks::io {

#define CHECK_IS_CLOSED(is_closed)                                                       \
    do {                                                                                 \
        if (UNLIKELY(is_closed)) return Status::InternalError("file has been close()d"); \
    } while (0)

FdInputStream::FdInputStream(int fd) : _fd(fd), _errno(0), _offset(0), _close_on_delete(false), _is_closed(false) {}

FdInputStream::~FdInputStream() {
    if (_close_on_delete) {
        auto st = close();
        LOG_IF(ERROR, !st.ok()) << "close() failed: " << st;
    }
}

Status FdInputStream::close() {
    CHECK_IS_CLOSED(_is_closed);
    int res;
    RETRY_ON_EINTR(res, ::close(_fd));
    if (res != 0) {
        _errno = errno;
        return io_error("close", _errno);
    }
    return Status::OK();
}

StatusOr<int64_t> FdInputStream::read(void* data, int64_t count) {
    CHECK_IS_CLOSED(_is_closed);
    MonotonicStopWatch watch;
    watch.start();
    ssize_t res;
    RETRY_ON_EINTR(res, ::pread(_fd, static_cast<char*>(data), count, _offset));
    if (UNLIKELY(res < 0)) {
        _errno = errno;
        return io_error("read", _errno);
    }
    _offset += res;
    IOProfiler::add_read(res, watch.elapsed_time());
    return res;
}

StatusOr<int64_t> FdInputStream::get_size() {
    CHECK_IS_CLOSED(_is_closed);
    struct stat st;
    auto res = ::fstat(_fd, &st);
    if (res != 0) {
        _errno = errno;
        return io_error("fstat", _errno);
    }
    return st.st_size;
}

Status FdInputStream::seek(int64_t offset) {
    if (offset < 0) return Status::InvalidArgument(fmt::format("Invalid offset {}", offset));
    _offset = offset;
    return Status::OK();
}

#undef CHECK_IS_CLOSED
} // namespace starrocks::io
