// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "io/fd_output_stream.h"

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/logging.h"
#include "gutil/macros.h"
#include "io/io_error.h"
#include "io/io_profiler.h"
#include "util/stopwatch.hpp"

namespace starrocks::io {

// Close file descriptor when object goes out of scope.
class ScopedFdCloser {
public:
    explicit ScopedFdCloser(int fd) : fd_(fd) {}

    ~ScopedFdCloser() {
        int err;
        RETRY_ON_EINTR(err, ::close(fd_));
        PLOG_IF(WARNING, err != 0) << "Failed to close fd " << fd_;
    }

private:
    const int fd_;
};

FdOutputStream::FdOutputStream(int fd) : _fd(fd), _sync_file_on_close(false), _closed(false), _sync_dir() {}

FdOutputStream::~FdOutputStream() {
    auto st = FdOutputStream::close();
    LOG_IF(WARNING, !st.ok()) << st;
}

Status FdOutputStream::write(const void* data, int64_t count) {
    // According to the man(2) manual, if count is zero and fd refers to a file other than a regular file,
    // the results of ::write(2) are not specified, so here handle zero ourselves.
    if (UNLIKELY(count == 0)) {
        return Status::OK();
    }
    if (UNLIKELY(count < 0)) {
        return Status::InvalidArgument(fmt::format("negative count: {}", count));
    }
    MonotonicStopWatch watch;
    watch.start();
    int64_t bytes_written = 0;
    while (bytes_written < count) {
        ssize_t r = ::write(_fd, static_cast<const char*>(data) + bytes_written, count - bytes_written);
        if (r > 0) {
            bytes_written += r;
        } else {
            if (errno == EINTR) {
                continue;
            } else {
                return io_error("write", errno);
            }
        }
    }
    IOProfiler::add_write(bytes_written, watch.elapsed_time());
    return Status::OK();
}

Status FdOutputStream::skip(int64_t /*count*/) {
    return Status::NotSupported("FdOutputStream::skip");
}

Status FdOutputStream::do_sync_if_needed() {
    if (_sync_file_on_close) {
        if (::fdatasync(_fd) != 0) {
            return io_error(fmt::format("fdatasync({})", _fd), errno);
        }
    }
    if (!_sync_dir.empty()) {
        int dir_fd;
        RETRY_ON_EINTR(dir_fd, ::open(_sync_dir.c_str(), O_DIRECTORY | O_RDONLY));
        if (dir_fd < 0) {
            return io_error(fmt::format("open(\"{}\", O_DIRECTORY | O_RDONLY)", _sync_dir), errno);
        }
        ScopedFdCloser fd_closer(dir_fd);
        if (::fsync(dir_fd) != 0) {
            return io_error(fmt::format("fsync({}: \"{}\")", dir_fd, _sync_dir), errno);
        }
    }
    return Status::OK();
}

Status FdOutputStream::close() {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;
    Status st = do_sync_if_needed();
    int r;
    RETRY_ON_EINTR(r, ::close(_fd));
    if (st.ok() && r != 0) {
        st = io_error(fmt::format("close({})", _fd), errno);
    }
    _fd = -1;
    return st;
}

} // namespace starrocks::io
