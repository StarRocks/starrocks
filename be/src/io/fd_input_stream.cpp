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

#include "io/fd_input_stream.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/logging.h"
#include "gutil/macros.h"
#include "io/io_error.h"

#ifdef USE_STAROS
#include "fslib/metric_key.h"
#include "metrics/metrics.h"
#endif

#ifdef USE_STAROS
namespace {
static const staros::starlet::metrics::Labels kSrPosixFsLables({{"fstype", "srposix"}});

DEFINE_HISTOGRAM_METRIC_KEY_WITH_TAG_BUCKET(s_posixread_iosize, staros::starlet::fslib::kMKReadIOSize, kSrPosixFsLables,
                                            staros::starlet::metrics::MetricsSystem::kIOSizeBuckets);
DEFINE_HISTOGRAM_METRIC_KEY_WITH_TAG_BUCKET(s_posixread_iolatency, staros::starlet::fslib::kMKReadIOLatency,
                                            kSrPosixFsLables,
                                            staros::starlet::metrics::MetricsSystem::kIOLatencyBuckets);
} // namespace
#endif

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
    ssize_t res;
#ifdef USE_STAROS
    staros::starlet::metrics::TimeObserver<prometheus::Histogram> observer(s_posixread_iolatency);
#endif
    RETRY_ON_EINTR(res, ::pread(_fd, static_cast<char*>(data), count, _offset));
    if (UNLIKELY(res < 0)) {
        _errno = errno;
        return io_error("read", _errno);
    }
#ifdef USE_STAROS
    s_posixread_iosize.Observe(res);
#endif
    _offset += res;
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
