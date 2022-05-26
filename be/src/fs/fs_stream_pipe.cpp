// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "fs/fs_stream_pipe.h"

#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "runtime/stream_load/stream_load_pipe.h"

namespace starrocks {

StreamLoadPipeInputStream::StreamLoadPipeInputStream(std::shared_ptr<StreamLoadPipe> file) : _file(std::move(file)) {}

StreamLoadPipeInputStream::~StreamLoadPipeInputStream() {
    _file->close();
}

StatusOr<int64_t> StreamLoadPipeInputStream::read(void* data, int64_t size) {
    bool eof = false;
    size_t nread = size;
    RETURN_IF_ERROR(_file->read(static_cast<uint8_t*>(data), &nread, &eof));
    return nread;
}

Status StreamLoadPipeInputStream::read(ByteBufferPtr* buf) {
    return _file->read(buf);
}

Status StreamLoadPipeInputStream::skip(int64_t n) {
    std::unique_ptr<char[]> buf(new char[n]);
    do {
        ASSIGN_OR_RETURN(auto r, read(buf.get(), n));
        if (r == 0) {
            break;
        }
        n -= r;
    } while (n > 0);
    return Status::OK();
}

} // namespace starrocks
