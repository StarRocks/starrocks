// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env_stream_pipe.h"

#include "env/env.h"
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

Status StreamLoadPipeInputStream::read_one_message(std::unique_ptr<uint8_t[]>* buf, size_t* length, size_t padding) {
    return _file->read_one_message(buf, length, padding);
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
