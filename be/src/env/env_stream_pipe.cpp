// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "env/env_stream_pipe.h"

#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "runtime/stream_load/stream_load_pipe.h"

namespace starrocks {

StreamPipeSequentialFile::StreamPipeSequentialFile(std::shared_ptr<StreamLoadPipe> file) : _file(std::move(file)) {}

StreamPipeSequentialFile::~StreamPipeSequentialFile() {
    _file->close();
}

Status StreamPipeSequentialFile::read(Slice* result) {
    bool eof = false;
    return _file->read(reinterpret_cast<uint8_t*>(result->data), &(result->size), &eof);
}

Status StreamPipeSequentialFile::read_one_message(std::unique_ptr<uint8_t[]>* buf, size_t* length) {
    return _file->read_one_message(buf, length);
}

Status StreamPipeSequentialFile::skip(uint64_t n) {
    return _file->seek(n);
}

} // namespace starrocks
