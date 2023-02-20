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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/message_body_sink.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/message_body_sink.h"

#include <fcntl.h>
#include <sys/stat.h>

namespace starrocks {

MessageBodyFileSink::~MessageBodyFileSink() {
    if (_fd >= 0) {
        close(_fd);
    }
}

Status MessageBodyFileSink::open() {
    _fd = ::open(_path.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (_fd < 0) {
        PLOG(WARNING) << "fail to open " << _path;
        return Status::InternalError("fail to open file");
    }
    return Status::OK();
}

Status MessageBodyFileSink::append(const char* data, size_t size) {
    auto written = ::write(_fd, data, size);
    if (written == size) {
        return Status::OK();
    }
    PLOG(WARNING) << "fail to write " << _path;
    return Status::InternalError("fail to write file");
}

Status MessageBodyFileSink::append(ByteBufferPtr&& buf) {
    return append(buf->ptr, buf->pos);
}

Status MessageBodyFileSink::finish() {
    if (::close(_fd) < 0) {
        PLOG(WARNING) << "fail to close " << _path;
        _fd = -1;
        return Status::InternalError("fail to close file");
    }
    _fd = -1;
    return Status::OK();
}

void MessageBodyFileSink::cancel(const Status& status) {
    unlink(_path.data());
}

} // namespace starrocks
