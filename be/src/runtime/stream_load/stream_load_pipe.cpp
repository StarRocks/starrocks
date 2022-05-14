// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/stream_load/stream_load_pipe.h

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

#include "runtime/stream_load/stream_load_pipe.h"

namespace starrocks {

StreamLoadPipeInputStream::StreamLoadPipeInputStream(std::shared_ptr<StreamLoadPipe> file) : _pipe(std::move(file)) {}

StreamLoadPipeInputStream::~StreamLoadPipeInputStream() {
    _pipe->close();
}

StatusOr<int64_t> StreamLoadPipeInputStream::read(void* data, int64_t size) {
    bool eof = false;
    size_t nread = size;
    RETURN_IF_ERROR(_pipe->read(static_cast<uint8_t*>(data), &nread, &eof));
    return nread;
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
