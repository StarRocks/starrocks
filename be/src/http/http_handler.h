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

#pragma once

#include <glog/logging.h>

#include <atomic>

#include "util/defer_op.h"

namespace starrocks {

class HttpRequest;
class HttpChannel;

#define CHECK_RUNNING_COUNT()                                              \
    if (_running_count >= 10) {                                            \
        LOG(WARNING) << "DEBUG: so many running task: " << _running_count; \
    }                                                                      \
    _running_count++;                                                      \
    DeferOp op([&] {                                                       \
        _running_count--;                                                  \
    });

// Handler for on http request
class HttpHandler {
public:
    virtual ~HttpHandler() = default;
    virtual void handle(HttpRequest* req) = 0;

    virtual bool request_will_be_read_progressively() { return false; }

    // This funciton will called when all headers are recept.
    // return 0 if process successfully. otherwise return -1;
    // If return -1, on_header function should send_reply to HTTP client
    // and function wont send any reply any more.
    virtual int on_header(HttpRequest* req) { return 0; }

    virtual void on_chunk_data(HttpRequest* req) {}
    virtual void free_handler_ctx(void* handler_ctx) {}

    virtual std::string type() const = 0;

protected:
    std::atomic<int64_t> _running_count = 0;
};

} // namespace starrocks
