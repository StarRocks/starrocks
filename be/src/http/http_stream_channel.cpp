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

#include "http/http_stream_channel.h"

#include <event2/buffer.h>
#include <event2/http.h>

#include "http/http_request.h"

namespace starrocks {

HttpStreamChannel::HttpStreamChannel(HttpRequest* req) : _req(req), _buffer(nullptr) {}

HttpStreamChannel::~HttpStreamChannel() {
    end();
}

HttpStreamChannel& HttpStreamChannel::start() {
    CHECK(_buffer == nullptr);
    _buffer = evbuffer_new();
    evhttp_send_reply_start(_req->get_evhttp_request(), 200, "OK");
    return *this;
}

HttpStreamChannel& HttpStreamChannel::write(const void* chunk, size_t size) {
    CHECK(_buffer != nullptr);
    evbuffer_add(_buffer, chunk, size);
    evhttp_send_reply_chunk(_req->get_evhttp_request(), _buffer);
    return *this;
}

void HttpStreamChannel::end() {
    if (_buffer != nullptr) {
        evbuffer_free(_buffer);
        evhttp_send_reply_end(_req->get_evhttp_request());
        _buffer = nullptr;
    }
}

} // namespace starrocks