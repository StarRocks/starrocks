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

#pragma once

#include <string_view>

#include "gutil/macros.h"
#include "http/http_request.h"

struct evbuffer;

namespace starrocks {

class HttpStreamChannel {
public:
    explicit HttpStreamChannel(HttpRequest* req);

    ~HttpStreamChannel();

    DISALLOW_COPY_AND_MOVE(HttpStreamChannel);

    HttpStreamChannel& start();

    HttpStreamChannel& write(std::string_view chunk) { return write(chunk.data(), chunk.size()); }

    HttpStreamChannel& write(const void* chunk, size_t size);

    void end();

private:
    HttpRequest* _req;
    evbuffer* _buffer;
};

} // namespace starrocks
