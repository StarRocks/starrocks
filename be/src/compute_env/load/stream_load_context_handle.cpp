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

#include "compute_env/load/stream_load_context_handle.h"

#include <utility>

#include "compute_env/load/stream_load_context.h"
#include "runtime/message_body_sink.h"

namespace starrocks {

StreamLoadContextHandle::StreamLoadContextHandle(StreamLoadContext* context, CloseCallback close_cb)
        : _context(context), _close_cb(std::move(close_cb)) {}

StreamLoadContextHandle::~StreamLoadContextHandle() {
    close(Status::Cancelled("Close the stream load pipe"));
}

void StreamLoadContextHandle::cancel(const Status& status) {
    if (_context == nullptr || _context->body_sink == nullptr) {
        return;
    }
    _context->body_sink->cancel(status);
}

void StreamLoadContextHandle::close(const Status& status) {
    if (_closed) {
        return;
    }
    _closed = true;

    if (_context == nullptr) {
        return;
    }

    cancel(status);
    if (_close_cb) {
        _close_cb(_context);
    }
    _context = nullptr;
}

} // namespace starrocks
