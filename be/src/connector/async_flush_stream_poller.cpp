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

#include "async_flush_stream_poller.h"

namespace starrocks::connector {

void AsyncFlushStreamPoller::enqueue(std::unique_ptr<Stream> stream) {
    auto async_status = stream->io_status();
    _queue.push_back({
            .stream = std::move(stream),
            .async_status = std::move(async_status),
    });
}

std::pair<Status, bool> AsyncFlushStreamPoller::poll() {
    Status status;
    while (!_queue.empty()) {
        auto& f = _queue.front();
        if (!is_ready(f.async_status)) {
            break;
        }
        status.update(f.async_status.get());
        DCHECK(f.stream->releasable_memory() == 0);
        _queue.pop_front();
    }

    return {status, _queue.empty()};
}

int64_t AsyncFlushStreamPoller::releasable_memory() {
    int64_t releasable_memory = 0;
    for (auto& ss : _queue) {
        releasable_memory += ss.stream->releasable_memory();
    }
    return releasable_memory;
}

} // namespace starrocks::connector
