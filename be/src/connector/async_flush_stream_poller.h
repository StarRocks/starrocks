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

#include <future>
#include <queue>

#include "common/status.h"
#include "formats/utils.h"
#include "io/async_flush_output_stream.h"

#pragma once

namespace starrocks::connector {

/// own a FIFO queue of `io::AsyncFlushOutputStream`
/// client periodically poll the state of async io operations via `poll()`
/// each `io::AsyncFlushOutputStream` will be destroyed once its async status is ready and fetched
class AsyncFlushStreamPoller {
public:
    using Stream = io::AsyncFlushOutputStream;

    AsyncFlushStreamPoller() = default;

    void enqueue(std::unique_ptr<Stream> stream);

    // return a pair of
    // 1. io status
    // 2. bool indicates if all io finished
    std::pair<Status, bool> poll();

    int64_t releasable_memory();

private:
    struct StreamWithStatus {
        std::unique_ptr<Stream> stream;
        std::future<Status> async_status;
    };

    std::deque<StreamWithStatus> _queue;
};

} // namespace starrocks::connector
