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

#include <bthread/condition_variable.h>
#include <bthread/execution_queue.h>
#include <bthread/mutex.h>

#include <atomic>
#include <map>
#include <string>
#include <unordered_set>

#include "common/statusor.h"
#include "runtime/batch_write/batch_write_util.h"
#include "util/countdown_latch.h"

namespace starrocks {

namespace bthreads {
class ThreadPoolExecutor;
}

class StreamLoadContext;

using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

class AsyncAppendDataContext;
struct Task {
    AsyncAppendDataContext* context;
};

class IsomorphicBatchWrite {
public:
    explicit IsomorphicBatchWrite(BatchWriteId batch_write_id, bthreads::ThreadPoolExecutor* executor);

    Status init();

    Status register_stream_load_pipe(StreamLoadContext* pipe_ctx);
    void unregister_stream_load_pipe(StreamLoadContext* pipe_ctx);
    // For testing
    bool contain_pipe(StreamLoadContext* pipe_ctx);
    bool is_pipe_alive(StreamLoadContext* pipe_ctx);

    Status append_data(StreamLoadContext* data_ctx);

    void stop();
    bool is_stopped() const { return _stopped.load(std::memory_order_relaxed); }

private:
    static int _execute_tasks(void* meta, bthread::TaskIterator<Task>& iter);

    Status _execute_write(AsyncAppendDataContext* async_ctx);
    Status _write_data_to_pipe(AsyncAppendDataContext* data_ctx);
    Status _send_rpc_request(StreamLoadContext* data_ctx);
    Status _wait_for_load_status(StreamLoadContext* data_ctx, int64_t timeout_ns);

    BatchWriteId _batch_write_id;
    bthreads::ThreadPoolExecutor* _executor;
    bool _batch_write_async{false};

    bthread::Mutex _mutex;
    bthread::ConditionVariable _cv;
    std::unordered_set<StreamLoadContext*> _alive_stream_load_pipe_ctxs;
    std::unordered_set<StreamLoadContext*> _dead_stream_load_pipe_ctxs;

    // Undocemented rule of bthread that -1(0xFFFFFFFFFFFFFFFF) is an invalid ExecutionQueueId
    constexpr static uint64_t kInvalidQueueId = (uint64_t)-1;
    bthread::ExecutionQueueId<Task> _queue_id{kInvalidQueueId};

    std::atomic<bool> _stopped{false};
};
using IsomorphicBatchWriteSharedPtr = std::shared_ptr<IsomorphicBatchWrite>;

} // namespace starrocks
