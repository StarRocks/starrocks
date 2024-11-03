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
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>

#include "common/statusor.h"
#include "util/countdown_latch.h"

namespace starrocks {

namespace bthreads {
class ThreadPoolExecutor;
}

class StreamLoadContext;

using LoadParams = std::map<std::string, std::string>;

struct BatchWriteId {
    std::string db;
    std::string table;
    LoadParams load_params;
};

std::ostream& operator<<(std::ostream& out, const BatchWriteId& id);

// Hash function for BatchWriteId
struct BatchWriteIdHash {
    std::size_t operator()(const BatchWriteId& id) const {
        std::size_t hash = std::hash<std::string>{}(id.db);
        hash ^= std::hash<std::string>{}(id.table) << 1;

        for (const auto& param : id.load_params) {
            hash ^= std::hash<std::string>{}(param.first) << 1;
            hash ^= std::hash<std::string>{}(param.second) << 1;
        }

        return hash;
    }
};

// Equality function for BatchWriteId
struct BatchWriteIdEqual {
    bool operator()(const BatchWriteId& lhs, const BatchWriteId& rhs) const {
        return lhs.db == rhs.db && lhs.table == rhs.table && lhs.load_params == rhs.load_params;
    }
};

using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

struct Task {
    int64_t create_time_ns;
    StreamLoadContext* data_ctx;
    BThreadCountDownLatch* latch;
    Status* result;
};

class IsomorphicBatchWrite {
public:
    explicit IsomorphicBatchWrite(BatchWriteId batch_write_id, bthreads::ThreadPoolExecutor* executor);

    Status init();

    Status register_stream_load_pipe(StreamLoadContext* pipe_ctx);
    void unregister_stream_load_pipe(StreamLoadContext* pipe_ctx);

    Status append_data(StreamLoadContext* data_ctx);

    void stop();

private:
    static int _execute_bthread_tasks(void* meta, bthread::TaskIterator<Task>& iter);

    Status _execute_write(StreamLoadContext* data_ctx);
    Status _write_data(StreamLoadContext* data_ctx);
    Status _wait_for_stream_load_pipe();
    Status _send_rpc_request(StreamLoadContext* context);

    BatchWriteId _batch_write_id;
    bthreads::ThreadPoolExecutor* _executor;

    std::mutex _mutex;
    std::condition_variable _cv;
    std::unordered_set<StreamLoadContext*> _alive_stream_load_pipe_ctxs;
    std::unordered_set<StreamLoadContext*> _dead_stream_load_pipe_ctxs;

    // Undocemented rule of bthread that -1(0xFFFFFFFFFFFFFFFF) is an invalid ExecutionQueueId
    constexpr static uint64_t kInvalidQueueId = (uint64_t)-1;
    bthread::ExecutionQueueId<Task> _queue_id{kInvalidQueueId};

    std::atomic<bool> _stopped{false};
};
using IsomorphicBatchWriteSharedPtr = std::shared_ptr<IsomorphicBatchWrite>;

} // namespace starrocks
