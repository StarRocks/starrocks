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

#include <unordered_map>

#include "common/statusor.h"
#include "runtime/batch_write/isomorphic_batch_write.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/bthreads/bthread_shared_mutex.h"
#include "util/bthreads/executor.h"

namespace brpc {
class Controller;
}

namespace starrocks {

class ExecEnv;
class PStreamLoadRequest;
class PStreamLoadResponse;
class StreamLoadContext;

class BatchWriteMgr {
public:
    BatchWriteMgr(std::unique_ptr<bthreads::ThreadPoolExecutor> executor) : _executor(std::move(executor)){};

    Status register_stream_load_pipe(StreamLoadContext* pipe_ctx);
    void unregister_stream_load_pipe(StreamLoadContext* pipe_ctx);
    StatusOr<IsomorphicBatchWriteSharedPtr> get_batch_write(const BatchWriteId& batch_write_id);

    Status append_data(StreamLoadContext* data_ctx);

    void stop();

    static StatusOr<StreamLoadContext*> create_and_register_pipe(
            ExecEnv* exec_env, BatchWriteMgr* batch_write_mgr, const string& db, const string& table,
            const std::map<std::string, std::string>& load_parameters, const string& label, long txn_id,
            const TUniqueId& load_id, int32_t batch_write_interval_ms);

    static void receive_stream_load_rpc(ExecEnv* exec_env, brpc::Controller* cntl, const PStreamLoadRequest* request,
                                        PStreamLoadResponse* response);

private:
    StatusOr<IsomorphicBatchWriteSharedPtr> _get_batch_write(const BatchWriteId& batch_write_id,
                                                             bool create_if_missing);

    std::unique_ptr<bthreads::ThreadPoolExecutor> _executor;
    bthreads::BThreadSharedMutex _rw_mutex;
    std::unordered_map<BatchWriteId, IsomorphicBatchWriteSharedPtr, BatchWriteIdHash, BatchWriteIdEqual>
            _batch_write_map;
    bool _stopped{false};
};

} // namespace starrocks
