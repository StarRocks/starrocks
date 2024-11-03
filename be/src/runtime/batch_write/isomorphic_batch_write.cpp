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

#include "runtime/batch_write/isomorphic_batch_write.h"

#include <brpc/controller.h>
#include <fmt/format.h>

#include <utility>

#include "agent/master_info.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/bthreads/executor.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

std::ostream& operator<<(std::ostream& out, const BatchWriteId& id) {
    out << "db: " << id.db << ", table: " << id.table << ", load_params: {";
    bool first = true;
    for (const auto& [key, value] : id.load_params) {
        if (!first) {
            out << ",";
        }
        first = false;
        out << key << ":" << value;
    }
    out << "}";
    return out;
}

IsomorphicBatchWrite::IsomorphicBatchWrite(BatchWriteId batch_write_id, bthreads::ThreadPoolExecutor* executor)
        : _batch_write_id(std::move(batch_write_id)), _executor(executor) {}

Status IsomorphicBatchWrite::init() {
    bthread::ExecutionQueueOptions opts;
    opts.executor = _executor;
    if (int r = bthread::execution_queue_start(&_queue_id, &opts, _execute_bthread_tasks, this); r != 0) {
        LOG(ERROR) << "Fail to start execution queue for batch write, " << _batch_write_id << ", result: " << r;
        return Status::InternalError(fmt::format("fail to start bthread execution queue: {}", r));
    }
    LOG(INFO) << "Init batch write, " << _batch_write_id;
    return Status::OK();
}

void IsomorphicBatchWrite::stop() {
    {
        std::unique_lock<std::mutex> lock(_mutex);
        if (_stopped) {
            return;
        }
        _stopped = true;
    }
    if (_queue_id.value != kInvalidQueueId) {
        int r = bthread::execution_queue_stop(_queue_id);
        LOG_IF(WARNING, r != 0) << "Fail to stop execution queue, " << _batch_write_id << ", result: " << r;
        r = bthread::execution_queue_join(_queue_id);
        LOG_IF(WARNING, r != 0) << "Fail to join execution queue, " << _batch_write_id << ", result: " << r;
    }
    LOG(INFO) << "Stop batch write, " << _batch_write_id;
}

Status IsomorphicBatchWrite::register_stream_load_pipe(StreamLoadContext* pipe_ctx) {
    std::unique_lock<std::mutex> lock(_mutex);
    if (_stopped.load(std::memory_order_relaxed)) {
        return Status::ServiceUnavailable("Batch write is stopped");
    }
    if (_alive_stream_load_pipe_ctxs.emplace(pipe_ctx).second) {
        pipe_ctx->ref();
        _cv.notify_one();
    }
    VLOG(1) << "Register stream load pipe, " << _batch_write_id << ", txn_id: " << pipe_ctx->txn_id;
    return Status::OK();
}

void IsomorphicBatchWrite::unregister_stream_load_pipe(StreamLoadContext* pipe_ctx) {
    bool find = false;
    {
        std::unique_lock<std::mutex> lock(_mutex);
        if (_stopped.load(std::memory_order_relaxed)) {
            return;
        }
        find = _alive_stream_load_pipe_ctxs.erase(pipe_ctx) > 0;
        if (!find) {
            find = _dead_stream_load_pipe_ctxs.erase(pipe_ctx) > 0;
        }
    }
    if (find) {
        StreamLoadContext::release(pipe_ctx);
    }
    VLOG(1) << "Unregister stream load pipe, " << _batch_write_id << ", txn_id: " << pipe_ctx->txn_id;
}

Status IsomorphicBatchWrite::append_data(StreamLoadContext* data_ctx) {
    if (_stopped.load(std::memory_order_relaxed)) {
        return Status::ServiceUnavailable("Batch write is stopped");
    }
    Status result;
    auto count_down_latch = BThreadCountDownLatch(1);
    Task task{.create_time_ns = MonotonicNanos(), .data_ctx = data_ctx, .latch = &count_down_latch, .result = &result};
    int r = bthread::execution_queue_execute(_queue_id, task);
    if (r != 0) {
        LOG(ERROR) << "Fail to add task to execution queue, db: " << data_ctx->db << ", table: " << data_ctx->table
                   << ", id: " << data_ctx->id;
        return Status::InternalError("Failed to add task to execution queue");
    }
    // TODO timeout
    count_down_latch.wait();
    return result;
}

int IsomorphicBatchWrite::_execute_bthread_tasks(void* meta, bthread::TaskIterator<Task>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }

    auto batch_write = static_cast<IsomorphicBatchWrite*>(meta);
    for (; iter; ++iter) {
        *iter->result = batch_write->_execute_write(iter->data_ctx);
        iter->latch->count_down();
    }
    return 0;
}

Status IsomorphicBatchWrite::_execute_write(StreamLoadContext* data_ctx) {
    Status st;
    int num_retries = 0;
    while (num_retries <= config::batch_write_retry_num) {
        st = _write_data(data_ctx);
        if (st.ok()) {
            return st;
        }
        // TODO check if the error is retryable
        st = _send_rpc_request(data_ctx);
        st = _wait_for_stream_load_pipe();

        num_retries += 1;
    }
    return st;
}

Status IsomorphicBatchWrite::_write_data(StreamLoadContext* data_ctx) {
    // TODO write data outside the lock
    std::unique_lock<std::mutex> lock(_mutex);
    Status st;
    for (auto it = _alive_stream_load_pipe_ctxs.begin(); it != _alive_stream_load_pipe_ctxs.end();) {
        StreamLoadContext* pipe_ctx = *it;
        ByteBufferPtr buffer = data_ctx->buffer;
        st = pipe_ctx->body_sink->append(std::move(buffer));
        if (st.ok()) {
            data_ctx->buffer.reset();
            data_ctx->txn_id = pipe_ctx->txn_id;
            data_ctx->label = pipe_ctx->label;
            return st;
        }
        _dead_stream_load_pipe_ctxs.emplace(pipe_ctx);
        it = _alive_stream_load_pipe_ctxs.erase(it);
    }
    return st.ok() ? Status::CapacityLimitExceed("") : st;
}

Status IsomorphicBatchWrite::_wait_for_stream_load_pipe() {
    std::unique_lock<std::mutex> lock(_mutex);
    if (!_alive_stream_load_pipe_ctxs.empty()) {
        return Status::OK();
    }
    _cv.wait_for(lock, std::chrono::milliseconds(config::batch_write_request_interval_ms),
                 [&]() { return !_alive_stream_load_pipe_ctxs.empty(); });
    if (!_alive_stream_load_pipe_ctxs.empty()) {
        return Status::OK();
    }
    return Status::TimedOut("");
}

Status IsomorphicBatchWrite::_send_rpc_request(StreamLoadContext* context) {
    TNetworkAddress master_addr = get_master_address();
    TBatchWriteRequest request;
    request.__set_db(_batch_write_id.db);
    request.__set_tbl(_batch_write_id.table);
    request.__set_user(context->auth.user);
    request.__set_passwd(context->auth.passwd);
    request.__set_user_ip(context->auth.user_ip);
    auto backend_id = get_backend_id();
    if (backend_id.has_value()) {
        request.__set_backend_id(backend_id.value());
    }
    request.__set_backend_host(BackendOptions::get_localhost());
    request.__set_params(_batch_write_id.load_params);

    TBatchWriteResult response;
    Status st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &response](FrontendServiceConnection& client) { client->requestBatchWrite(response, request); },
            config::batch_write_reqeust_timeout_ms);
    return st.ok() ? Status(response.status) : st;
}

} // namespace starrocks