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

#include <atomic>
#include <utility>

#include "agent/master_info.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/time_bounded_stream_load_pipe.h"
#include "testutil/sync_point.h"
#include "util/bthreads/executor.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

class AsyncAppendDataContext {
public:
    AsyncAppendDataContext(StreamLoadContext* data_ctx) : _data_ctx(data_ctx), _latch(1) { data_ctx->ref(); }

    ~AsyncAppendDataContext() { StreamLoadContext::release(_data_ctx); }

    AsyncAppendDataContext(const AsyncAppendDataContext&) = delete;
    void operator=(const AsyncAppendDataContext&) = delete;
    AsyncAppendDataContext(AsyncAppendDataContext&&) = delete;
    void operator=(AsyncAppendDataContext&&) = delete;

    const BThreadCountDownLatch& latch() const { return _latch; }

    StreamLoadContext* data_ctx() { return _data_ctx; }

    void set_txn(int64_t txn_id, const std::string& label) {
        std::lock_guard l(_result_lock);
        _txn_id = txn_id;
        _label = label;
    }

    // called after async process finishes
    void finish_async(const Status& status) {
        {
            std::lock_guard l(_result_lock);
            if (_async_finished) {
                return;
            }
            _async_finished = true;
            _status = status;
        }
        _latch.count_down();
    }

    bool async_finished() const {
        std::lock_guard l(_result_lock);
        return _async_finished;
    }

    Status get_status() {
        std::lock_guard l(_result_lock);
        return _status;
    }

    int64_t txn_id() const {
        std::lock_guard l(_result_lock);
        return _txn_id;
    }

    const std::string& label() const {
        std::lock_guard l(_result_lock);
        return _label;
    }

    void ref() { _refs.fetch_add(1); }
    // If unref() returns true, this object should be delete
    bool unref() { return _refs.fetch_sub(1) == 1; }

    static void release(AsyncAppendDataContext* ctx) {
        if (ctx != nullptr && ctx->unref()) {
            delete ctx;
        }
    }

private:
    StreamLoadContext* const _data_ctx;
    BThreadCountDownLatch _latch;
    std::atomic<int> _refs{0};
    mutable bthread::Mutex _result_lock;
    bool _async_finished{false};
    Status _status;
    // the txn and label that data belongs to if success (_status is OK)
    int64_t _txn_id{-1};
    std::string _label;

public:
    // statistics =======================
    std::atomic_long create_time_ts{-1};
    std::atomic_long task_pending_cost_ns{-1};
    std::atomic_long total_async_cost_ns{-1};
    std::atomic_long append_pipe_cost_ns{-1};
    std::atomic_long rpc_cost_ns{-1};
    std::atomic_long wait_pipe_cost_ns{-1};
    std::atomic_long pipe_left_active_ns{-1};
    std::atomic_long total_cost_ns{-1};
    std::atomic_int num_retries{-1};
};

IsomorphicBatchWrite::IsomorphicBatchWrite(BatchWriteId batch_write_id, bthreads::ThreadPoolExecutor* executor)
        : _batch_write_id(std::move(batch_write_id)), _executor(executor) {}

Status IsomorphicBatchWrite::init() {
    TEST_ERROR_POINT("IsomorphicBatchWrite::init::error");
    bthread::ExecutionQueueOptions opts;
    opts.executor = _executor;
    if (int r = bthread::execution_queue_start(&_queue_id, &opts, _execute_tasks, this); r != 0) {
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

    std::vector<StreamLoadContext*> release_contexts;
    {
        std::unique_lock<std::mutex> lock(_mutex);
        release_contexts.insert(release_contexts.end(), _alive_stream_load_pipe_ctxs.begin(),
                                _alive_stream_load_pipe_ctxs.end());
        release_contexts.insert(release_contexts.end(), _dead_stream_load_pipe_ctxs.begin(),
                                _dead_stream_load_pipe_ctxs.end());
        _alive_stream_load_pipe_ctxs.clear();
        _dead_stream_load_pipe_ctxs.clear();
    }
    for (StreamLoadContext* ctx : release_contexts) {
        LOG(INFO) << "Stop stream load pipe, txn_id: " << ctx->txn_id << ", label: " << ctx->label
                  << ", load_id: " << print_id(ctx->id) << ", " << _batch_write_id;
        StreamLoadContext::release(ctx);
    }
    LOG(INFO) << "Stop batch write, " << _batch_write_id;
}

Status IsomorphicBatchWrite::register_stream_load_pipe(StreamLoadContext* pipe_ctx) {
    std::unique_lock<std::mutex> lock(_mutex);
    if (_stopped.load(std::memory_order_acquire)) {
        return Status::ServiceUnavailable("Batch write is stopped");
    }
    if (_alive_stream_load_pipe_ctxs.emplace(pipe_ctx).second) {
        pipe_ctx->ref();
        _cv.notify_one();
    }
    TRACE_BATCH_WRITE << "Register stream load pipe, txn_id: " << pipe_ctx->txn_id << ", label: " << pipe_ctx->label
                      << ", load_id: " << print_id(pipe_ctx->id) << ", " << _batch_write_id;
    return Status::OK();
}

void IsomorphicBatchWrite::unregister_stream_load_pipe(StreamLoadContext* pipe_ctx) {
    bool find = false;
    {
        std::unique_lock<std::mutex> lock(_mutex);
        find = _alive_stream_load_pipe_ctxs.erase(pipe_ctx) > 0;
        if (!find) {
            find = _dead_stream_load_pipe_ctxs.erase(pipe_ctx) > 0;
        }
    }
    TRACE_BATCH_WRITE << "Unregister stream load pipe, txn_id: " << pipe_ctx->txn_id << ", label: " << pipe_ctx->label
                      << ", load_id: " << print_id(pipe_ctx->id) << ", " << _batch_write_id << ", find: " << find;
    if (find) {
        StreamLoadContext::release(pipe_ctx);
    }
}

bool IsomorphicBatchWrite::contain_pipe(StreamLoadContext* pipe_ctx) {
    std::unique_lock<std::mutex> lock(_mutex);
    auto it = _alive_stream_load_pipe_ctxs.find(pipe_ctx);
    if (it != _alive_stream_load_pipe_ctxs.end()) {
        return true;
    }
    return _dead_stream_load_pipe_ctxs.find(pipe_ctx) != _dead_stream_load_pipe_ctxs.end();
}

Status IsomorphicBatchWrite::append_data(StreamLoadContext* data_ctx) {
    if (_stopped.load(std::memory_order_acquire)) {
        return Status::ServiceUnavailable("Batch write is stopped");
    }
    AsyncAppendDataContext* async_ctx = new AsyncAppendDataContext(data_ctx);
    async_ctx->ref();
    async_ctx->create_time_ts.store(MonotonicNanos());
    DeferOp defer([&] { AsyncAppendDataContext::release(async_ctx); });
    Task task{.context = async_ctx};
    // this reference is for async task
    async_ctx->ref();
    int r = bthread::execution_queue_execute(_queue_id, task);
    if (r != 0) {
        AsyncAppendDataContext::release(async_ctx);
        LOG(ERROR) << "Fail to add task to execution queue, " << _batch_write_id << ", user label: " << data_ctx->label
                   << ", result: " << r;
        return Status::InternalError(fmt::format("Failed to add task to execution queue, result: {}", r));
    }
    // TODO timeout
    async_ctx->latch().wait();
    async_ctx->total_cost_ns.store(MonotonicNanos() - async_ctx->create_time_ts);
    TRACE_BATCH_WRITE << "append data finish, " << _batch_write_id << ", user label: " << async_ctx->data_ctx()->label
                      << ", data size: " << data_ctx->receive_bytes
                      << ", total_cost: " << (async_ctx->total_cost_ns / 1000)
                      << "us, total_async_cost: " << (async_ctx->total_async_cost_ns / 1000)
                      << "us, task_pending_cost: " << (async_ctx->task_pending_cost_ns / 1000)
                      << "us, append_pipe_cost: " << (async_ctx->append_pipe_cost_ns / 1000)
                      << "us, rpc_cost: " << (async_ctx->rpc_cost_ns / 1000)
                      << "us, wait_pipe_cost: " << (async_ctx->wait_pipe_cost_ns / 1000)
                      << "us, num retries: " << async_ctx->num_retries
                      << ", pipe_left_active_us: " << (async_ctx->pipe_left_active_ns / 1000)
                      << "us, async_finished: " << async_ctx->async_finished()
                      << ", async_status: " << async_ctx->get_status() << ", txn_id: " << async_ctx->txn_id()
                      << ", label: " << async_ctx->label();
    bool async_finished = async_ctx->async_finished();
    if (async_finished && async_ctx->get_status().ok()) {
        data_ctx->txn_id = async_ctx->txn_id();
        data_ctx->batch_write_label = async_ctx->label();
        data_ctx->batch_left_time_nanos = async_ctx->pipe_left_active_ns;
    }
    return async_ctx->get_status();
}

int IsomorphicBatchWrite::_execute_tasks(void* meta, bthread::TaskIterator<Task>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }

    auto batch_write = static_cast<IsomorphicBatchWrite*>(meta);
    for (; iter; ++iter) {
        int64_t start_ts = MonotonicNanos();
        AsyncAppendDataContext* ctx = iter->context;
        ctx->task_pending_cost_ns.store(MonotonicNanos() - ctx->create_time_ts);
        auto st = batch_write->_execute_write(ctx);
        ctx->finish_async(st);
        ctx->total_async_cost_ns.store(MonotonicNanos() - start_ts);
        TRACE_BATCH_WRITE << "async task finish, " << batch_write->_batch_write_id
                          << ", user label: " << ctx->data_ctx()->label
                          << ", data size: " << ctx->data_ctx()->receive_bytes
                          << ", total_async_cost: " << (ctx->total_async_cost_ns / 1000)
                          << "us, task_pending_cost: " << (ctx->task_pending_cost_ns / 1000)
                          << "us, append_pipe_cost: " << (ctx->append_pipe_cost_ns / 1000)
                          << "us, rpc_cost: " << (ctx->rpc_cost_ns / 1000)
                          << "us, wait_pipe_cost: " << (ctx->wait_pipe_cost_ns / 1000)
                          << "us, num retries: " << ctx->num_retries
                          << ", pipe_left_active_us: " << (ctx->pipe_left_active_ns / 1000) << "us, status: " << st
                          << ", txn_id: " << ctx->txn_id() << ", label: " << ctx->label();
        ;
        AsyncAppendDataContext::release(ctx);
    }
    return 0;
}

Status IsomorphicBatchWrite::_execute_write(AsyncAppendDataContext* async_ctx) {
    if (_stopped.load(std::memory_order_acquire)) {
        return Status::ServiceUnavailable("Batch write is stopped");
    }
    Status st;
    int64_t append_pipe_cost_ns = 0;
    int64_t rpc_cost_ns = 0;
    int64_t wait_pipe_cost_ns = 0;
    int num_retries = 0;
    while (num_retries <= config::batch_write_rpc_request_retry_num) {
        int64_t append_ts = MonotonicNanos();
        st = _write_data(async_ctx);
        int64_t rpc_ts = MonotonicNanos();
        append_pipe_cost_ns += rpc_ts - append_ts;
        if (st.ok()) {
            break;
        }
        // TODO check if the error is retryable
        st = _send_rpc_request(async_ctx->data_ctx());
        int64_t wait_ts = MonotonicNanos();
        rpc_cost_ns += wait_ts - rpc_ts;
        st = _wait_for_stream_load_pipe();
        wait_pipe_cost_ns += MonotonicNanos() - wait_ts;
        num_retries += 1;
    }
    async_ctx->append_pipe_cost_ns.store(append_pipe_cost_ns);
    async_ctx->rpc_cost_ns.store(rpc_cost_ns);
    async_ctx->wait_pipe_cost_ns.store(wait_pipe_cost_ns);
    async_ctx->num_retries.store(num_retries);
    return st;
}

Status IsomorphicBatchWrite::_write_data(AsyncAppendDataContext* async_ctx) {
    // TODO write data outside the lock
    std::unique_lock<std::mutex> lock(_mutex);
    Status st;
    StreamLoadContext* data_ctx = async_ctx->data_ctx();
    for (auto it = _alive_stream_load_pipe_ctxs.begin(); it != _alive_stream_load_pipe_ctxs.end();) {
        StreamLoadContext* pipe_ctx = *it;
        // add reference to the buffer to avoid being released if append fails
        ByteBufferPtr buffer = data_ctx->buffer;
        st = pipe_ctx->body_sink->append(std::move(buffer));
        if (st.ok()) {
            data_ctx->buffer.reset();
            async_ctx->pipe_left_active_ns.store(
                    static_cast<TimeBoundedStreamLoadPipe*>(pipe_ctx->body_sink.get())->left_active_ns());
            async_ctx->set_txn(pipe_ctx->txn_id, pipe_ctx->label);
            return st;
        }
        _dead_stream_load_pipe_ctxs.emplace(pipe_ctx);
        it = _alive_stream_load_pipe_ctxs.erase(it);
    }
    return st.ok() ? Status::CapacityLimitExceed("") : st;
}

Status IsomorphicBatchWrite::_wait_for_stream_load_pipe() {
    std::unique_lock<std::mutex> lock(_mutex);
    _cv.wait_for(lock, std::chrono::milliseconds(config::batch_write_rpc_request_retry_interval_ms),
                 [&]() { return !_alive_stream_load_pipe_ctxs.empty(); });
    if (!_alive_stream_load_pipe_ctxs.empty()) {
        return Status::OK();
    }
    return Status::TimedOut("");
}

Status IsomorphicBatchWrite::_send_rpc_request(StreamLoadContext* data_ctx) {
    TNetworkAddress master_addr = get_master_address();
    TBatchWriteRequest request;
    request.__set_db(_batch_write_id.db);
    request.__set_tbl(_batch_write_id.table);
    request.__set_user(data_ctx->auth.user);
    request.__set_passwd(data_ctx->auth.passwd);
    request.__set_user_ip(data_ctx->auth.user_ip);
    auto backend_id = get_backend_id();
    if (backend_id.has_value()) {
        request.__set_backend_id(backend_id.value());
    }
    request.__set_backend_host(BackendOptions::get_localhost());
    request.__set_params(_batch_write_id.load_params);

    TBatchWriteResult response;
    Status st;

#ifndef BE_TEST
    int64_t start_ts = MonotonicNanos();
    st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &response](FrontendServiceConnection& client) { client->requestBatchWrite(response, request); },
            config::batch_write_rpc_reqeust_timeout_ms);
    TRACE_BATCH_WRITE << "receive rpc response, " << _batch_write_id << ", user label: " << data_ctx->label
                      << ", master: " << master_addr << ", cost: " << ((MonotonicNanos() - start_ts) / 1000)
                      << "us, status: " << st << ", response: " << response;
#else
    TEST_SYNC_POINT_CALLBACK("IsomorphicBatchWrite::send_rpc_request::request", &request);
    TEST_SYNC_POINT_CALLBACK("IsomorphicBatchWrite::send_rpc_request::status", &st);
    TEST_SYNC_POINT_CALLBACK("IsomorphicBatchWrite::send_rpc_request::response", &response);
#endif

    return st.ok() ? Status(response.status) : st;
}

} // namespace starrocks