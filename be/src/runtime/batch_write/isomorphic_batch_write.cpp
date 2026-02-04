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
#include <bthread/bthread.h>
#include <fmt/format.h>

#include <atomic>
#include <utility>

#include "agent/master_info.h"
#include "base/testutil/sync_point.h"
#include "common/utils.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/internal_service.pb.h"
#include "http/http_common.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/time_bounded_stream_load_pipe.h"
#include "util/bthreads/executor.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

// Counter for total number of merge commit requests received
bvar::Adder<int64_t> g_mc_request_total("merge_commit", "request_total");
// Counter for total bytes of data received in merge commit requests
bvar::Adder<int64_t> g_mc_request_bytes("merge_commit", "request_bytes");
// Counter for successfully processed merge commit requests
bvar::Adder<int64_t> g_mc_success_total("merge_commit", "success_total");
// Counter for failed merge commit requests
bvar::Adder<int64_t> g_mc_fail_total("merge_commit", "fail_total");
// Counter for merge commit tasks currently waiting in the execution queue
bvar::Adder<int64_t> g_mc_pending_total("merge_commit", "pending_total");
// Counter for total bytes of data in pending merge commit tasks
bvar::Adder<int64_t> g_mc_pending_bytes("merge_commit", "pending_bytes");
// Counter for RPC requests sent to initiate new merge commit operations
bvar::Adder<int64_t> g_mc_send_rpc_total("merge_commit", "send_rpc_total");
// Counter for stream load pipes registered for merge commit operations
bvar::Adder<int64_t> g_mc_register_pipe_total("merge_commit", "register_pipe_total");
// Counter for stream load pipes unregistered for merge commit operations
bvar::Adder<int64_t> g_mc_unregister_pipe_total("merge_commit", "unregister_pipe_total");
// Latency recorder for end-to-end merge commit processing time (microseconds)
bvar::LatencyRecorder g_mc_total_latency_us("merge_commit", "request");
// Latency recorder for time tasks spend in pending queue before execution (microseconds)
bvar::LatencyRecorder g_mc_pending_latency_us("merge_commit", "pending");
// Latency recorder for combined RPC request time and pipe availability wait time (microseconds)
bvar::LatencyRecorder g_mc_wait_plan_latency_us("merge_commit", "wait_plan");
// Latency recorder for time spent appending data to stream load pipes (microseconds)
bvar::LatencyRecorder g_mc_append_pipe_latency_us("merge_commit", "append_pipe");
// Latency recorder for time spent waiting for load operations to complete (microseconds)
bvar::LatencyRecorder g_mc_wait_finish_latency_us("merge_commit", "wait_finish");

#define NS_TO_US(x) ((x) / 1000)

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

IsomorphicBatchWrite::IsomorphicBatchWrite(BatchWriteId batch_write_id, bthreads::ThreadPoolExecutor* executor,
                                           TxnStateCache* txn_state_cache)
        : _batch_write_id(std::move(batch_write_id)), _executor(executor), _txn_state_cache(txn_state_cache) {}

Status IsomorphicBatchWrite::init() {
    TEST_ERROR_POINT("IsomorphicBatchWrite::init::error");
    auto it = _batch_write_id.load_params.find(HTTP_MERGE_COMMIT_ASYNC);
    if (it != _batch_write_id.load_params.end()) {
        _batch_write_async = it->second == "true";
    }
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
        std::unique_lock<bthread::Mutex> lock(_mutex);
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
        std::unique_lock<bthread::Mutex> lock(_mutex);
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
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (_stopped.load(std::memory_order_acquire)) {
        return Status::ServiceUnavailable("Batch write is stopped");
    }
    if (_alive_stream_load_pipe_ctxs.emplace(pipe_ctx).second) {
        pipe_ctx->ref();
        _cv.notify_one();
        g_mc_register_pipe_total << 1;
    }
    TRACE_BATCH_WRITE << "Register stream load pipe, txn_id: " << pipe_ctx->txn_id << ", label: " << pipe_ctx->label
                      << ", load_id: " << print_id(pipe_ctx->id) << ", " << _batch_write_id;
    return Status::OK();
}

void IsomorphicBatchWrite::unregister_stream_load_pipe(StreamLoadContext* pipe_ctx) {
    bool find = false;
    {
        std::unique_lock<bthread::Mutex> lock(_mutex);
        find = _alive_stream_load_pipe_ctxs.erase(pipe_ctx) > 0;
        if (!find) {
            find = _dead_stream_load_pipe_ctxs.erase(pipe_ctx) > 0;
        }
    }
    TRACE_BATCH_WRITE << "Unregister stream load pipe, txn_id: " << pipe_ctx->txn_id << ", label: " << pipe_ctx->label
                      << ", load_id: " << print_id(pipe_ctx->id) << ", " << _batch_write_id << ", find: " << find;
    if (find) {
        StreamLoadContext::release(pipe_ctx);
        g_mc_unregister_pipe_total << 1;
    }
}

bool IsomorphicBatchWrite::contain_pipe(StreamLoadContext* pipe_ctx) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    auto it = _alive_stream_load_pipe_ctxs.find(pipe_ctx);
    if (it != _alive_stream_load_pipe_ctxs.end()) {
        return true;
    }
    return _dead_stream_load_pipe_ctxs.find(pipe_ctx) != _dead_stream_load_pipe_ctxs.end();
}

bool IsomorphicBatchWrite::is_pipe_alive(starrocks::StreamLoadContext* pipe_ctx) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    auto it = _alive_stream_load_pipe_ctxs.find(pipe_ctx);
    return it != _alive_stream_load_pipe_ctxs.end();
}

Status IsomorphicBatchWrite::append_data(StreamLoadContext* data_ctx) {
    g_mc_request_total << 1;
    g_mc_request_bytes << data_ctx->receive_bytes;
    bool success = false;
    long start_time_ns = MonotonicNanos();
    DeferOp defer([&] {
        if (success) {
            g_mc_success_total << 1;
        } else {
            g_mc_fail_total << 1;
        }
        g_mc_total_latency_us << NS_TO_US(MonotonicNanos() - start_time_ns);
    });
    if (_stopped.load(std::memory_order_acquire)) {
        return Status::ServiceUnavailable("Batch write is stopped");
    }
    RETURN_IF_ERROR(_create_and_wait_async_task(data_ctx));
    if (_batch_write_async) {
        success = true;
        return Status::OK();
    }
    Status status = _wait_for_load_finish(data_ctx);
    success = status.ok();
    return status;
}

Status IsomorphicBatchWrite::_create_and_wait_async_task(starrocks::StreamLoadContext* data_ctx) {
    AsyncAppendDataContext* async_ctx = new AsyncAppendDataContext(data_ctx);
    async_ctx->ref();
    async_ctx->create_time_ts.store(MonotonicNanos());
    g_mc_pending_total << 1;
    g_mc_pending_bytes << data_ctx->receive_bytes;
    DeferOp defer([&] { AsyncAppendDataContext::release(async_ctx); });
    Task task{.context = async_ctx};
    // this reference is for async task
    async_ctx->ref();
    int r = bthread::execution_queue_execute(_queue_id, task);
    if (r != 0) {
        g_mc_pending_total << -1;
        g_mc_pending_bytes << -data_ctx->receive_bytes;
        AsyncAppendDataContext::release(async_ctx);
        LOG(ERROR) << "Fail to add task, " << _batch_write_id << ", user label: " << data_ctx->label << ", ret: " << r;
        return Status::InternalError(fmt::format("Failed to add task to execution queue, result: {}", r));
    }
    async_ctx->latch().wait();
    async_ctx->total_cost_ns.store(MonotonicNanos() - async_ctx->create_time_ts);
    TRACE_BATCH_WRITE << "wait async finish, " << _batch_write_id << ", user label: " << async_ctx->data_ctx()->label
                      << ", user ip: " << data_ctx->auth.user_ip << ", data size: " << data_ctx->receive_bytes
                      << ", total_cost: " << NS_TO_US(async_ctx->total_cost_ns)
                      << "us, total_async_cost: " << NS_TO_US(async_ctx->total_async_cost_ns)
                      << "us, task_pending_cost: " << NS_TO_US(async_ctx->task_pending_cost_ns)
                      << "us, append_pipe_cost: " << NS_TO_US(async_ctx->append_pipe_cost_ns)
                      << "us, rpc_cost: " << NS_TO_US(async_ctx->rpc_cost_ns)
                      << "us, wait_pipe_cost: " << NS_TO_US(async_ctx->wait_pipe_cost_ns)
                      << "us, num retries: " << async_ctx->num_retries
                      << ", pipe_left_active: " << NS_TO_US(async_ctx->pipe_left_active_ns)
                      << ", async_status: " << async_ctx->get_status() << ", txn_id: " << async_ctx->txn_id()
                      << ", label: " << async_ctx->label();
    data_ctx->txn_id = async_ctx->txn_id();
    data_ctx->batch_write_label = async_ctx->label();
    data_ctx->mc_pending_cost_nanos = async_ctx->task_pending_cost_ns;
    data_ctx->mc_wait_plan_cost_nanos = async_ctx->wait_pipe_cost_ns + async_ctx->rpc_cost_ns;
    data_ctx->mc_write_data_cost_nanos = async_ctx->append_pipe_cost_ns;
    data_ctx->mc_left_merge_time_nanos = async_ctx->pipe_left_active_ns;
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
        g_mc_pending_total << -1;
        g_mc_pending_bytes << -ctx->data_ctx()->receive_bytes;
        g_mc_pending_latency_us << NS_TO_US(ctx->task_pending_cost_ns);
        auto st = batch_write->_execute_write(ctx);
        ctx->finish_async(st);
        ctx->total_async_cost_ns.store(MonotonicNanos() - start_ts);
        TRACE_BATCH_WRITE << "async task finish, " << batch_write->_batch_write_id
                          << ", user label: " << ctx->data_ctx()->label
                          << ", data size: " << ctx->data_ctx()->receive_bytes
                          << ", total_async_cost: " << NS_TO_US(ctx->total_async_cost_ns)
                          << "us, task_pending_cost: " << NS_TO_US(ctx->task_pending_cost_ns)
                          << "us, append_pipe_cost: " << NS_TO_US(ctx->append_pipe_cost_ns)
                          << "us, rpc_cost: " << NS_TO_US(ctx->rpc_cost_ns)
                          << "us, wait_pipe_cost: " << NS_TO_US(ctx->wait_pipe_cost_ns)
                          << "us, num retries: " << ctx->num_retries
                          << ", pipe_left_active: " << NS_TO_US(ctx->pipe_left_active_ns) << "us, status: " << st
                          << ", txn_id: " << ctx->txn_id() << ", label: " << ctx->label();
        ;
        AsyncAppendDataContext::release(ctx);
    }
    return 0;
}

Status IsomorphicBatchWrite::_execute_write(AsyncAppendDataContext* async_ctx) {
    int64_t write_data_cost_ns = 0;
    int64_t rpc_cost_ns = 0;
    int64_t wait_pipe_cost_ns = 0;
    int num_retries = 0;
    Status st;
    while (true) {
        if (_stopped.load(std::memory_order_acquire)) {
            st = Status::ServiceUnavailable("Batch write is stopped");
            break;
        }
        {
            SCOPED_RAW_TIMER(&write_data_cost_ns);
            st = _write_data_to_pipe(async_ctx);
        }
        if (st.ok() || num_retries >= config::merge_commit_rpc_request_retry_num) {
            break;
        }
        num_retries += 1;
        {
            SCOPED_RAW_TIMER(&rpc_cost_ns);
            st = _send_rpc_request(async_ctx->data_ctx());
            g_mc_send_rpc_total << 1;
            if (!st.ok()) {
                break;
            }
        }
        {
            SCOPED_RAW_TIMER(&wait_pipe_cost_ns);
            std::unique_lock<bthread::Mutex> lock(_mutex);
            if (_alive_stream_load_pipe_ctxs.empty()) {
                _cv.wait_for(lock, config::merge_commit_rpc_request_retry_interval_ms * 1000);
            }
        }
    }
    async_ctx->append_pipe_cost_ns.store(write_data_cost_ns);
    async_ctx->rpc_cost_ns.store(rpc_cost_ns);
    async_ctx->wait_pipe_cost_ns.store(wait_pipe_cost_ns);
    async_ctx->num_retries.store(num_retries);
    g_mc_append_pipe_latency_us << NS_TO_US(write_data_cost_ns);
    g_mc_wait_plan_latency_us << NS_TO_US(rpc_cost_ns + wait_pipe_cost_ns);
    if (!st.ok()) {
        std::stringstream stream;
        stream << "Failed to write data to stream load pipe, num retry: " << num_retries
               << ", write_data: " << NS_TO_US(write_data_cost_ns) << " us, rpc: " << NS_TO_US(rpc_cost_ns)
               << "us, wait_pipe: " << NS_TO_US(wait_pipe_cost_ns) << " us, last error: " << st;
        st = Status::InternalError(stream.str());
    }
    return st;
}

Status IsomorphicBatchWrite::_write_data_to_pipe(AsyncAppendDataContext* async_ctx) {
    StreamLoadContext* data_ctx = async_ctx->data_ctx();
    while (true) {
        StreamLoadContext* pipe_ctx;
        {
            std::unique_lock<bthread::Mutex> lock(_mutex);
            if (!_alive_stream_load_pipe_ctxs.empty()) {
                pipe_ctx = *(_alive_stream_load_pipe_ctxs.begin());
                // take a reference to avoid being released when appending data to the pipe outside the lock
                pipe_ctx->ref();
            } else {
                return Status::CapacityLimitExceed("No available stream load pipe");
            }
        }
        DeferOp defer([&] { StreamLoadContext::release(pipe_ctx); });
        // task a reference to avoid being released by the pipe if append fails
        ByteBufferPtr buffer = data_ctx->buffer;
        Status st = pipe_ctx->body_sink->append(std::move(buffer));
        if (st.ok()) {
            data_ctx->buffer.reset();
            async_ctx->pipe_left_active_ns.store(
                    static_cast<TimeBoundedStreamLoadPipe*>(pipe_ctx->body_sink.get())->left_active_ns());
            async_ctx->set_txn(pipe_ctx->txn_id, pipe_ctx->label);
            return Status::OK();
        }
        TRACE_BATCH_WRITE << "Fail to append data to stream load pipe, " << _batch_write_id
                          << ", user label: " << data_ctx->label << ", txn_id: " << pipe_ctx->txn_id
                          << ", label: " << pipe_ctx->label << ", status: " << st;
        // if failed, the pipe can't be appended anymore and move it from
        // the alive to the dead, and wait for being unregistered
        {
            std::unique_lock<bthread::Mutex> lock(_mutex);
            if (_alive_stream_load_pipe_ctxs.erase(pipe_ctx)) {
                _dead_stream_load_pipe_ctxs.emplace(pipe_ctx);
            }
        }
    }
}

Status IsomorphicBatchWrite::_send_rpc_request(StreamLoadContext* data_ctx) {
    TNetworkAddress master_addr = get_master_address();
    TMergeCommitRequest request;
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

    TMergeCommitResult response;
    Status st;

#ifndef BE_TEST
    int64_t start_ts = MonotonicNanos();
    st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &response](FrontendServiceConnection& client) { client->requestMergeCommit(response, request); },
            config::merge_commit_rpc_reqeust_timeout_ms);
    TRACE_BATCH_WRITE << "receive requestBatchWrite response, " << _batch_write_id
                      << ", user label: " << data_ctx->label << ", master: " << master_addr
                      << ", cost: " << NS_TO_US(MonotonicNanos() - start_ts) << "us, status: " << st
                      << ", response: " << response;
#else
    TEST_SYNC_POINT_CALLBACK("IsomorphicBatchWrite::send_rpc_request::request", &request);
    TEST_SYNC_POINT_CALLBACK("IsomorphicBatchWrite::send_rpc_request::status", &st);
    TEST_SYNC_POINT_CALLBACK("IsomorphicBatchWrite::send_rpc_request::response", &response);
#endif

    return st.ok() ? Status(response.status) : st;
}

Status IsomorphicBatchWrite::_wait_for_load_finish(StreamLoadContext* data_ctx) {
    int64_t total_timeout_ms =
            data_ctx->timeout_second > 0 ? data_ctx->timeout_second * 1000 : config::merge_commit_default_timeout_ms;
    int64_t left_timeout_ms =
            std::max((int64_t)0, total_timeout_ms - (MonotonicNanos() - data_ctx->start_nanos) / 1000000);
    StatusOr<TxnStateSubscriberPtr> subscriber_status = _txn_state_cache->subscribe_state(
            data_ctx->txn_id, data_ctx->label, data_ctx->db, data_ctx->table, data_ctx->auth);
    if (!subscriber_status.ok()) {
        return Status::InternalError("Failed to create txn state subscriber, " +
                                     subscriber_status.status().to_string());
    }
    TxnStateSubscriberPtr subscriber = std::move(subscriber_status.value());
    int64_t start_ts = MonotonicNanos();
    StatusOr<TxnState> status_or = subscriber->wait_finished_state(left_timeout_ms * 1000);
    data_ctx->mc_wait_finish_cost_nanos = MonotonicNanos() - start_ts;
    g_mc_wait_finish_latency_us << NS_TO_US(data_ctx->mc_wait_finish_cost_nanos);
    TRACE_BATCH_WRITE << "finish to wait load, " << _batch_write_id << ", user label: " << data_ctx->label
                      << ", txn_id: " << data_ctx->txn_id << ", load label: " << data_ctx->batch_write_label
                      << ", cost: " << NS_TO_US(data_ctx->mc_wait_finish_cost_nanos)
                      << "us, wait status: " << status_or.status() << ", "
                      << (status_or.ok() ? status_or.value() : subscriber->current_state());
    if (!status_or.ok()) {
        TxnState current_state = subscriber->current_state();
        return Status::InternalError(fmt::format("Failed to get load final status, current status: {}, error: {}",
                                                 to_string(current_state.txn_status), status_or.status().to_string()));
    }
    switch (status_or.value().txn_status) {
    case TTransactionStatus::COMMITTED:
        return Status::PublishTimeout("Load has not been published before timeout");
    case TTransactionStatus::VISIBLE:
        return Status::OK();
    case TTransactionStatus::ABORTED:
        return Status::InternalError("Load is aborted, reason: " + status_or.value().reason);
    case TTransactionStatus::UNKNOWN:
        return Status::InternalError("Can't find the transaction, reason: " + status_or.value().reason);
    default:
        return Status::InternalError("Load status is not final: " + to_string(status_or.value().txn_status));
    }
}

} // namespace starrocks