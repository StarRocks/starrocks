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

#include <gtest/gtest.h>

#include "gen_cpp/FrontendService.h"
#include "http/http_common.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/time_bounded_stream_load_pipe.h"
#include "testutil/assert.h"
#include "testutil/sync_point.h"
#include "util/bthreads/executor.h"
#include "util/monotime.h"
#include "util/threadpool.h"

namespace starrocks {

class IsomorphicBatchWriteTest : public testing::Test {
public:
    IsomorphicBatchWriteTest() = default;
    ~IsomorphicBatchWriteTest() override = default;
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();
        std::unique_ptr<ThreadPool> thread_pool;
        ASSERT_OK(ThreadPoolBuilder("IsomorphicBatchWriteTest")
                          .set_min_threads(0)
                          .set_max_threads(1)
                          .set_max_queue_size(2048)
                          .set_idle_timeout(MonoDelta::FromMilliseconds(10000))
                          .build(&thread_pool));
        _executor = std::make_unique<bthreads::ThreadPoolExecutor>(thread_pool.release(), kTakesOwnership);
    }

    void TearDown() override {
        for (auto* ctx : _to_release_contexts) {
            StreamLoadContext::release(ctx);
        }
    }

    StreamLoadContext* build_pipe_context(const std::string& label, int64_t txn_id, const BatchWriteId& batch_write_id,
                                          std::shared_ptr<TimeBoundedStreamLoadPipe> pipe) {
        StreamLoadContext* ctx = new StreamLoadContext(_exec_env);
        ctx->ref();
        ctx->db = batch_write_id.db;
        ctx->table = batch_write_id.table;
        ctx->label = std::move(label);
        ctx->txn_id = txn_id;
        ctx->body_sink = pipe;
        ctx->enable_batch_write = true;
        ctx->load_parameters = batch_write_id.load_params;
        _to_release_contexts.emplace(ctx);
        return ctx;
    }

    StreamLoadContext* build_data_context(const BatchWriteId& batch_write_id, const std::string& data) {
        StreamLoadContext* ctx = new StreamLoadContext(_exec_env);
        ctx->ref();
        ctx->db = batch_write_id.db;
        ctx->table = batch_write_id.table;
        ctx->enable_batch_write = true;
        auto timeout_it = batch_write_id.load_params.find(HTTP_TIMEOUT);
        if (timeout_it != batch_write_id.load_params.end()) {
            ctx->timeout_second = std::stoi(timeout_it->second);
        }
        ctx->load_parameters = batch_write_id.load_params;
        auto buf = ByteBuffer::allocate_with_tracker(64).value();
        buf->put_bytes(data.c_str(), data.size());
        buf->flip();
        ctx->buffer = buf;
        _to_release_contexts.emplace(ctx);
        return ctx;
    }

    void test_append_data_sync_base(const Status& rpc_status, TTransactionStatus::type txn_status,
                                    const Status& expect_st);

protected:
    ExecEnv* _exec_env;
    std::unique_ptr<bthreads::ThreadPoolExecutor> _executor;
    std::unordered_set<StreamLoadContext*> _to_release_contexts;
};

void verify_data(std::string expected, ByteBufferPtr actual) {
    ASSERT_EQ(expected.size(), actual->limit);
    for (int i = 0; i < actual->pos; ++i) {
        ASSERT_EQ(expected[i], *(actual->ptr + i));
    }
}

TEST_F(IsomorphicBatchWriteTest, register_and_unregister_pipe) {
    BatchWriteId batch_write_id{.db = "db", .table = "table", .load_params = {}};
    IsomorphicBatchWriteSharedPtr batch_write = std::make_shared<IsomorphicBatchWrite>(batch_write_id, _executor.get());
    ASSERT_OK(batch_write->init());
    DeferOp defer_writer([&] { batch_write->stop(); });

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("StreamLoadContext::release");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    StreamLoadContext* pipe_ctx1 =
            build_pipe_context("label1", 1, batch_write_id, std::make_shared<TimeBoundedStreamLoadPipe>("p1", 1000));
    StreamLoadContext* pipe_ctx2 =
            build_pipe_context("label2", 2, batch_write_id, std::make_shared<TimeBoundedStreamLoadPipe>("p2", 1000));

    ASSERT_OK(batch_write->register_stream_load_pipe(pipe_ctx1));
    ASSERT_EQ(2, pipe_ctx1->num_refs());
    ASSERT_TRUE(batch_write->contain_pipe(pipe_ctx1));
    ASSERT_OK(batch_write->register_stream_load_pipe(pipe_ctx2));
    ASSERT_EQ(2, pipe_ctx2->num_refs());
    ASSERT_TRUE(batch_write->contain_pipe(pipe_ctx2));

    batch_write->unregister_stream_load_pipe(pipe_ctx1);
    ASSERT_FALSE(batch_write->contain_pipe(pipe_ctx1));
    ASSERT_EQ(1, pipe_ctx1->num_refs());
    batch_write->unregister_stream_load_pipe(pipe_ctx2);
    ASSERT_FALSE(batch_write->contain_pipe(pipe_ctx2));
    ASSERT_EQ(1, pipe_ctx2->num_refs());
}

TEST_F(IsomorphicBatchWriteTest, append_data_async) {
    BatchWriteId batch_write_id{.db = "db", .table = "table", .load_params = {{HTTP_MERGE_COMMIT_ASYNC, "true"}}};
    IsomorphicBatchWriteSharedPtr batch_write = std::make_shared<IsomorphicBatchWrite>(batch_write_id, _executor.get());
    ASSERT_OK(batch_write->init());
    DeferOp defer_writer([&] { batch_write->stop(); });

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TimeBoundedStreamLoadPipe::get_current_ns");
        SyncPoint::GetInstance()->ClearCallBack("IsomorphicBatchWrite::send_rpc_request::request");
        SyncPoint::GetInstance()->ClearCallBack("IsomorphicBatchWrite::send_rpc_request::status");
        SyncPoint::GetInstance()->ClearCallBack("IsomorphicBatchWrite::send_rpc_request::response");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    int num_rpc_request = 0;
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::request",
                                          [&](void* arg) { num_rpc_request += 1; });

    SyncPoint::GetInstance()->SetCallBack("TimeBoundedStreamLoadPipe::get_current_ns",
                                          [&](void* arg) { *((int64_t*)arg) = 0; });
    StreamLoadContext* pipe_ctx1 =
            build_pipe_context("label1", 1, batch_write_id, std::make_shared<TimeBoundedStreamLoadPipe>("p1", 1000));
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::status",
                                          [&](void* arg) { *((Status*)arg) = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::response", [&](void* arg) {
        TMergeCommitResult* result = (TMergeCommitResult*)arg;
        TStatus status;
        status.__set_status_code(TStatusCode::OK);
        result->__set_status(status);
        result->__set_label("label1");
        ASSERT_OK(batch_write->register_stream_load_pipe(pipe_ctx1));
    });

    StreamLoadContext* data_ctx1 = build_data_context(batch_write_id, "data1");
    ASSERT_OK(batch_write->append_data(data_ctx1));
    ASSERT_EQ(1, num_rpc_request);
    auto read_data1 = static_cast<TimeBoundedStreamLoadPipe*>(pipe_ctx1->body_sink.get())->read();
    ASSERT_OK(read_data1.status());
    verify_data("data1", read_data1.value());

    StreamLoadContext* data_ctx2 = build_data_context(batch_write_id, "data2");
    ASSERT_OK(batch_write->append_data(data_ctx2));
    ASSERT_EQ(1, num_rpc_request);
    auto read_data2 = static_cast<TimeBoundedStreamLoadPipe*>(pipe_ctx1->body_sink.get())->read();
    verify_data("data2", read_data2.value());

    SyncPoint::GetInstance()->SetCallBack("TimeBoundedStreamLoadPipe::get_current_ns",
                                          [&](void* arg) { *((int64_t*)arg) = 2000000000; });
    ASSERT_TRUE(static_cast<TimeBoundedStreamLoadPipe*>(pipe_ctx1->body_sink.get())->read().status().is_end_of_file());

    StreamLoadContext* pipe_ctx2 =
            build_pipe_context("label2", 2, batch_write_id, std::make_shared<TimeBoundedStreamLoadPipe>("p2", 1000));
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::status",
                                          [&](void* arg) { *((Status*)arg) = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::response", [&](void* arg) {
        TMergeCommitResult* result = (TMergeCommitResult*)arg;
        TStatus status;
        status.__set_status_code(TStatusCode::OK);
        result->__set_status(status);
        result->__set_label("label2");
        ASSERT_OK(batch_write->register_stream_load_pipe(pipe_ctx2));
    });

    StreamLoadContext* data_ctx3 = build_data_context(batch_write_id, "data3");
    ASSERT_OK(batch_write->append_data(data_ctx3));
    ASSERT_EQ(2, num_rpc_request);
    auto read_data3 = static_cast<TimeBoundedStreamLoadPipe*>(pipe_ctx2->body_sink.get())->read();
    ASSERT_OK(read_data3.status());
    verify_data("data3", read_data3.value());
}

TEST_F(IsomorphicBatchWriteTest, append_data_sync) {
    test_append_data_sync_base(Status::InternalError("Artificial failure"), TTransactionStatus::UNKNOWN,
                               Status::InternalError("Failed to get load status, Internal error: Artificial failure"));
    test_append_data_sync_base(Status::OK(), TTransactionStatus::PREPARE,
                               Status::TimedOut("load timeout, txn status: PREPARE"));
    test_append_data_sync_base(Status::OK(), TTransactionStatus::PREPARED,
                               Status::TimedOut("load timeout, txn status: PREPARED"));
    test_append_data_sync_base(Status::OK(), TTransactionStatus::COMMITTED,
                               Status::PublishTimeout("Load has not been published before timeout"));
    test_append_data_sync_base(Status::OK(), TTransactionStatus::VISIBLE, Status::OK());
    test_append_data_sync_base(Status::OK(), TTransactionStatus::ABORTED,
                               Status::InternalError("Load is aborted because of failure"));
    test_append_data_sync_base(Status::OK(), TTransactionStatus::UNKNOWN,
                               Status::InternalError("Load status is unknown: UNKNOWN"));
}

void IsomorphicBatchWriteTest::test_append_data_sync_base(const Status& rpc_status, TTransactionStatus::type txn_status,
                                                          const Status& expect_st) {
    BatchWriteId batch_write_id{
            .db = "db", .table = "table", .load_params = {{HTTP_MERGE_COMMIT_ASYNC, "false"}, {HTTP_TIMEOUT, "1"}}};
    IsomorphicBatchWriteSharedPtr batch_write = std::make_shared<IsomorphicBatchWrite>(batch_write_id, _executor.get());
    ASSERT_OK(batch_write->init());
    DeferOp defer_writer([&] { batch_write->stop(); });

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TimeBoundedStreamLoadPipe::get_current_ns");
        SyncPoint::GetInstance()->ClearCallBack("IsomorphicBatchWrite::send_rpc_request::request");
        SyncPoint::GetInstance()->ClearCallBack("IsomorphicBatchWrite::send_rpc_request::status");
        SyncPoint::GetInstance()->ClearCallBack("IsomorphicBatchWrite::send_rpc_request::response");
        SyncPoint::GetInstance()->ClearCallBack("IsomorphicBatchWrite::_wait_for_load_status::request");
        SyncPoint::GetInstance()->ClearCallBack("IsomorphicBatchWrite::_wait_for_load_status::status");
        SyncPoint::GetInstance()->ClearCallBack("IsomorphicBatchWrite::_wait_for_load_status::response");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    int num_rpc_request = 0;
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::request",
                                          [&](void* arg) { num_rpc_request += 1; });
    SyncPoint::GetInstance()->SetCallBack("TimeBoundedStreamLoadPipe::get_current_ns",
                                          [&](void* arg) { *((int64_t*)arg) = 0; });
    StreamLoadContext* pipe_ctx1 =
            build_pipe_context("label1", 1, batch_write_id, std::make_shared<TimeBoundedStreamLoadPipe>("p1", 1000));
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::status",
                                          [&](void* arg) { *((Status*)arg) = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::response", [&](void* arg) {
        TMergeCommitResult* result = (TMergeCommitResult*)arg;
        TStatus status;
        status.__set_status_code(TStatusCode::OK);
        result->__set_status(status);
        result->__set_label("label1");
        ASSERT_OK(batch_write->register_stream_load_pipe(pipe_ctx1));
    });

    // stream pipe left time is 100ms
    SyncPoint::GetInstance()->SetCallBack("TimeBoundedStreamLoadPipe::get_current_ns",
                                          [&](void* arg) { *((int64_t*)arg) = 900000000; });
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::_wait_for_load_status::request", [&](void* arg) {
        TGetLoadTxnStatusRequest* request = (TGetLoadTxnStatusRequest*)arg;
        EXPECT_EQ(batch_write_id.db, request->db);
        EXPECT_EQ(batch_write_id.table, request->tbl);
        EXPECT_EQ(1, request->txnId);
    });
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::_wait_for_load_status::status",
                                          [&](void* arg) { *((Status*)arg) = rpc_status; });
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::_wait_for_load_status::response", [&](void* arg) {
        TGetLoadTxnStatusResult* result = (TGetLoadTxnStatusResult*)arg;
        result->__set_status(txn_status);
    });
    StreamLoadContext* data_ctx1 = build_data_context(batch_write_id, "data1");
    Status result = batch_write->append_data(data_ctx1);
    ASSERT_EQ(1, num_rpc_request);
    auto read_data1 = static_cast<TimeBoundedStreamLoadPipe*>(pipe_ctx1->body_sink.get())->read();
    ASSERT_OK(read_data1.status());
    verify_data("data1", read_data1.value());
    ASSERT_EQ(expect_st.to_string(), result.to_string());
}

TEST_F(IsomorphicBatchWriteTest, stop_write) {
    BatchWriteId batch_write_id{.db = "db", .table = "table", .load_params = {}};
    IsomorphicBatchWriteSharedPtr batch_write = std::make_shared<IsomorphicBatchWrite>(batch_write_id, _executor.get());
    ASSERT_OK(batch_write->init());
    DeferOp defer_writer([&] { batch_write->stop(); });

    StreamLoadContext* pipe_ctx1 =
            build_pipe_context("label1", 1, batch_write_id, std::make_shared<TimeBoundedStreamLoadPipe>("p1", 1000));
    StreamLoadContext* pipe_ctx2 =
            build_pipe_context("label2", 2, batch_write_id, std::make_shared<TimeBoundedStreamLoadPipe>("p2", 1000));
    ASSERT_OK(batch_write->register_stream_load_pipe(pipe_ctx1));
    ASSERT_EQ(2, pipe_ctx1->num_refs());
    ASSERT_TRUE(batch_write->contain_pipe(pipe_ctx1));
    ASSERT_OK(batch_write->register_stream_load_pipe(pipe_ctx2));
    ASSERT_EQ(2, pipe_ctx2->num_refs());
    ASSERT_TRUE(batch_write->contain_pipe(pipe_ctx2));

    batch_write->stop();
    ASSERT_EQ(1, pipe_ctx1->num_refs());
    ASSERT_FALSE(batch_write->contain_pipe(pipe_ctx1));
    ASSERT_EQ(1, pipe_ctx2->num_refs());
    ASSERT_FALSE(batch_write->contain_pipe(pipe_ctx2));

    StreamLoadContext* pipe_ctx3 =
            build_pipe_context("label3", 3, batch_write_id, std::make_shared<TimeBoundedStreamLoadPipe>("p3", 1000));
    ASSERT_TRUE(batch_write->register_stream_load_pipe(pipe_ctx3).is_service_unavailable());
    StreamLoadContext* data_ctx = build_data_context(batch_write_id, "data");
    ASSERT_TRUE(batch_write->append_data(data_ctx).is_service_unavailable());
}

} // namespace starrocks
