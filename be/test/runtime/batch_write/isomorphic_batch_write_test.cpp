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
        _batch_write_id.db = "db";
        _batch_write_id.table = "table";
        std::unique_ptr<ThreadPool> thread_pool;
        ASSERT_OK(ThreadPoolBuilder("IsomorphicBatchWriteTest")
                          .set_min_threads(0)
                          .set_max_threads(1)
                          .set_max_queue_size(2048)
                          .set_idle_timeout(MonoDelta::FromMilliseconds(10000))
                          .build(&thread_pool));
        _executor = std::make_unique<bthreads::ThreadPoolExecutor>(thread_pool.release(), kTakesOwnership);
        _batch_write = std::make_shared<IsomorphicBatchWrite>(_batch_write_id, _executor.get());
        ASSERT_OK(_batch_write->init());
    }

    void TearDown() override {
        if (_batch_write != nullptr) {
            _batch_write->stop();
        }
        for (auto* ctx : _to_release_contexts) {
            StreamLoadContext::release(ctx);
        }
    }

    StreamLoadContext* build_pipe_context(const std::string& label, int64_t txn_id,
                                          std::shared_ptr<TimeBoundedStreamLoadPipe> pipe) {
        StreamLoadContext* ctx = new StreamLoadContext(_exec_env);
        ctx->ref();
        ctx->db = _batch_write_id.db;
        ctx->table = _batch_write_id.table;
        ctx->label = std::move(label);
        ctx->txn_id = txn_id;
        ctx->body_sink = pipe;
        ctx->enable_batch_write = true;
        _to_release_contexts.emplace(ctx);
        return ctx;
    }

    StreamLoadContext* build_data_context(const std::string& data) {
        StreamLoadContext* ctx = new StreamLoadContext(_exec_env);
        ctx->ref();
        ctx->db = _batch_write_id.db;
        ctx->table = _batch_write_id.table;
        ctx->enable_batch_write = true;
        auto buf = ByteBuffer::allocate_with_tracker(64).value();
        buf->put_bytes(data.c_str(), data.size());
        buf->flip();
        ctx->buffer = buf;
        _to_release_contexts.emplace(ctx);
        return ctx;
    }

protected:
    ExecEnv* _exec_env;
    BatchWriteId _batch_write_id;
    std::unique_ptr<bthreads::ThreadPoolExecutor> _executor;
    IsomorphicBatchWriteSharedPtr _batch_write;
    std::unordered_set<StreamLoadContext*> _to_release_contexts;
};

void verify_data(std::string expected, ByteBufferPtr actual) {
    ASSERT_EQ(expected.size(), actual->limit);
    for (int i = 0; i < actual->pos; ++i) {
        ASSERT_EQ(expected[i], *(actual->ptr + i));
    }
}

TEST_F(IsomorphicBatchWriteTest, register_and_unregister_pipe) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("StreamLoadContext::release");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    StreamLoadContext* pipe_ctx1 =
            build_pipe_context("label1", 1, std::make_shared<TimeBoundedStreamLoadPipe>("p1", 1000));
    StreamLoadContext* pipe_ctx2 =
            build_pipe_context("label2", 2, std::make_shared<TimeBoundedStreamLoadPipe>("p2", 1000));

    ASSERT_OK(_batch_write->register_stream_load_pipe(pipe_ctx1));
    ASSERT_EQ(2, pipe_ctx1->num_refs());
    ASSERT_TRUE(_batch_write->contain_pipe(pipe_ctx1));
    ASSERT_OK(_batch_write->register_stream_load_pipe(pipe_ctx2));
    ASSERT_EQ(2, pipe_ctx2->num_refs());
    ASSERT_TRUE(_batch_write->contain_pipe(pipe_ctx2));

    _batch_write->unregister_stream_load_pipe(pipe_ctx1);
    ASSERT_FALSE(_batch_write->contain_pipe(pipe_ctx1));
    ASSERT_EQ(1, pipe_ctx1->num_refs());
    _batch_write->unregister_stream_load_pipe(pipe_ctx2);
    ASSERT_FALSE(_batch_write->contain_pipe(pipe_ctx2));
    ASSERT_EQ(1, pipe_ctx2->num_refs());
}

TEST_F(IsomorphicBatchWriteTest, append_data) {
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
            build_pipe_context("label1", 1, std::make_shared<TimeBoundedStreamLoadPipe>("p1", 1000));
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::status",
                                          [&](void* arg) { *((Status*)arg) = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::response", [&](void* arg) {
        TBatchWriteResult* result = (TBatchWriteResult*)arg;
        TStatus status;
        status.__set_status_code(TStatusCode::OK);
        result->__set_status(status);
        result->__set_label("label1");
        ASSERT_OK(_batch_write->register_stream_load_pipe(pipe_ctx1));
    });

    StreamLoadContext* data_ctx1 = build_data_context("data1");
    ASSERT_OK(_batch_write->append_data(data_ctx1));
    ASSERT_EQ(1, num_rpc_request);
    auto read_data1 = static_cast<TimeBoundedStreamLoadPipe*>(pipe_ctx1->body_sink.get())->read();
    ASSERT_OK(read_data1.status());
    verify_data("data1", read_data1.value());

    StreamLoadContext* data_ctx2 = build_data_context("data2");
    ASSERT_OK(_batch_write->append_data(data_ctx2));
    ASSERT_EQ(1, num_rpc_request);
    auto read_data2 = static_cast<TimeBoundedStreamLoadPipe*>(pipe_ctx1->body_sink.get())->read();
    verify_data("data2", read_data2.value());

    SyncPoint::GetInstance()->SetCallBack("TimeBoundedStreamLoadPipe::get_current_ns",
                                          [&](void* arg) { *((int64_t*)arg) = 2000000000; });
    ASSERT_TRUE(static_cast<TimeBoundedStreamLoadPipe*>(pipe_ctx1->body_sink.get())->read().status().is_end_of_file());

    StreamLoadContext* pipe_ctx2 =
            build_pipe_context("label2", 2, std::make_shared<TimeBoundedStreamLoadPipe>("p2", 1000));
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::status",
                                          [&](void* arg) { *((Status*)arg) = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("IsomorphicBatchWrite::send_rpc_request::response", [&](void* arg) {
        TBatchWriteResult* result = (TBatchWriteResult*)arg;
        TStatus status;
        status.__set_status_code(TStatusCode::OK);
        result->__set_status(status);
        result->__set_label("label2");
        ASSERT_OK(_batch_write->register_stream_load_pipe(pipe_ctx2));
    });

    StreamLoadContext* data_ctx3 = build_data_context("data3");
    ASSERT_OK(_batch_write->append_data(data_ctx3));
    ASSERT_EQ(2, num_rpc_request);
    auto read_data3 = static_cast<TimeBoundedStreamLoadPipe*>(pipe_ctx2->body_sink.get())->read();
    ASSERT_OK(read_data3.status());
    verify_data("data3", read_data3.value());
}

TEST_F(IsomorphicBatchWriteTest, stop_write) {
    StreamLoadContext* pipe_ctx1 =
            build_pipe_context("label1", 1, std::make_shared<TimeBoundedStreamLoadPipe>("p1", 1000));
    StreamLoadContext* pipe_ctx2 =
            build_pipe_context("label2", 2, std::make_shared<TimeBoundedStreamLoadPipe>("p2", 1000));
    ASSERT_OK(_batch_write->register_stream_load_pipe(pipe_ctx1));
    ASSERT_EQ(2, pipe_ctx1->num_refs());
    ASSERT_TRUE(_batch_write->contain_pipe(pipe_ctx1));
    ASSERT_OK(_batch_write->register_stream_load_pipe(pipe_ctx2));
    ASSERT_EQ(2, pipe_ctx2->num_refs());
    ASSERT_TRUE(_batch_write->contain_pipe(pipe_ctx2));

    _batch_write->stop();
    ASSERT_EQ(1, pipe_ctx1->num_refs());
    ASSERT_FALSE(_batch_write->contain_pipe(pipe_ctx1));
    ASSERT_EQ(1, pipe_ctx2->num_refs());
    ASSERT_FALSE(_batch_write->contain_pipe(pipe_ctx2));

    StreamLoadContext* pipe_ctx3 =
            build_pipe_context("label3", 3, std::make_shared<TimeBoundedStreamLoadPipe>("p3", 1000));
    ASSERT_TRUE(_batch_write->register_stream_load_pipe(pipe_ctx3).is_service_unavailable());
    StreamLoadContext* data_ctx = build_data_context("data");
    ASSERT_TRUE(_batch_write->append_data(data_ctx).is_service_unavailable());
}

} // namespace starrocks
