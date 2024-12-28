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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/http/stream_load_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/action/stream_load.h"

#include <event2/buffer.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <cstring>

#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "testutil/sync_point.h"
#include "util/brpc_stub_cache.h"
#include "util/concurrent_limiter.h"
#include "util/cpu_info.h"

class mg_connection;

namespace starrocks {

extern void (*s_injected_send_reply)(HttpRequest*, HttpStatus, std::string_view);

namespace {
static std::string k_response_str;
static void inject_send_reply(HttpRequest* request, HttpStatus status, std::string_view content) {
    k_response_str = content;
}
} // namespace

extern TLoadTxnBeginResult k_stream_load_begin_result;
extern TLoadTxnCommitResult k_stream_load_commit_result;
extern TLoadTxnRollbackResult k_stream_load_rollback_result;
extern TStreamLoadPutResult k_stream_load_put_result;

class StreamLoadActionTest : public testing::Test {
public:
    StreamLoadActionTest() = default;
    ~StreamLoadActionTest() override = default;
    static void SetUpTestSuite() { s_injected_send_reply = inject_send_reply; }
    static void TearDownTestSuite() { s_injected_send_reply = nullptr; }

    void SetUp() override {
        k_stream_load_begin_result = TLoadTxnBeginResult();
        k_stream_load_commit_result = TLoadTxnCommitResult();
        k_stream_load_rollback_result = TLoadTxnRollbackResult();
        k_stream_load_put_result = TStreamLoadPutResult();
        k_response_str = "";
        config::streaming_load_max_mb = 1;

        _env._load_stream_mgr = new LoadStreamMgr();
        _env._brpc_stub_cache = new BrpcStubCache();
        _env._stream_load_executor = new StreamLoadExecutor(&_env);

        _evhttp_req = evhttp_request_new(nullptr, nullptr);
        _limiter.reset(new ConcurrentLimiter(1000));
    }
    void TearDown() override {
        delete _env._brpc_stub_cache;
        _env._brpc_stub_cache = nullptr;
        delete _env._load_stream_mgr;
        _env._load_stream_mgr = nullptr;
        delete _env._stream_load_executor;
        _env._stream_load_executor = nullptr;

        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }
    }

private:
    ExecEnv _env;
    evhttp_request* _evhttp_req = nullptr;
    std::unique_ptr<ConcurrentLimiter> _limiter;
};

TEST_F(StreamLoadActionTest, no_auth) {
    StreamLoadAction action(&_env, _limiter.get());

    HttpRequest request(_evhttp_req);
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Fail", doc["Status"].GetString());
}

#if 0
TEST_F(StreamLoadActionTest, no_content_length) {
    StreamLoadAction action(&__env, _limiter.get());

    HttpRequest request(_evhttp_req);
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Fail", doc["Status"].GetString());
}

TEST_F(StreamLoadActionTest, unknown_encoding) {
    StreamLoadAction action(&_env, _limiter.get());

    HttpRequest request(_evhttp_req);
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::TRANSFER_ENCODING, "chunked111");
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Fail", doc["Status"].GetString());
}
#endif

TEST_F(StreamLoadActionTest, normal) {
    StreamLoadAction action(&_env, _limiter.get());

    HttpRequest request(_evhttp_req);

    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;

    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Success", doc["Status"].GetString());
}

TEST_F(StreamLoadActionTest, put_fail) {
    StreamLoadAction action(&_env, _limiter.get());

    HttpRequest request(_evhttp_req);

    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;

    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
    Status status = Status::InternalError("TestFail");
    status.to_thrift(&k_stream_load_put_result.status);
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Fail", doc["Status"].GetString());
}

TEST_F(StreamLoadActionTest, commit_fail) {
    StreamLoadAction action(&_env, _limiter.get());

    HttpRequest request(_evhttp_req);
    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
    Status status = Status::InternalError("TestFail");
    status.to_thrift(&k_stream_load_commit_result.status);
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Fail", doc["Status"].GetString());
}

TEST_F(StreamLoadActionTest, commit_try) {
    StreamLoadAction action(&_env, _limiter.get());

    HttpRequest request(_evhttp_req);
    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
    Status status = Status::ServiceUnavailable("service_unavailable");
    status.to_thrift(&k_stream_load_commit_result.status);
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Fail", doc["Status"].GetString());
}

TEST_F(StreamLoadActionTest, begin_fail) {
    StreamLoadAction action(&_env, _limiter.get());

    HttpRequest request(_evhttp_req);
    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
    Status status = Status::InternalError("TestFail");
    status.to_thrift(&k_stream_load_begin_result.status);
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Fail", doc["Status"].GetString());
}

#if 0
TEST_F(StreamLoadActionTest, receive_failed) {
    StreamLoadAction action(&_env, _limiter.get());

    HttpRequest request(_evhttp_req);
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::TRANSFER_ENCODING, "chunked");
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Fail", doc["Status"].GetString());
}
#endif

TEST_F(StreamLoadActionTest, plan_fail) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("StreamLoadExecutor::execute_plan_fragment:1",
                                          [](void* arg) { *((Status*)arg) = Status::InternalError("TestFail"); });

    StreamLoadAction action(&_env, _limiter.get());

    HttpRequest request(_evhttp_req);
    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");

    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Fail", doc["Status"].GetString());

    SyncPoint::GetInstance()->ClearCallBack("StreamLoadExecutor::execute_plan_fragment:1");
    SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(StreamLoadActionTest, huge_malloc) {
    StreamLoadAction action(&_env, _limiter.get());
    auto ctx = new StreamLoadContext(&_env);
    ctx->ref();
    ctx->body_sink = std::make_shared<StreamLoadPipe>();
    HttpRequest request(_evhttp_req);
    std::string content = "abc";

    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    auto evb = evbuffer_new();
    ev_req.input_buffer = evb;
    request._ev_req = &ev_req;

    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
    request._headers.emplace(HTTP_DB_KEY, "db");
    request._headers.emplace(HTTP_LABEL_KEY, "123");
    request._headers.emplace(HTTP_COLUMN_SEPARATOR, "|");
    request.set_handler(&action);
    request.set_handler_ctx(ctx);

    evbuffer_add(evb, content.data(), content.size());
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("ByteBuffer::allocate_with_tracker",
                                          [](void* arg) { *((Status*)arg) = Status::MemoryLimitExceeded("TestFail"); });
    ctx->status = Status::OK();
    action.on_chunk_data(&request);
    ASSERT_TRUE(ctx->status.is_mem_limit_exceeded());
    SyncPoint::GetInstance()->ClearCallBack("ByteBuffer::allocate_with_tracker");
    SyncPoint::GetInstance()->DisableProcessing();
    ctx->status = Status::OK();
    action.on_chunk_data(&request);
    ASSERT_TRUE(ctx->status.ok());

    evbuffer_add(evb, content.data(), content.size());
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("ByteBuffer::allocate_with_tracker",
                                          [](void* arg) { *((Status*)arg) = Status::MemoryLimitExceeded("TestFail"); });
    ctx->buffer = ByteBufferPtr(new ByteBuffer(1));
    ctx->status = Status::OK();
    action.on_chunk_data(&request);
    ASSERT_TRUE(ctx->status.is_mem_limit_exceeded());
    ctx->buffer = nullptr;
    SyncPoint::GetInstance()->ClearCallBack("ByteBuffer::allocate_with_tracker");
    SyncPoint::GetInstance()->DisableProcessing();
    ctx->buffer = ByteBufferPtr(new ByteBuffer(1));
    ctx->status = Status::OK();
    action.on_chunk_data(&request);
    ASSERT_TRUE(ctx->status.ok());
    ctx->buffer = nullptr;

    evbuffer_add(evb, content.data(), content.size());
    auto old_format = ctx->format;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("ByteBuffer::allocate_with_tracker",
                                          [](void* arg) { *((Status*)arg) = Status::MemoryLimitExceeded("TestFail"); });
    ctx->format = TFileFormatType::FORMAT_JSON;
    ctx->buffer = ByteBufferPtr(new ByteBuffer(1));
    ctx->status = Status::OK();
    action.on_chunk_data(&request);
    ASSERT_TRUE(ctx->status.is_mem_limit_exceeded());
    ctx->buffer = nullptr;
    SyncPoint::GetInstance()->ClearCallBack("ByteBuffer::allocate_with_tracker");
    SyncPoint::GetInstance()->DisableProcessing();
    ctx->format = TFileFormatType::FORMAT_JSON;
    ctx->buffer = ByteBufferPtr(new ByteBuffer(1));
    ctx->status = Status::OK();
    action.on_chunk_data(&request);
    ASSERT_TRUE(ctx->status.ok());
    ctx->buffer = nullptr;
    ctx->format = old_format;

    request.set_handler_ctx(nullptr);
    request.set_handler(nullptr);
    if (ctx->unref()) {
        delete ctx;
    }
    evbuffer_free(evb);
}

TEST_F(StreamLoadActionTest, batch_write_csv) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("BatchWriteMgr::append_data::cb");
        SyncPoint::GetInstance()->ClearCallBack("BatchWriteMgr::append_data::success");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    StreamLoadAction action(&_env, _limiter.get());
    HttpRequest request(_evhttp_req);

    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._params.emplace(HTTP_DB_KEY, "db");
    request._params.emplace(HTTP_TABLE_KEY, "tbl");
    request._headers.emplace(HTTP_LABEL_KEY, "batch_write_csv");
    request._headers.emplace(HTTP_ENABLE_MERGE_COMMIT, "true");
    request._headers.emplace(HTTP_MERGE_COMMIT_INTERVAL_MS, "1000");
    request._headers.emplace(HTTP_MERGE_COMMIT_ASYNC, "true");

    std::string content = "a|b|c|d";
    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    auto evb = evbuffer_new();
    DeferOp defer_evb([&] { evbuffer_free(evb); });
    ev_req.input_buffer = evb;
    request._ev_req = &ev_req;
    request._headers.emplace(HTTP_FORMAT_KEY, "csv");
    request._headers.emplace(HTTP_COLUMN_SEPARATOR, "|");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, std::to_string(content.length()));
    request.set_handler(&action);

    ASSERT_EQ(0, action.on_header(&request));
    StreamLoadContext* ctx = static_cast<StreamLoadContext*>(request._handler_ctx);
    ASSERT_NE(nullptr, ctx);
    ASSERT_TRUE(ctx->status.ok());
    ASSERT_TRUE(ctx->enable_batch_write);
    ASSERT_NE(nullptr, ctx->buffer);
    ASSERT_EQ(content.length(), ctx->buffer->limit);

    evbuffer_add(evb, content.data(), content.size());
    ctx->status = Status::OK();
    action.on_chunk_data(&request);
    ASSERT_TRUE(ctx->status.ok());
    ASSERT_EQ(content.length(), ctx->buffer->pos);

    SyncPoint::GetInstance()->SetCallBack("BatchWriteMgr::append_data::cb",
                                          [&](void* arg) { EXPECT_EQ(ctx, *(StreamLoadContext**)arg); });
    SyncPoint::GetInstance()->SetCallBack("BatchWriteMgr::append_data::success",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    action.handle(&request);
    ASSERT_TRUE(ctx->status.ok());
    std::map<std::string, std::string> load_params = {{HTTP_ENABLE_MERGE_COMMIT, "true"},
                                                      {HTTP_MERGE_COMMIT_INTERVAL_MS, "1000"},
                                                      {HTTP_MERGE_COMMIT_ASYNC, "true"},
                                                      {HTTP_FORMAT_KEY, "csv"},
                                                      {HTTP_COLUMN_SEPARATOR, "|"}};
    ASSERT_EQ(load_params, ctx->load_parameters);
    ASSERT_EQ(content, std::string(ctx->buffer->ptr, ctx->buffer->limit));

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Success", doc["Status"].GetString());
}

TEST_F(StreamLoadActionTest, batch_write_json) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("BatchWriteMgr::append_data::cb");
        SyncPoint::GetInstance()->ClearCallBack("BatchWriteMgr::append_data::success");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    StreamLoadAction action(&_env, _limiter.get());
    HttpRequest request(_evhttp_req);

    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._params.emplace(HTTP_DB_KEY, "db");
    request._params.emplace(HTTP_TABLE_KEY, "tbl");
    request._headers.emplace(HTTP_LABEL_KEY, "batch_write_csv");
    request._headers.emplace(HTTP_ENABLE_MERGE_COMMIT, "true");
    request._headers.emplace(HTTP_MERGE_COMMIT_INTERVAL_MS, "1000");
    request._headers.emplace(HTTP_MERGE_COMMIT_ASYNC, "true");

    std::string content = "{\"c0\":\"a\",\"c1\":\"b\"}";
    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    auto evb = evbuffer_new();
    DeferOp defer_evb([&] { evbuffer_free(evb); });
    ev_req.input_buffer = evb;
    request._ev_req = &ev_req;
    request._headers.emplace(HTTP_FORMAT_KEY, "json");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, std::to_string(content.length()));
    request.set_handler(&action);

    ASSERT_EQ(0, action.on_header(&request));
    StreamLoadContext* ctx = static_cast<StreamLoadContext*>(request._handler_ctx);
    ASSERT_NE(nullptr, ctx);
    ASSERT_TRUE(ctx->status.ok());
    ASSERT_TRUE(ctx->enable_batch_write);
    ASSERT_NE(nullptr, ctx->buffer);
    ASSERT_EQ(content.length(), ctx->buffer->limit);

    evbuffer_add(evb, content.data(), content.size());
    ctx->status = Status::OK();
    action.on_chunk_data(&request);
    ASSERT_TRUE(ctx->status.ok());
    ASSERT_EQ(content.length(), ctx->buffer->pos);

    SyncPoint::GetInstance()->SetCallBack("BatchWriteMgr::append_data::cb",
                                          [&](void* arg) { EXPECT_EQ(ctx, *(StreamLoadContext**)arg); });
    SyncPoint::GetInstance()->SetCallBack("BatchWriteMgr::append_data::success",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    action.handle(&request);
    ASSERT_TRUE(ctx->status.ok());
    std::map<std::string, std::string> load_params = {{HTTP_ENABLE_MERGE_COMMIT, "true"},
                                                      {HTTP_MERGE_COMMIT_INTERVAL_MS, "1000"},
                                                      {HTTP_MERGE_COMMIT_ASYNC, "true"},
                                                      {HTTP_FORMAT_KEY, "json"}};
    ASSERT_EQ(load_params, ctx->load_parameters);
    ASSERT_EQ(content, std::string(ctx->buffer->ptr, ctx->buffer->limit));

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Success", doc["Status"].GetString());
}

TEST_F(StreamLoadActionTest, enable_batch_write_wrong_argument) {
    StreamLoadAction action(&_env, _limiter.get());

    HttpRequest request(_evhttp_req);

    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;

    request._params.emplace(HTTP_DB_KEY, "db");
    request._params.emplace(HTTP_TABLE_KEY, "tbl");
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HTTP_ENABLE_MERGE_COMMIT, "abc");
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Fail", doc["Status"].GetString());
    ASSERT_NE(nullptr, std::strstr(doc["Message"].GetString(), "Invalid parameter enable_merge_commit"));
}

TEST_F(StreamLoadActionTest, merge_commit_response) {
    // success
    {
        StreamLoadContext ctx(&_env);
        ctx.enable_batch_write = true;
        ctx.status = Status::OK();
        ctx.txn_id = 1;
        ctx.batch_write_label = "label1";
        ctx.label = "request_id_1";
        ctx.receive_bytes = 10;
        ctx.load_cost_nanos = 1'200'000'000;
        ctx.mc_read_data_cost_nanos = 10'000'000;
        ctx.mc_pending_cost_nanos = 20'000'000;
        ctx.mc_wait_plan_cost_nanos = 100'000'000;
        ctx.mc_write_data_cost_nanos = 70'000'000;
        ctx.mc_wait_finish_cost_nanos = 1'000'000'000;
        ctx.mc_left_merge_time_nanos = 800'000'000;
        auto result = ctx.to_json();
        ASSERT_EQ(
                "{\n"
                "    \"TxnId\": 1,\n"
                "    \"Label\": \"label1\",\n"
                "    \"Status\": \"Success\",\n"
                "    \"Message\": \"OK\",\n"
                "    \"RequestId\": \"request_id_1\",\n"
                "    \"LoadBytes\": 10,\n"
                "    \"LoadTimeMs\": 1200,\n"
                "    \"ReadDataTimeMs\": 10,\n"
                "    \"PendingTimeMs\": 20,\n"
                "    \"WaitPlanTimeMs\": 100,\n"
                "    \"WriteDataTimeMs\": 70,\n"
                "    \"WaitFinishTimeMs\": 1000,\n"
                "    \"LeftMergeTimeMs\": 800\n"
                "}",
                result);
    }

    // fail
    {
        StreamLoadContext ctx(&_env);
        ctx.enable_batch_write = true;
        ctx.status = Status::InternalError("TestFail");
        ctx.txn_id = 2;
        ctx.batch_write_label = "label2";
        ctx.label = "request_id_2";
        ctx.receive_bytes = 20;
        ctx.load_cost_nanos = 100'000'000;
        ctx.mc_read_data_cost_nanos = 10'000'000;
        ctx.mc_pending_cost_nanos = 20'000'000;
        ctx.mc_wait_plan_cost_nanos = 70'000'000;
        ctx.mc_write_data_cost_nanos = 0;
        ctx.mc_wait_finish_cost_nanos = 0;
        ctx.mc_left_merge_time_nanos = 0;
        auto result = ctx.to_json();
        ASSERT_EQ(
                "{\n"
                "    \"TxnId\": 2,\n"
                "    \"Label\": \"label2\",\n"
                "    \"Status\": \"Fail\",\n"
                "    \"Message\": \"TestFail\",\n"
                "    \"RequestId\": \"request_id_2\",\n"
                "    \"LoadBytes\": 20,\n"
                "    \"LoadTimeMs\": 100,\n"
                "    \"ReadDataTimeMs\": 10,\n"
                "    \"PendingTimeMs\": 20,\n"
                "    \"WaitPlanTimeMs\": 70,\n"
                "    \"WriteDataTimeMs\": 0,\n"
                "    \"WaitFinishTimeMs\": 0,\n"
                "    \"LeftMergeTimeMs\": 0\n"
                "}",
                result);
    }
}

} // namespace starrocks
