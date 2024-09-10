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

} // namespace starrocks
