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

#include "http/action/transaction_stream_load.h"

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
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/transaction_mgr.h"
#include "testutil/assert.h"
#include "testutil/sync_point.h"
#include "util/brpc_stub_cache.h"
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

class TransactionStreamLoadActionTest : public testing::Test {
public:
    TransactionStreamLoadActionTest() = default;
    ~TransactionStreamLoadActionTest() override = default;
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
        _env._stream_context_mgr = new StreamContextMgr();
        _env._transaction_mgr = new TransactionMgr(&_env);

        _evhttp_req = evhttp_request_new(nullptr, nullptr);
    }
    void TearDown() override {
        delete _env._transaction_mgr;
        _env._transaction_mgr = nullptr;
        delete _env._stream_context_mgr;
        _env._stream_context_mgr = nullptr;
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

protected:
    ExecEnv _env;
    evhttp_request* _evhttp_req = nullptr;
};

TEST_F(TransactionStreamLoadActionTest, txn_begin_no_auth) {
    TransactionManagerAction txn_action(&_env);

    HttpRequest b(_evhttp_req);
    b._headers.emplace(HTTP_LABEL_KEY, "123");
    b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
    txn_action.handle(&b);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("INTERNAL_ERROR", doc["Status"].GetString());
}

TEST_F(TransactionStreamLoadActionTest, txn_begin_invalid) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("INVALID_ARGUMENT", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("INVALID_ARGUMENT", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, "xxx");
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("INVALID_ARGUMENT", doc["Status"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_begin_normal) {
    TransactionManagerAction txn_action(&_env);

    HttpRequest b(_evhttp_req);
    b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
    b._headers.emplace(HTTP_LABEL_KEY, "123");
    b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
    txn_action.handle(&b);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("OK", doc["Status"].GetString());
}

TEST_F(TransactionStreamLoadActionTest, txn_commit_fail) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_COMMIT);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("INTERNAL_ERROR", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_COMMIT);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("TXN_NOT_EXISTS", doc["Status"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_prepare_fail) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_COMMIT);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("INTERNAL_ERROR", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_PREPARE);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("TXN_NOT_EXISTS", doc["Status"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_rollback) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_ROLLBACK);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_ROLLBACK);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("TXN_NOT_EXISTS", doc["Status"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_commit_success) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        TransactionStreamLoadAction action(&_env);

        HttpRequest request(_evhttp_req);
        request.set_handler(&action);

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        action.on_header(&request);
        action.handle(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_COMMIT);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_prepared_success) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        TransactionStreamLoadAction action(&_env);

        HttpRequest request(_evhttp_req);
        request.set_handler(&action);

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        action.on_header(&request);
        action.handle(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_PREPARE);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_put_fail) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        TransactionStreamLoadAction action(&_env);

        HttpRequest request(_evhttp_req);
        request.set_handler(&action);

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        Status status = Status::InternalError("TestFail");
        status.to_thrift(&k_stream_load_put_result.status);
        action.on_header(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("INTERNAL_ERROR", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_COMMIT);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("TXN_NOT_EXISTS", doc["Status"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_commit_fe_fail) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        TransactionStreamLoadAction action(&_env);

        HttpRequest request(_evhttp_req);
        request.set_handler(&action);

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        action.on_header(&request);
        action.handle(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_COMMIT);
        Status status = Status::InternalError("TestFail");
        status.to_thrift(&k_stream_load_commit_result.status);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("INTERNAL_ERROR", doc["Status"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_prepare_fe_fail) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        TransactionStreamLoadAction action(&_env);

        HttpRequest request(_evhttp_req);
        request.set_handler(&action);

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        action.on_header(&request);
        action.handle(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_PREPARE);
        Status status = Status::InternalError("TestFail");
        status.to_thrift(&k_stream_load_commit_result.status);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("INTERNAL_ERROR", doc["Status"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_begin_fe_fail) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        Status status = Status::InternalError("TestFail");
        status.to_thrift(&k_stream_load_begin_result.status);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("INTERNAL_ERROR", doc["Status"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_plan_fail) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        SyncPoint::GetInstance()->EnableProcessing();
        SyncPoint::GetInstance()->SetCallBack("StreamLoadExecutor::execute_plan_fragment:1",
                                              [](void* arg) { *(Status*)arg = Status::InternalError("TestFail"); });
        TransactionStreamLoadAction action(&_env);

        HttpRequest request(_evhttp_req);
        request.set_handler(&action);

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        action.on_header(&request);
        action.handle(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());

        SyncPoint::GetInstance()->ClearCallBack("StreamLoadExecutor::execute_plan_fragment:1");
        SyncPoint::GetInstance()->DisableProcessing();
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_list) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_LIST);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
        ASSERT_STREQ("123", doc["Label"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_idle_timeout) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._headers.emplace(HTTP_IDLE_TRANSACTION_TIMEOUT, "1");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    sleep(4);

    {
        TransactionStreamLoadAction action(&_env);

        HttpRequest request(_evhttp_req);
        request.set_handler(&action);

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        action.on_header(&request);
        action.handle(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("TXN_NOT_EXISTS", doc["Status"].GetString());
    }
}

TEST_F(TransactionStreamLoadActionTest, txn_not_same_load) {
    TransactionManagerAction txn_action(&_env);

    {
        HttpRequest b(_evhttp_req);
        b._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        b._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
        b._headers.emplace(HTTP_DB_KEY, "db");
        b._headers.emplace(HTTP_LABEL_KEY, "123");
        b._params.emplace(HTTP_TXN_OP_KEY, TXN_BEGIN);
        txn_action.handle(&b);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    TransactionStreamLoadAction action(&_env);
    {
        HttpRequest request(_evhttp_req);
        request.set_handler(&action);

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_DB_KEY, "db");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        request._headers.emplace(HTTP_COLUMN_SEPARATOR, "|");
        action.on_header(&request);
        action.handle(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        HttpRequest request(_evhttp_req);
        request.set_handler(&action);

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_DB_KEY, "db");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        request._headers.emplace(HTTP_COLUMN_SEPARATOR, "|");
        action.on_header(&request);
        action.handle(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }

    {
        HttpRequest request(_evhttp_req);
        request.set_handler(&action);

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_DB_KEY, "db");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        request._headers.emplace(HTTP_COLUMN_SEPARATOR, ",");
        ASSERT_EQ(-1, action.on_header(&request));
    }
}

#define SET_MEMORY_LIMIT_EXCEEDED(stmt)                                                            \
    do {                                                                                           \
        DeferOp defer([]() {                                                                       \
            SyncPoint::GetInstance()->ClearCallBack("ByteBuffer::allocate_with_tracker");          \
            SyncPoint::GetInstance()->DisableProcessing();                                         \
        });                                                                                        \
        SyncPoint::GetInstance()->EnableProcessing();                                              \
        SyncPoint::GetInstance()->SetCallBack("ByteBuffer::allocate_with_tracker", [](void* arg) { \
            *((Status*)arg) = Status::MemoryLimitExceeded("TestFail");                             \
        });                                                                                        \
        { stmt; }                                                                                  \
    } while (0)

TEST_F(TransactionStreamLoadActionTest, huge_malloc) {
    TransactionStreamLoadAction action(&_env);
    auto ctx = new StreamLoadContext(&_env);
    ctx->db = "db";
    ctx->table = "tbl";
    ctx->label = "huge_malloc";
    ctx->ref();
    ctx->body_sink = std::make_shared<StreamLoadPipe>();
    bool remove_from_stream_context_mgr = false;
    auto evb = evbuffer_new();
    DeferOp defer([&]() {
        if (remove_from_stream_context_mgr) {
            _env.stream_context_mgr()->remove(ctx->label);
        }
        if (ctx->unref()) {
            delete ctx;
        }
        evbuffer_free(evb);
    });
    ASSERT_OK((_env.stream_context_mgr())->put(ctx->label, ctx));
    remove_from_stream_context_mgr = true;

    HttpRequest request(_evhttp_req);
    request.set_handler(&action);
    std::string content = "abc";

    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    ev_req.input_buffer = evb;
    request._ev_req = &ev_req;

    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
    request._headers.emplace(HTTP_DB_KEY, ctx->db);
    request._headers.emplace(HTTP_TABLE_KEY, ctx->table);
    request._headers.emplace(HTTP_LABEL_KEY, ctx->label);
    ASSERT_EQ(0, action.on_header(&request));

    evbuffer_add(evb, content.data(), content.size());
    SET_MEMORY_LIMIT_EXCEEDED({
        ctx->status = Status::OK();
        action.on_chunk_data(&request);
        ASSERT_TRUE(ctx->status.is_mem_limit_exceeded());
    });
    ctx->status = Status::OK();
    action.on_chunk_data(&request);
    ASSERT_TRUE(ctx->status.ok());

    evbuffer_add(evb, content.data(), content.size());
    SET_MEMORY_LIMIT_EXCEEDED({
        ctx->buffer = ByteBufferPtr(new ByteBuffer(1));
        ctx->status = Status::OK();
        action.on_chunk_data(&request);
        ASSERT_TRUE(ctx->status.is_mem_limit_exceeded());
        ctx->buffer = nullptr;
    });
    ctx->buffer = ByteBufferPtr(new ByteBuffer(1));
    ctx->status = Status::OK();
    action.on_chunk_data(&request);
    ASSERT_TRUE(ctx->status.ok());
    ctx->buffer = nullptr;

    evbuffer_add(evb, content.data(), content.size());
    auto old_format = ctx->format;
    SET_MEMORY_LIMIT_EXCEEDED({
        ctx->format = TFileFormatType::FORMAT_JSON;
        ctx->buffer = ByteBufferPtr(new ByteBuffer(1));
        ctx->status = Status::OK();
        action.on_chunk_data(&request);
        ASSERT_TRUE(ctx->status.is_mem_limit_exceeded());
        ctx->buffer = nullptr;
    });
    ctx->format = TFileFormatType::FORMAT_JSON;
    ctx->buffer = ByteBufferPtr(new ByteBuffer(1));
    ctx->status = Status::OK();
    action.on_chunk_data(&request);
    ASSERT_TRUE(ctx->status.ok());
    ctx->buffer = nullptr;
    ctx->format = old_format;
}

TEST_F(TransactionStreamLoadActionTest, release_resource_for_success_request) {
    TransactionStreamLoadAction action(&_env);
    auto ctx = new StreamLoadContext(&_env);
    ctx->ref();
    ctx->db = "db";
    ctx->table = "tbl";
    ctx->label = "release_resource_for_success_request";
    ctx->body_sink = std::make_shared<StreamLoadPipe>();
    bool remove_from_stream_context_mgr = false;
    DeferOp defer([&]() {
        if (remove_from_stream_context_mgr) {
            _env.stream_context_mgr()->remove(ctx->label);
        }
        if (ctx->unref()) {
            delete ctx;
        }
    });
    ASSERT_OK((_env.stream_context_mgr())->put(ctx->label, ctx));
    remove_from_stream_context_mgr = true;
    ASSERT_TRUE(ctx->lock.try_lock());
    ctx->lock.unlock();

    // normal request
    {
        k_response_str = "";
        HttpRequest request(_evhttp_req);
        request.set_handler(&action);
        std::string content = "abc";
        auto evb = evbuffer_new();
        evbuffer_add(evb, content.data(), content.size());
        DeferOp free_evb([&]() { evbuffer_free(evb); });
        struct evhttp_request ev_req {
            .remote_host = nullptr, .input_buffer = evb
        };
        request._ev_req = &ev_req;
        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, std::to_string(content.length()));
        request._headers.emplace(HTTP_DB_KEY, ctx->db);
        request._headers.emplace(HTTP_TABLE_KEY, ctx->table);
        request._headers.emplace(HTTP_LABEL_KEY, ctx->label);
        ASSERT_EQ(0, action.on_header(&request));
        ASSERT_EQ(3, ctx->num_refs());
        ASSERT_FALSE(ctx->lock.try_lock());
        ASSERT_TRUE(k_response_str.empty());
        action.on_chunk_data(&request);
        ASSERT_EQ(3, ctx->num_refs());
        ASSERT_FALSE(ctx->lock.try_lock());
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("TransactionStreamLoad::send_reply");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        SyncPoint::GetInstance()->SetCallBack("TransactionStreamLoad::send_reply", [&](void* arg) {
            ASSERT_EQ(2, ctx->num_refs());
            ASSERT_TRUE(ctx->lock.try_lock());
            ctx->lock.unlock();
        });
        action.handle(&request);
        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
    }
    ASSERT_EQ(2, ctx->num_refs());
    ASSERT_TRUE(ctx->lock.try_lock());
    ctx->lock.unlock();
}

TEST_F(TransactionStreamLoadActionTest, release_resource_for_on_header_failure) {
    TransactionStreamLoadAction action(&_env);
    auto ctx = new StreamLoadContext(&_env);
    ctx->ref();
    ctx->db = "db";
    ctx->table = "tbl";
    ctx->label = "release_resource_for_on_header_failure";
    ctx->body_sink = std::make_shared<StreamLoadPipe>();
    bool remove_from_stream_context_mgr = false;
    DeferOp defer([&]() {
        if (remove_from_stream_context_mgr) {
            _env.stream_context_mgr()->remove(ctx->label);
        }
        if (ctx->unref()) {
            delete ctx;
        }
    });
    ASSERT_OK((_env.stream_context_mgr())->put(ctx->label, ctx));
    remove_from_stream_context_mgr = true;
    ASSERT_TRUE(ctx->lock.try_lock());
    ctx->lock.unlock();

    // on_header fail because of invalid format
    {
        k_response_str = "";
        HttpRequest request(_evhttp_req);
        request.set_handler(&action);
        std::string content = "abc";
        auto evb = evbuffer_new();
        evbuffer_add(evb, content.data(), content.size());
        DeferOp free_evb([&]() { evbuffer_free(evb); });
        struct evhttp_request ev_req {
            .remote_host = nullptr, .input_buffer = evb
        };
        request._ev_req = &ev_req;
        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, std::to_string(content.length()));
        request._headers.emplace(HTTP_DB_KEY, ctx->db);
        request._headers.emplace(HTTP_TABLE_KEY, ctx->table);
        request._headers.emplace(HTTP_LABEL_KEY, ctx->label);
        request._headers.emplace(HTTP_FORMAT_KEY, "unknown");
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("TransactionStreamLoad::send_reply");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        SyncPoint::GetInstance()->SetCallBack("TransactionStreamLoad::send_reply", [&](void* arg) {
            ASSERT_EQ(3, ctx->num_refs());
            ASSERT_TRUE(ctx->lock.try_lock());
            ctx->lock.unlock();
        });
        ASSERT_EQ(-1, action.on_header(&request));
        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("INTERNAL_ERROR", doc["Status"].GetString());
        ASSERT_NE(nullptr, std::strstr(doc["Message"].GetString(), "unknown data format, format=unknown"));
    }
    ASSERT_EQ(2, ctx->num_refs());
    ASSERT_TRUE(ctx->lock.try_lock());
    ctx->lock.unlock();
}

TEST_F(TransactionStreamLoadActionTest, release_resource_for_not_handle) {
    TransactionStreamLoadAction action(&_env);
    auto ctx = new StreamLoadContext(&_env);
    ctx->ref();
    ctx->db = "db";
    ctx->table = "tbl";
    ctx->label = "release_resource_for_not_handle";
    ctx->body_sink = std::make_shared<StreamLoadPipe>();
    bool remove_from_stream_context_mgr = false;
    DeferOp defer([&]() {
        if (remove_from_stream_context_mgr) {
            _env.stream_context_mgr()->remove(ctx->label);
        }
        if (ctx->unref()) {
            delete ctx;
        }
    });
    ASSERT_OK((_env.stream_context_mgr())->put(ctx->label, ctx));
    remove_from_stream_context_mgr = true;
    ASSERT_TRUE(ctx->lock.try_lock());
    ctx->lock.unlock();

    // skip on_chunk_data and handle
    {
        k_response_str = "";
        HttpRequest request(_evhttp_req);
        request.set_handler(&action);
        std::string content = "abc";
        auto evb = evbuffer_new();
        evbuffer_add(evb, content.data(), content.size());
        DeferOp free_evb([&]() { evbuffer_free(evb); });
        struct evhttp_request ev_req {
            .remote_host = nullptr, .input_buffer = evb
        };
        request._ev_req = &ev_req;
        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, std::to_string(content.length()));
        request._headers.emplace(HTTP_DB_KEY, ctx->db);
        request._headers.emplace(HTTP_TABLE_KEY, ctx->table);
        request._headers.emplace(HTTP_LABEL_KEY, ctx->label);
        ASSERT_EQ(0, action.on_header(&request));
        ASSERT_EQ(3, ctx->num_refs());
        ASSERT_FALSE(ctx->lock.try_lock());
        ASSERT_TRUE(k_response_str.empty());
    }
    ASSERT_EQ(2, ctx->num_refs());
    ASSERT_TRUE(ctx->lock.try_lock());
    ctx->lock.unlock();
}

} // namespace starrocks
