// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "http/action/transaction_stream_load.h"

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
#include "util/brpc_stub_cache.h"
#include "util/cpu_info.h"

class mg_connection;

namespace starrocks {

extern std::string k_response_str;

extern TLoadTxnBeginResult k_stream_load_begin_result;
extern TLoadTxnCommitResult k_stream_load_commit_result;
extern TLoadTxnRollbackResult k_stream_load_rollback_result;
extern TStreamLoadPutResult k_stream_load_put_result;
extern Status k_stream_load_plan_status;

class TransactionStreamLoadActionTest : public testing::Test {
public:
    TransactionStreamLoadActionTest() {}
    virtual ~TransactionStreamLoadActionTest() {}
    void SetUp() override {
        k_stream_load_begin_result = TLoadTxnBeginResult();
        k_stream_load_commit_result = TLoadTxnCommitResult();
        k_stream_load_rollback_result = TLoadTxnRollbackResult();
        k_stream_load_put_result = TStreamLoadPutResult();
        k_stream_load_plan_status = Status::OK();
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

private:
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

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        Status status = Status::InternalError("TestFail");
        status.to_thrift(&k_stream_load_put_result.status);
        action.on_header(&request);
        action.handle(&request);

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
        TransactionStreamLoadAction action(&_env);

        HttpRequest request(_evhttp_req);

        struct evhttp_request ev_req;
        ev_req.remote_host = nullptr;
        request._ev_req = &ev_req;

        request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
        request._headers.emplace(HTTP_LABEL_KEY, "123");
        k_stream_load_plan_status = Status::InternalError("TestFail");
        action.on_header(&request);
        action.handle(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        ASSERT_STREQ("OK", doc["Status"].GetString());
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

} // namespace starrocks
