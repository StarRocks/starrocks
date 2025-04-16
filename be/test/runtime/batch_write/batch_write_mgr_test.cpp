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

#include "runtime/batch_write/batch_write_mgr.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "brpc/controller.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/internal_service.pb.h"
#include "http/http_common.h"
#include "http/http_headers.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/time_bounded_stream_load_pipe.h"
#include "testutil/assert.h"
#include "util/bthreads/executor.h"
#include "util/monotime.h"
#include "util/threadpool.h"

namespace starrocks {

class BatchWriteMgrTest : public testing::Test {
public:
    BatchWriteMgrTest() = default;
    ~BatchWriteMgrTest() override = default;
    void SetUp() override {
        config::merge_commit_trace_log_enable = true;
        _exec_env = ExecEnv::GetInstance();
        std::unique_ptr<ThreadPool> thread_pool;
        ASSERT_OK(ThreadPoolBuilder("BatchWriteMgrTest")
                          .set_min_threads(0)
                          .set_max_threads(4)
                          .set_max_queue_size(2048)
                          .set_idle_timeout(MonoDelta::FromMilliseconds(10000))
                          .build(&thread_pool));
        auto executor = std::make_unique<bthreads::ThreadPoolExecutor>(thread_pool.release(), kTakesOwnership);
        _batch_write_mgr = std::make_unique<BatchWriteMgr>(std::move(executor));
        ASSERT_OK(_batch_write_mgr->init());
    }

    void TearDown() override {
        if (_batch_write_mgr != nullptr) {
            _batch_write_mgr->stop();
        }
        for (auto* ctx : _to_release_contexts) {
            StreamLoadContext::release(ctx);
        }
    }

    StreamLoadContext* build_data_context(const BatchWriteId& batch_write_id, const std::string& data) {
        StreamLoadContext* ctx = new StreamLoadContext(_exec_env);
        ctx->ref();
        ctx->db = batch_write_id.db;
        ctx->table = batch_write_id.table;
        ctx->load_parameters = batch_write_id.load_params;
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
    std::unique_ptr<BatchWriteMgr> _batch_write_mgr;
    std::unordered_set<StreamLoadContext*> _to_release_contexts;
};

TEST_F(BatchWriteMgrTest, register_and_unregister_pipe) {
    std::vector<StreamLoadContext*> contexts;

    for (int i = 0; i < 5; ++i) {
        std::string db = (i == 2) ? "db2" : "db1";
        std::string table = (i == 3) ? "table2" : "table1";
        std::map<std::string, std::string> load_params =
                (i == 4) ? std::map<std::string, std::string>{{"param3", "value3"}}
                         : std::map<std::string, std::string>{{"param1", "value1"}, {"param2", "value2"}};
        std::string label = "label" + std::to_string(i);
        long txn_id = i;
        TUniqueId load_id = generate_uuid();

        auto status_or_ctx = BatchWriteMgr::create_and_register_pipe(_exec_env, _batch_write_mgr.get(), db, table,
                                                                     load_params, label, txn_id, load_id, 1000);
        ASSERT_OK(status_or_ctx.status());
        StreamLoadContext* ctx = status_or_ctx.value();
        ASSERT_NE(ctx, nullptr);
        contexts.push_back(ctx);

        ASSERT_EQ(1, ctx->num_refs());
        ASSERT_EQ(ctx->db, db);
        ASSERT_EQ(ctx->table, table);
        ASSERT_EQ(ctx->label, label);
        ASSERT_EQ(ctx->txn_id, txn_id);
        ASSERT_TRUE(ctx->enable_batch_write);
        ASSERT_EQ(ctx->load_parameters, load_params);
        ASSERT_NE(dynamic_cast<TimeBoundedStreamLoadPipe*>(ctx->body_sink.get()), nullptr);
    }

    for (auto* ctx : contexts) {
        BatchWriteId batch_write_id{.db = ctx->db, .table = ctx->table, .load_params = ctx->load_parameters};
        auto status_or_batch_write = _batch_write_mgr->get_batch_write(batch_write_id);
        ASSERT_OK(status_or_batch_write.status());
        auto batch_write = status_or_batch_write.value();
        ASSERT_NE(batch_write, nullptr);
        ASSERT_TRUE(batch_write->contain_pipe(ctx));
    }

    for (auto* ctx : contexts) {
        BatchWriteId batch_write_id{.db = ctx->db, .table = ctx->table, .load_params = ctx->load_parameters};
        _batch_write_mgr->unregister_stream_load_pipe(ctx);
        auto status_or_batch_write = _batch_write_mgr->get_batch_write(batch_write_id);
        ASSERT_OK(status_or_batch_write.status());
        auto batch_write = status_or_batch_write.value();
        ASSERT_FALSE(batch_write->contain_pipe(ctx));
    }
}

TEST_F(BatchWriteMgrTest, append_data) {
    BatchWriteId batch_write_id = {
            "db1", "table1", {{HTTP_ENABLE_MERGE_COMMIT, "true"}, {HTTP_MERGE_COMMIT_ASYNC, "true"}}};
    auto status_or_ctx = BatchWriteMgr::create_and_register_pipe(_exec_env, _batch_write_mgr.get(), batch_write_id.db,
                                                                 batch_write_id.table, batch_write_id.load_params,
                                                                 "label1", 1, generate_uuid(), 1000);
    ASSERT_OK(status_or_ctx.status());
    StreamLoadContext* ctx = status_or_ctx.value();
    ASSERT_NE(ctx, nullptr);

    StreamLoadContext* data_ctx = build_data_context(batch_write_id, "data1");
    ASSERT_OK(_batch_write_mgr->append_data(data_ctx));
    auto read_data = static_cast<TimeBoundedStreamLoadPipe*>(ctx->body_sink.get())->read();
    ASSERT_OK(read_data.status());
    ASSERT_EQ("data1", std::string(read_data.value()->ptr, read_data.value()->limit));
}

TEST_F(BatchWriteMgrTest, create_batch_write_fail) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        TEST_DISABLE_ERROR_POINT("IsomorphicBatchWrite::init::error");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    TEST_ENABLE_ERROR_POINT("IsomorphicBatchWrite::init::error", Status::IOError("Artificial error"));
    BatchWriteId batch_write_id = {"db1", "table1", {{"param1", "value1"}, {"param2", "value2"}}};
    auto st = BatchWriteMgr::create_and_register_pipe(_exec_env, _batch_write_mgr.get(), batch_write_id.db,
                                                      batch_write_id.table, batch_write_id.load_params, "label1", 1,
                                                      generate_uuid(), 1000);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.status().message().find("Artificial error") != std::string::npos);
    ASSERT_TRUE(_batch_write_mgr->get_batch_write(batch_write_id).status().is_not_found());

    StreamLoadContext* data_ctx = build_data_context(batch_write_id, "data1");
    auto st1 = _batch_write_mgr->append_data(data_ctx);
    ASSERT_FALSE(st1.ok());
    ASSERT_TRUE(st1.message().find("Artificial error") != std::string::npos);
}

TEST_F(BatchWriteMgrTest, stop) {
    BatchWriteId batch_write_id1 = {"db1", "table1", {{"param1", "value1"}, {"param2", "value2"}}};
    auto status_or_ctx1 = BatchWriteMgr::create_and_register_pipe(_exec_env, _batch_write_mgr.get(), batch_write_id1.db,
                                                                  batch_write_id1.table, batch_write_id1.load_params,
                                                                  "label1", 1, generate_uuid(), 1000);
    ASSERT_OK(status_or_ctx1.status());
    StreamLoadContext* ctx1 = status_or_ctx1.value();
    ASSERT_NE(ctx1, nullptr);
    auto batch_write1 = _batch_write_mgr->get_batch_write(batch_write_id1);
    ASSERT_TRUE(batch_write1.ok());
    ASSERT_NE(batch_write1.value(), nullptr);

    BatchWriteId batch_write_id2 = {"db2", "table2", {{"param3", "value3"}, {"param4", "value4"}}};
    auto status_or_ctx2 = BatchWriteMgr::create_and_register_pipe(_exec_env, _batch_write_mgr.get(), batch_write_id2.db,
                                                                  batch_write_id2.table, batch_write_id2.load_params,
                                                                  "label2", 2, generate_uuid(), 1000);
    ASSERT_OK(status_or_ctx2.status());
    StreamLoadContext* ctx2 = status_or_ctx2.value();
    ASSERT_NE(ctx2, nullptr);
    auto batch_write2 = _batch_write_mgr->get_batch_write(batch_write_id2);
    ASSERT_TRUE(batch_write2.ok());
    ASSERT_NE(batch_write2.value(), nullptr);

    _batch_write_mgr->stop();
    ASSERT_TRUE(batch_write1.value()->is_stopped());
    ASSERT_TRUE(batch_write2.value()->is_stopped());

    BatchWriteId batch_write_id3 = {"db3", "table3", {{"param5", "value5"}, {"param6", "value6"}}};
    auto status_or_ctx3 = BatchWriteMgr::create_and_register_pipe(_exec_env, _batch_write_mgr.get(), batch_write_id3.db,
                                                                  batch_write_id3.table, batch_write_id3.load_params,
                                                                  "label3", 3, generate_uuid(), 1000);
    ASSERT_TRUE(status_or_ctx3.status().is_service_unavailable());
    StreamLoadContext* data_ctx = build_data_context(batch_write_id1, "data1");
    ASSERT_TRUE(_batch_write_mgr->append_data(data_ctx).is_service_unavailable());
}

#define ADD_KEY_VALUE(request, key, value)            \
    {                                                 \
        PStringPair* pair = request.add_parameters(); \
        pair->set_key(key);                           \
        pair->set_val(value);                         \
    }

TEST_F(BatchWriteMgrTest, stream_load_rpc_success) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("BatchWriteMgr::append_data::cb");
        SyncPoint::GetInstance()->ClearCallBack("BatchWriteMgr::append_data::success");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    PStreamLoadRequest request;
    PStreamLoadResponse response;
    request.set_db("db");
    request.set_table("tbl");
    request.set_user("root");
    request.set_passwd("123456");
    ADD_KEY_VALUE(request, "label", "test1");
    ADD_KEY_VALUE(request, HTTP_ENABLE_MERGE_COMMIT, "true");
    ADD_KEY_VALUE(request, HTTP_MERGE_COMMIT_INTERVAL_MS, "1000");
    ADD_KEY_VALUE(request, HTTP_MERGE_COMMIT_ASYNC, "true");
    ADD_KEY_VALUE(request, HTTP_TIMEOUT, "60");
    ADD_KEY_VALUE(request, HTTP_FORMAT_KEY, "json");
    std::string data = "{\"c0\":\"a\",\"c1\":\"b\"}";
    cntl.request_attachment().append(data);

    SyncPoint::GetInstance()->SetCallBack("BatchWriteMgr::append_data::cb", [&](void* arg) {
        StreamLoadContext* ctx = *(StreamLoadContext**)arg;
        EXPECT_EQ("db", ctx->db);
        EXPECT_EQ("tbl", ctx->table);
        EXPECT_EQ("test1", ctx->label);
        EXPECT_EQ("root", ctx->auth.user);
        EXPECT_EQ("123456", ctx->auth.passwd);
        EXPECT_TRUE(ctx->enable_batch_write);
        EXPECT_EQ(60, ctx->timeout_second);
        std::map<std::string, std::string> load_params = {{HTTP_ENABLE_MERGE_COMMIT, "true"},
                                                          {HTTP_MERGE_COMMIT_INTERVAL_MS, "1000"},
                                                          {HTTP_MERGE_COMMIT_ASYNC, "true"},
                                                          {HTTP_TIMEOUT, "60"},
                                                          {HTTP_FORMAT_KEY, "json"}};
        EXPECT_EQ(load_params, ctx->load_parameters);
        EXPECT_NE(nullptr, ctx->buffer);
        EXPECT_EQ(data, std::string(ctx->buffer->ptr, ctx->buffer->limit));
    });
    SyncPoint::GetInstance()->SetCallBack("BatchWriteMgr::append_data::success",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    _batch_write_mgr->receive_stream_load_rpc(_exec_env, &cntl, &request, &response);
    ASSERT_TRUE(response.has_json_result());
    rapidjson::Document doc;
    doc.Parse(response.json_result().c_str());
    ASSERT_STREQ("Success", doc["Status"].GetString());
}

TEST_F(BatchWriteMgrTest, stream_load_rpc_fail) {
    std::string data = "{\"c0\":\"a\",\"c1\":\"b\"}";
    // HTTP_ENABLE_MERGE_COMMIT is invalid
    {
        brpc::Controller cntl;
        cntl.request_attachment().append(data);
        PStreamLoadRequest request;
        request.set_db("db");
        request.set_table("tbl");
        request.set_user("root");
        request.set_passwd("123456");
        ADD_KEY_VALUE(request, "label", "test1");
        ADD_KEY_VALUE(request, HTTP_ENABLE_MERGE_COMMIT, "abc");
        PStreamLoadResponse response;
        _batch_write_mgr->receive_stream_load_rpc(_exec_env, &cntl, &request, &response);
        rapidjson::Document doc;
        doc.Parse(response.json_result().c_str());
        ASSERT_STREQ("Fail", doc["Status"].GetString());
        ASSERT_NE(nullptr, std::strstr(doc["Message"].GetString(), "Invalid parameter enable_merge_commit"));
    }

    // HTTP_ENABLE_MERGE_COMMIT is false
    {
        brpc::Controller cntl;
        cntl.request_attachment().append(data);
        PStreamLoadRequest request;
        request.set_db("db");
        request.set_table("tbl");
        request.set_user("root");
        request.set_passwd("123456");
        ADD_KEY_VALUE(request, "label", "test1");
        ADD_KEY_VALUE(request, HTTP_ENABLE_MERGE_COMMIT, "false");
        PStreamLoadResponse response;
        _batch_write_mgr->receive_stream_load_rpc(_exec_env, &cntl, &request, &response);
        rapidjson::Document doc;
        doc.Parse(response.json_result().c_str());
        ASSERT_STREQ("Fail", doc["Status"].GetString());
        ASSERT_NE(nullptr, std::strstr(doc["Message"].GetString(), "RPC interface only support batch write currently"));
    }

    // timeout format is invalid
    {
        brpc::Controller cntl;
        cntl.request_attachment().append(data);
        PStreamLoadRequest request;
        request.set_db("db");
        request.set_table("tbl");
        request.set_user("root");
        request.set_passwd("123456");
        ADD_KEY_VALUE(request, "label", "test1");
        ADD_KEY_VALUE(request, HTTP_ENABLE_MERGE_COMMIT, "true");
        ADD_KEY_VALUE(request, HTTP_TIMEOUT, "abc");
        PStreamLoadResponse response;
        _batch_write_mgr->receive_stream_load_rpc(_exec_env, &cntl, &request, &response);
        rapidjson::Document doc;
        doc.Parse(response.json_result().c_str());
        ASSERT_STREQ("Fail", doc["Status"].GetString());
        ASSERT_NE(nullptr, std::strstr(doc["Message"].GetString(), "Invalid timeout format: abc"));
    }

    // data is empty
    {
        brpc::Controller cntl;
        cntl.request_attachment().append("");
        PStreamLoadRequest request;
        request.set_db("db");
        request.set_table("tbl");
        request.set_user("root");
        request.set_passwd("123456");
        ADD_KEY_VALUE(request, "label", "test1");
        ADD_KEY_VALUE(request, HTTP_ENABLE_MERGE_COMMIT, "true");
        ADD_KEY_VALUE(request, HTTP_MERGE_COMMIT_INTERVAL_MS, "1000");
        ADD_KEY_VALUE(request, HTTP_MERGE_COMMIT_ASYNC, "true");
        ADD_KEY_VALUE(request, HTTP_FORMAT_KEY, "json");
        PStreamLoadResponse response;
        _batch_write_mgr->receive_stream_load_rpc(_exec_env, &cntl, &request, &response);
        rapidjson::Document doc;
        doc.Parse(response.json_result().c_str());
        ASSERT_STREQ("Fail", doc["Status"].GetString());
        ASSERT_NE(nullptr, std::strstr(doc["Message"].GetString(), "The data can not be empty"));
    }

    // append data fail
    {
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("BatchWriteMgr::append_data::fail");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        brpc::Controller cntl;
        cntl.request_attachment().append(data);
        PStreamLoadRequest request;
        request.set_db("db");
        request.set_table("tbl");
        request.set_user("root");
        request.set_passwd("123456");
        ADD_KEY_VALUE(request, "label", "test1");
        ADD_KEY_VALUE(request, HTTP_ENABLE_MERGE_COMMIT, "true");
        ADD_KEY_VALUE(request, HTTP_MERGE_COMMIT_INTERVAL_MS, "1000");
        ADD_KEY_VALUE(request, HTTP_MERGE_COMMIT_ASYNC, "true");
        ADD_KEY_VALUE(request, HTTP_FORMAT_KEY, "json");
        PStreamLoadResponse response;
        SyncPoint::GetInstance()->SetCallBack("BatchWriteMgr::append_data::fail", [](void* arg) {
            *(Status*)arg = Status::InternalError("Artificial failure");
        });
        _batch_write_mgr->receive_stream_load_rpc(_exec_env, &cntl, &request, &response);
        rapidjson::Document doc;
        doc.Parse(response.json_result().c_str());
        ASSERT_STREQ("Fail", doc["Status"].GetString());
        ASSERT_NE(nullptr, std::strstr(doc["Message"].GetString(), "Artificial failure"));
    }
}

TEST_F(BatchWriteMgrTest, update_transaction_state) {
    PUpdateTransactionStateRequest request;
    std::vector<TxnState> expected_cache_state;

    auto prepare_state = request.add_states();
    prepare_state->set_txn_id(1);
    prepare_state->set_status(TransactionStatusPB::TRANS_PREPARE);
    prepare_state->set_reason("");
    expected_cache_state.push_back({TTransactionStatus::PREPARE, ""});

    auto prepared_state = request.add_states();
    prepared_state->set_txn_id(2);
    prepared_state->set_status(TransactionStatusPB::TRANS_PREPARED);
    prepared_state->set_reason("");
    expected_cache_state.push_back({TTransactionStatus::PREPARED, ""});

    auto commited_state = request.add_states();
    commited_state->set_txn_id(3);
    commited_state->set_status(TransactionStatusPB::TRANS_COMMITTED);
    commited_state->set_reason("");
    expected_cache_state.push_back({TTransactionStatus::COMMITTED, ""});

    auto visible_state = request.add_states();
    visible_state->set_txn_id(4);
    visible_state->set_status(TransactionStatusPB::TRANS_VISIBLE);
    visible_state->set_reason("");
    expected_cache_state.push_back({TTransactionStatus::VISIBLE, ""});

    auto aborted_state = request.add_states();
    aborted_state->set_txn_id(5);
    aborted_state->set_status(TransactionStatusPB::TRANS_ABORTED);
    aborted_state->set_reason("artificial failure");
    expected_cache_state.push_back({TTransactionStatus::ABORTED, "artificial failure"});

    auto unknown_state = request.add_states();
    unknown_state->set_txn_id(6);
    unknown_state->set_status(TransactionStatusPB::TRANS_UNKNOWN);
    unknown_state->set_reason("");
    expected_cache_state.push_back({TTransactionStatus::UNKNOWN, ""});

    PUpdateTransactionStateResponse response;
    _batch_write_mgr->update_transaction_state(&request, &response);
    ASSERT_EQ(request.states_size(), response.results_size());
    for (int i = 1; i <= expected_cache_state.size(); ++i) {
        ASSERT_EQ(TStatusCode::OK, response.results(i - 1).status_code());
        auto actual_state = _batch_write_mgr->txn_state_cache()->get_state(i);
        ASSERT_OK(actual_state.status());
        ASSERT_EQ(expected_cache_state[i - 1].txn_status, actual_state.value().txn_status);
        ASSERT_EQ(expected_cache_state[i - 1].reason, actual_state.value().reason);
    }
}

} // namespace starrocks