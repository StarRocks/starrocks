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

#include "gen_cpp/FrontendService.h"
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
    BatchWriteId batch_write_id = {"db1", "table1", {{"param1", "value1"}, {"param2", "value2"}}};
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

} // namespace starrocks