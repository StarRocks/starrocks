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

#include "runtime/load_channel_mgr.h"

#include <brpc/controller.h>
#include <gtest/gtest.h>

#include "service/brpc_service_test_util.h"
#include "storage/chunk_helper.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "util/await.h"

namespace starrocks {

class LoadChannelMgrTest : public testing::Test {
public:
    LoadChannelMgrTest() = default;
    ~LoadChannelMgrTest() override = default;

protected:
    void SetUp() override {
        _mem_tracker = std::make_unique<MemTracker>(-1);
        _load_channel_mgr = std::make_unique<LoadChannelMgr>();
        _node_id = 100;
        _db_id = 100;
        _table_id = 101;
        _partition_id = 10;
        _index_id = 1;
        _tablet_id = rand();
        _tablet = create_tablet(_tablet_id, rand());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet->tablet_schema()));
    }
    void TearDown() override {
        if (_tablet) {
            _tablet.reset();
            ASSERT_OK(StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet_id));
        }
        if (_load_channel_mgr) {
            _load_channel_mgr->close();
        }
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::DUP_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn c0;
        c0.column_name = "c0";
        c0.__set_is_key(true);
        c0.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(c0);

        TColumn c1;
        c1.column_name = "c1";
        c1.__set_is_key(false);
        c1.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(c1);

        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    PTabletWriterOpenRequest create_open_request(PUniqueId load_id, int64_t txn_id) {
        PTabletWriterOpenRequest request;
        request.mutable_id()->CopyFrom(load_id);
        request.set_index_id(_index_id);
        request.set_txn_id(txn_id);
        request.set_is_lake_tablet(false);
        request.set_is_replicated_storage(true);
        request.set_node_id(_node_id);
        request.set_write_quorum(WriteQuorumTypePB::MAJORITY);
        request.set_miss_auto_increment_column(false);
        request.set_table_id(_table_id);
        request.set_is_incremental(false);
        request.set_num_senders(1);
        request.set_sender_id(0);
        request.set_need_gen_rollup(false);
        request.set_load_channel_timeout_s(10);
        request.set_is_vectorized(true);
        request.set_timeout_ms(10000);

        request.set_immutable_tablet_size(0);
        auto tablet = request.add_tablets();
        tablet->set_partition_id(_partition_id);
        tablet->set_tablet_id(_tablet_id);
        auto replica = tablet->add_replicas();
        replica->set_host("127.0.0.1");
        replica->set_port(8060);
        replica->set_node_id(_node_id);

        auto schema = request.mutable_schema();
        schema->set_db_id(_db_id);
        schema->set_table_id(_table_id);
        schema->set_version(1);
        auto index = schema->add_indexes();
        index->set_id(_index_id);
        index->set_schema_hash(0);
        for (int i = 0, sz = _tablet->tablet_schema()->num_columns(); i < sz; i++) {
            auto slot = request.mutable_schema()->add_slot_descs();
            auto& column = _tablet->tablet_schema()->column(i);
            slot->set_id(i);
            slot->set_byte_offset(i * sizeof(int) /*unused*/);
            slot->set_col_name(std::string(column.name()));
            slot->set_slot_idx(i);
            slot->set_is_materialized(true);
            slot->mutable_slot_type()->add_types()->mutable_scalar_type()->set_type(column.type());
            index->add_columns(std::string(column.name()));
        }
        auto tuple_desc = schema->mutable_tuple_desc();
        tuple_desc->set_id(1);
        tuple_desc->set_byte_size(8 /*unused*/);
        tuple_desc->set_num_null_bytes(0 /*unused*/);
        tuple_desc->set_table_id(_table_id);

        return request;
    }

    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<LoadChannelMgr> _load_channel_mgr;
    TabletSharedPtr _tablet;

    int64_t _node_id;
    int64_t _db_id;
    int64_t _table_id;
    int64_t _partition_id;
    int32_t _index_id;
    int64_t _tablet_id;
    std::shared_ptr<Schema> _schema;
    std::shared_ptr<OlapTableSchemaParam> _schema_param;
};

TEST_F(LoadChannelMgrTest, async_open_success) {
    ASSERT_OK(_load_channel_mgr->init(_mem_tracker.get()));
    PUniqueId load_id;
    load_id.set_hi(456789);
    load_id.set_lo(987654);
    brpc::Controller cntl;
    MockClosure closure;
    PTabletWriterOpenRequest request = create_open_request(load_id, rand());
    PTabletWriterOpenResult result;
    _load_channel_mgr->open(&cntl, request, &result, &closure);
    ASSERT_TRUE(Awaitility().timeout(60000).until(
            [&] { return _load_channel_mgr->async_rpc_pool()->total_executed_tasks() == 1; }));
    ASSERT_TRUE(closure.has_run());
    ASSERT_TRUE(result.status().status_code() == TStatusCode::OK);
    auto load_channel = _load_channel_mgr->TEST_get_load_channel(UniqueId(load_id));
    ASSERT_TRUE(load_channel != nullptr);
}

TEST_F(LoadChannelMgrTest, async_open_submit_task_fail) {
    ASSERT_OK(_load_channel_mgr->init(_mem_tracker.get()));
    PUniqueId load_id;
    load_id.set_hi(456789);
    load_id.set_lo(987654);
    brpc::Controller cntl;
    MockClosure closure;
    PTabletWriterOpenRequest request = create_open_request(load_id, rand());
    PTabletWriterOpenResult result;

    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });

    _load_channel_mgr->open(&cntl, request, &result, &closure);
    ASSERT_TRUE(closure.has_run());
    ASSERT_TRUE(result.status().status_code() == TStatusCode::SERVICE_UNAVAILABLE);
    auto load_channel = _load_channel_mgr->TEST_get_load_channel(UniqueId(load_id));
    ASSERT_TRUE(load_channel == nullptr);
}

TEST_F(LoadChannelMgrTest, sync_open_success) {
    ASSERT_OK(_load_channel_mgr->init(_mem_tracker.get()));
    PUniqueId load_id;
    load_id.set_hi(456789);
    load_id.set_lo(987654);
    brpc::Controller cntl;
    MockClosure closure;
    PTabletWriterOpenRequest request = create_open_request(load_id, rand());
    PTabletWriterOpenResult result;

    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
        config::enable_load_channel_rpc_async = true;
    });
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
    config::enable_load_channel_rpc_async = false;
    _load_channel_mgr->open(&cntl, request, &result, &closure);
    ASSERT_TRUE(closure.has_run());
    ASSERT_TRUE(result.status().status_code() == TStatusCode::OK);
    auto load_channel = _load_channel_mgr->TEST_get_load_channel(UniqueId(load_id));
    ASSERT_TRUE(load_channel != nullptr);
}

} // namespace starrocks