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

#include "runtime/local_tablets_channel.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/load_channel.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/mem_tracker.h"
#include "serde/protobuf_serde.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/segment_options.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/runtime_profile.h"

namespace starrocks {

class LocalTabletsChannelTest : public testing::Test {
protected:
    void SetUp() override {
        srand(GetCurrentTimeMicros());

        _node_id = 100;
        _secondary_node_id = 101;
        _load_id.set_hi(456789);
        _load_id.set_lo(987654);
        _txn_id = 10000;
        _db_id = 100;
        _table_id = 101;
        _partition_id = 10;
        _index_id = 1;
        _tablet_id = rand();
        _tablet = create_tablet(_tablet_id, rand());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet->tablet_schema()));

        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _root_profile = std::make_unique<RuntimeProfile>("LoadChannel");
        _load_channel_mgr = std::make_unique<LoadChannelMgr>();
        auto load_mem_tracker = std::make_unique<MemTracker>(-1, "", _mem_tracker.get());
        _load_channel = std::make_shared<LoadChannel>(_load_channel_mgr.get(), nullptr, _load_id, _txn_id, string(),
                                                      1000, std::move(load_mem_tracker));
        _open_single_replica_request = create_open_request(true);
        _open_primary_request = create_open_request(false);
        TabletsChannelKey key{_load_id, 0, _index_id};
        _schema_param.reset(new OlapTableSchemaParam());
        ASSERT_OK(_schema_param->init(_open_single_replica_request.schema()));
        _tablets_channel =
                new_local_tablets_channel(_load_channel.get(), key, _load_channel->mem_tracker(), _root_profile.get());
    }

    void TearDown() override {
        _tablets_channel.reset();
        _load_channel.reset();
        if (_tablet) {
            _tablet.reset();
            auto st = StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet_id);
            ASSERT_OK(st);
        }
    }

    void test_cancel_secondary_replica_base(bool is_empty_tablet);

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

    PTabletWriterOpenRequest create_open_request(bool single_replica) {
        PTabletWriterOpenRequest request;
        request.mutable_id()->CopyFrom(_load_id);
        request.set_index_id(_index_id);
        request.set_txn_id(_txn_id);
        request.set_is_lake_tablet(false);
        request.set_is_replicated_storage(true);
        request.set_node_id(_node_id);
        request.set_write_quorum(WriteQuorumTypePB::ONE);
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
        if (!single_replica) {
            auto secondary_replica = tablet->add_replicas();
            secondary_replica->set_host("127.0.0.2");
            secondary_replica->set_port(8060);
            secondary_replica->set_node_id(_secondary_node_id);
        }

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

    Chunk generate_data(int64_t chunk_size) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i;
        }
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        Chunk chunk({c0, c1}, _schema);
        chunk.set_slot_id_to_index(0, 0);
        chunk.set_slot_id_to_index(1, 1);
        return chunk;
    }

    int64_t _node_id;
    int64_t _secondary_node_id;
    PUniqueId _load_id;
    int64_t _txn_id;
    int64_t _db_id;
    int64_t _table_id;
    int64_t _partition_id;
    int32_t _index_id;
    int64_t _tablet_id;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<RuntimeProfile> _root_profile;
    std::unique_ptr<LoadChannelMgr> _load_channel_mgr;
    std::shared_ptr<LoadChannel> _load_channel;
    std::shared_ptr<TabletsChannel> _tablets_channel;

    TabletSharedPtr _tablet;
    std::shared_ptr<Schema> _schema;
    std::shared_ptr<OlapTableSchemaParam> _schema_param;

    PTabletWriterOpenRequest _open_single_replica_request;
    PTabletWriterOpenRequest _open_primary_request;
    PTabletWriterOpenResult _open_response;
};

<<<<<<< HEAD
TEST_F(LocalTabletsChannelTest, test_profile) {
    auto open_request = _open_single_replica_request;
    ASSERT_OK(_tablets_channel->open(open_request, &_open_response, _schema_param, false));
=======
using RpcLoadDisagnosePair = std::pair<PLoadDiagnoseRequest*, ReusableClosure<PLoadDiagnoseResult>*>;

TEST_F(LocalTabletsChannelTest, test_add_chunk_not_exist_tablet) {
    _create_tablets(1);

    ReplicaInfo replica_info{_tablets[0]->tablet_id(), _nodes};
    PTabletWriterOpenRequest request;
    _create_open_request(_nodes[1].node_id(), {replica_info}, &request);
    // turn off _is_replicated_storage to avoid launching secondary waiter
    request.set_is_replicated_storage(false);

    std::shared_ptr<OlapTableSchemaParam> schema_param(new OlapTableSchemaParam());
    ASSERT_OK(schema_param->init(request.schema()));
    PTabletWriterOpenResult response;
    ASSERT_OK(_tablets_channel->open(request, &response, schema_param, false));

    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sink_id(_sink_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);
    add_chunk_request.set_timeout_ms(60000);

    // NOTE: this is a malformed request, because the chunk is nullptr but tablet_ids is not empty().
    auto non_exist_tablet_id = _tablets[0]->tablet_id() + 1;
    add_chunk_request.add_tablet_ids(non_exist_tablet_id);

    bool close_channel = false;
    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_EQ(TStatusCode::INTERNAL_ERROR, add_chunk_response.status().status_code()) << add_chunk_response.status();
    ASSERT_TRUE(close_channel); // set_eos(true)
    _tablets_channel->abort();
}

TEST_F(LocalTabletsChannelTest, test_add_chunk_not_exist_tablet_for_chunk_rows) {
    // create 3 tablets
    _create_tablets(3);

    // open tablets[0] & tablets[1]
    std::vector<ReplicaInfo> replica_infos;
    replica_infos.emplace_back(ReplicaInfo{_tablets[0]->tablet_id(), _nodes});
    replica_infos.emplace_back(ReplicaInfo{_tablets[1]->tablet_id(), _nodes});
    PTabletWriterOpenRequest request;
    _create_open_request(_nodes[1].node_id(), replica_infos, &request);
    // Turn off _is_replicated_storage to avoid launching secondary waiter
    request.set_is_replicated_storage(false);

    std::shared_ptr<OlapTableSchemaParam> schema_param(new OlapTableSchemaParam());
    ASSERT_OK(schema_param->init(request.schema()));
    PTabletWriterOpenResult response;
    ASSERT_OK(_tablets_channel->open(request, &response, schema_param, false));

    PTabletWriterAddChunkRequest add_chunk_request;
    PTabletWriterAddBatchResult add_chunk_response;

    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sink_id(_sink_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(false);
    add_chunk_request.set_packet_seq(0);
    add_chunk_request.set_timeout_ms(60000);

    {
        int num_rows = 10;
        auto chunk = _generate_data(num_rows, _tablets[0]->tablet_schema());
        for (int i = 0; i < num_rows; i++) {
            // tablets[2] not opened, so every 3 rows, there will be one row whose tablet_id can't be found.
            add_chunk_request.add_tablet_ids(_tablets[i % 3]->tablet_id());
            add_chunk_request.add_partition_ids(_partition_id);
        }
        bool close_channel = false;
        _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
        // chunk is released when out of the scope, simulating the resource release after RPC done.
    }

    ASSERT_EQ(TStatusCode::INTERNAL_ERROR, add_chunk_response.status().status_code()) << add_chunk_response.status();
    _tablets_channel->abort();
}

TEST_F(LocalTabletsChannelTest, diagnose_stack_trace) {
    _create_tablets(1);
    // open as a secondary replica of 3 replicas
    ReplicaInfo replica_info{_tablets[0]->tablet_id(), _nodes};
    _open_channel(_nodes[1].node_id(), {replica_info});

    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sink_id(_sink_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);
    add_chunk_request.set_timeout_ms(100);

    auto old_threshold = config::load_diagnose_rpc_timeout_stack_trace_threshold_ms;
    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearCallBack("LocalTabletsChannel::rpc::load_diagnose_send");
        SyncPoint::GetInstance()->DisableProcessing();
        config::load_diagnose_rpc_timeout_stack_trace_threshold_ms = old_threshold;
    });
    config::load_diagnose_rpc_timeout_stack_trace_threshold_ms = 0;

    int32_t num_diagnose = 0;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("LocalTabletsChannel::rpc::load_diagnose_send", [&](void* arg) {
        RpcLoadDisagnosePair* rpc_pair = (RpcLoadDisagnosePair*)arg;
        PLoadDiagnoseRequest* request = rpc_pair->first;
        ReusableClosure<PLoadDiagnoseResult>* closure = rpc_pair->second;
        EXPECT_FALSE(request->has_profile());
        EXPECT_TRUE(request->has_stack_trace() && request->stack_trace());
        closure->result.mutable_stack_trace_status()->set_status_code(TStatusCode::OK);
        closure->Run();
        num_diagnose += 1;
    });

    bool close_channel;
    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK)
            << add_chunk_response.status().error_msgs(0);
    ASSERT_TRUE(close_channel);
    ASSERT_EQ(1, num_diagnose);
}

TEST_F(LocalTabletsChannelTest, test_add_chunk_missing_timeout_ms) {
    _create_tablets(1);
    // open as a secondary replica of 3 replicas
    ReplicaInfo replica_info{_tablets[0]->tablet_id(), _nodes};
    _open_channel(_nodes[1].node_id(), {replica_info});

    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sink_id(_sink_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);
    // intentionally do NOT set timeout_ms to trigger validation error

    bool close_channel = true; // will be reset to false inside add_chunk
    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, add_chunk_response.status().status_code()) << add_chunk_response.status();
    ASSERT_TRUE(add_chunk_response.status().error_msgs_size() > 0);
    ASSERT_TRUE(add_chunk_response.status().error_msgs(0).find("missing timeout_ms") != std::string::npos)
            << add_chunk_response.status().error_msgs(0);
    ASSERT_FALSE(close_channel);
}

TEST_F(LocalTabletsChannelTest, test_add_chunk_negative_timeout_ms) {
    _create_tablets(1);
    // open as a secondary replica of 3 replicas
    ReplicaInfo replica_info{_tablets[0]->tablet_id(), _nodes};
    _open_channel(_nodes[1].node_id(), {replica_info});

    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sink_id(_sink_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);
    add_chunk_request.set_timeout_ms(-1); // negative to trigger validation error

    bool close_channel = true; // will be reset to false inside add_chunk
    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, add_chunk_response.status().status_code()) << add_chunk_response.status();
    ASSERT_TRUE(add_chunk_response.status().error_msgs_size() > 0);
    ASSERT_TRUE(add_chunk_response.status().error_msgs(0).find("negtive timeout_ms") != std::string::npos)
            << add_chunk_response.status().error_msgs(0);
    ASSERT_FALSE(close_channel);
}

TEST_F(LocalTabletsChannelTest, test_primary_replica_profile) {
    _create_tablets(1);
    auto& tablet = _tablets[0];
    // open as a primary replica of 1 replica
    ReplicaInfo replica_info{tablet->tablet_id(), {_nodes[0]}};
    _open_channel(_nodes[0].node_id(), {replica_info});
>>>>>>> f3c417348b ([BugFix] Fix tablets channel use-after-free issue (#64360))

    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);

    int chunk_size = 16;
    auto chunk = generate_data(chunk_size);
    ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
    add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

    for (int i = 0; i < chunk_size; i++) {
        add_chunk_request.add_tablet_ids(_tablet_id);
        add_chunk_request.add_partition_ids(_partition_id);
    }

    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK)
            << add_chunk_response.status().error_msgs(0);

    auto* profile = _root_profile->get_child(fmt::format("Index (id={})", _index_id));
    ASSERT_NE(nullptr, profile);
    ASSERT_EQ(1, profile->get_counter("PrimaryTabletsNum")->value());
    ASSERT_EQ(0, profile->get_counter("SecondaryTabletsNum")->value());
    ASSERT_EQ(1, profile->get_counter("OpenCount")->value());
    ASSERT_TRUE(profile->get_counter("OpenTime")->value() > 0);
    ASSERT_EQ(1, profile->get_counter("AddChunkCount")->value());
    ASSERT_TRUE(profile->get_counter("AddChunkTime")->value() > 0);
    ASSERT_EQ(chunk.num_rows(), profile->get_counter("AddRowNum")->value());
}

TEST_F(LocalTabletsChannelTest, test_add_chunk_not_exist_tablet) {
    auto open_request = _open_single_replica_request;
    ASSERT_OK(_tablets_channel->open(open_request, &_open_response, _schema_param, false));

    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);

    auto non_exist_tablet_id = _tablet->tablet_id() + 1;
    add_chunk_request.add_tablet_ids(non_exist_tablet_id);

    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response);
    ASSERT_EQ(TStatusCode::INTERNAL_ERROR, add_chunk_response.status().status_code()) << add_chunk_response.status();
    _tablets_channel->abort();
}

using RpcTabletWriterCancelTuple =
        std::tuple<PTabletWriterCancelRequest*, google::protobuf::Closure*, brpc::Controller*>;

void LocalTabletsChannelTest::test_cancel_secondary_replica_base(bool is_empty_tablet) {
    ASSERT_OK(_tablets_channel->open(_open_primary_request, &_open_response, _schema_param, false));

    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_txn_id(_txn_id);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);
    add_chunk_request.set_wait_all_sender_close(true);

    auto chunk = generate_data(1);
    ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
    if (!is_empty_tablet) {
        add_chunk_request.mutable_chunk()->Swap(&chunk_pb);
        add_chunk_request.add_tablet_ids(_tablet_id);
        add_chunk_request.add_partition_ids(_partition_id);
    }

    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearCallBack("LocalTabletsChannel::rpc::tablet_writer_cancel");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    int num_cancel = 0;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("LocalTabletsChannel::rpc::tablet_writer_cancel", [&](void* arg) {
        RpcTabletWriterCancelTuple* rpc_tuple = (RpcTabletWriterCancelTuple*)arg;
        PTabletWriterCancelRequest* request = std::get<0>(*rpc_tuple);
        EXPECT_EQ(print_id(request->id()), print_id(_load_id));
        EXPECT_EQ(request->index_id(), _index_id);
        EXPECT_EQ(request->sender_id(), 0);
        EXPECT_EQ(request->txn_id(), _txn_id);
        EXPECT_EQ(1, request->tablet_ids().size());
        EXPECT_EQ(_tablet_id, request->tablet_ids().Get(0));
        EXPECT_EQ(request->reason(),
                  is_empty_tablet ? "" : "primary replica on host [] failed to sync data to secondary replica");
        google::protobuf::Closure* closure = std::get<1>(*rpc_tuple);
        closure->Run();
        num_cancel += 1;
    });

    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(is_empty_tablet ? nullptr : &chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK)
            << add_chunk_response.status().error_msgs(0);
    ASSERT_EQ(1, num_cancel);
}

TEST_F(LocalTabletsChannelTest, test_cancel_empty_secondary_replica) {
    test_cancel_secondary_replica_base(true);
}

TEST_F(LocalTabletsChannelTest, test_cancel_failed_secondary_replica) {
    test_cancel_secondary_replica_base(false);
}

TEST_F(LocalTabletsChannelTest, test_cancel_secondary_replica_rpc_fail) {
    ASSERT_OK(_tablets_channel->open(_open_primary_request, &_open_response, _schema_param, false));

    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_txn_id(_txn_id);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);
    add_chunk_request.set_wait_all_sender_close(true);

    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearCallBack("LocalTabletsChannel::rpc::tablet_writer_cancel");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    int num_cancel = 0;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("LocalTabletsChannel::rpc::tablet_writer_cancel", [&](void* arg) {
        RpcTabletWriterCancelTuple* rpc_tuple = (RpcTabletWriterCancelTuple*)arg;
        google::protobuf::Closure* closure = std::get<1>(*rpc_tuple);
        brpc::Controller* cntl = std::get<2>(*rpc_tuple);
        cntl->SetFailed("artificial intelligent rpc failure");
        closure->Run();
        num_cancel += 1;
    });

    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK)
            << add_chunk_response.status().error_msgs(0);
    ASSERT_EQ(1, num_cancel);
}

} // namespace starrocks
