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
#include "common/logging.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/load_channel.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/mem_tracker.h"
#include "serde/protobuf_serde.h"
#include "storage/chunk_helper.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/failpoint/fail_point.h"
#include "util/reusable_closure.h"
#include "util/runtime_profile.h"

namespace starrocks {

class SecondaryReplicasWaiterTestCase;

class LocalTabletsChannelTest : public testing::Test {
protected:
    void SetUp() override {
        srand(GetCurrentTimeMicros());

        for (int i = 0; i < 3; i++) {
            PNetworkAddress node;
            node.set_node_id(i);
            node.set_host(fmt::format("127.0.0.{}", i));
            node.set_port(8060);
            _nodes.push_back(node);
        }
        _load_id.set_hi(456789);
        _load_id.set_lo(987654);
        _txn_id = 10000;
        _db_id = 100;
        _table_id = 101;
        _partition_id = 10;
        _index_id = 1;
        _sink_id = 1;

        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _root_profile = std::make_unique<RuntimeProfile>("LoadChannel");
        _load_channel_mgr = std::make_unique<LoadChannelMgr>();
        auto load_mem_tracker = std::make_unique<MemTracker>(-1, "", _mem_tracker.get());
        _load_channel = std::make_shared<LoadChannel>(_load_channel_mgr.get(), nullptr, _load_id, _txn_id, string(),
                                                      1000, std::move(load_mem_tracker));
        _tablets_channel = new_local_tablets_channel(_load_channel.get(), {_load_id, _sink_id, _index_id},
                                                     _load_channel->mem_tracker(), _root_profile.get());
    }

    void TearDown() override {
        _tablets_channel.reset();
        _load_channel.reset();
        for (auto& tablet : _tablets) {
            auto st = StorageEngine::instance()->tablet_manager()->drop_tablet(tablet->tablet_id());
            ASSERT_OK(st);
            tablet.reset();
        }
    }

    void test_cancel_secondary_replica_base(bool is_empty_tablet);
    void test_secondary_replicas_waiter_base(SecondaryReplicasWaiterTestCase& test_case);

    struct ReplicaInfo {
        int64_t tablet_id;
        std::vector<PNetworkAddress> nodes;
    };

    void _open_channel(int64_t node_id, std::vector<ReplicaInfo> replica_infos) {
        PTabletWriterOpenRequest request;
        _create_open_request(node_id, replica_infos, &request);
        std::shared_ptr<OlapTableSchemaParam> schema_param(new OlapTableSchemaParam());
        ASSERT_OK(schema_param->init(request.schema()));
        PTabletWriterOpenResult response;
        ASSERT_OK(_tablets_channel->open(request, &response, schema_param, false));
    }

    void _create_tablets(int num_tablets) {
        std::unordered_set<int64_t> tablet_ids;
        for (auto& tablet : _tablets) {
            tablet_ids.emplace(tablet->tablet_id());
        }
        int size = _tablets.size() + num_tablets;
        while (_tablets.size() < size) {
            int64_t tablet_id = rand();
            if (tablet_ids.find(tablet_id) != tablet_ids.end()) {
                continue;
            }
            tablet_ids.emplace(tablet_id);
            _tablets.push_back(_create_tablet(tablet_id, rand()));
        }
    }

    TabletSharedPtr _create_tablet(int64_t tablet_id, int32_t schema_hash) {
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

    void _create_open_request(int64_t node_id, std::vector<ReplicaInfo> tablet_vec, PTabletWriterOpenRequest* request) {
        ASSERT_FALSE(tablet_vec.empty());
        request->mutable_id()->CopyFrom(_load_id);
        request->set_index_id(_index_id);
        request->set_txn_id(_txn_id);
        request->set_is_lake_tablet(false);
        request->set_is_replicated_storage(true);
        request->set_node_id(node_id);
        request->set_sink_id(_sink_id);
        request->set_write_quorum(WriteQuorumTypePB::ONE);
        request->set_miss_auto_increment_column(false);
        request->set_table_id(_table_id);
        request->set_is_incremental(false);
        request->set_num_senders(1);
        request->set_sender_id(0);
        request->set_need_gen_rollup(false);
        request->set_load_channel_timeout_s(10);
        request->set_is_vectorized(true);
        request->set_timeout_ms(10000);
        request->set_immutable_tablet_size(0);
        for (auto& tablet_replicas : tablet_vec) {
            auto tablet = request->add_tablets();
            tablet->set_partition_id(_partition_id);
            tablet->set_tablet_id(tablet_replicas.tablet_id);
            for (auto& node : tablet_replicas.nodes) {
                auto replica = tablet->add_replicas();
                replica->CopyFrom(node);
            }
        }

        ASSERT_FALSE(_tablets.empty());
        TabletSharedPtr tablet = _tablets[0];
        auto schema = request->mutable_schema();
        schema->set_db_id(_db_id);
        schema->set_table_id(_table_id);
        schema->set_version(1);
        auto index = schema->add_indexes();
        index->set_id(_index_id);
        index->set_schema_hash(0);
        for (int i = 0, sz = tablet->tablet_schema()->num_columns(); i < sz; i++) {
            auto slot = request->mutable_schema()->add_slot_descs();
            auto& column = tablet->tablet_schema()->column(i);
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
    }

    Chunk _generate_data(int64_t chunk_size, TabletSchemaCSPtr tablet_schema) {
        auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i;
        }
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        Chunk chunk({std::move(c0), std::move(c1)}, schema);
        chunk.set_slot_id_to_index(0, 0);
        chunk.set_slot_id_to_index(1, 1);
        return chunk;
    }

    std::vector<PNetworkAddress> _nodes;
    PUniqueId _load_id;
    int64_t _txn_id;
    int64_t _db_id;
    int64_t _table_id;
    int64_t _partition_id;
    int32_t _index_id;
    int32_t _sink_id;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<RuntimeProfile> _root_profile;
    std::unique_ptr<LoadChannelMgr> _load_channel_mgr;
    std::shared_ptr<LoadChannel> _load_channel;
    std::shared_ptr<LocalTabletsChannel> _tablets_channel;
    std::vector<TabletSharedPtr> _tablets;
};

using RpcLoadDisagnosePair = std::pair<PLoadDiagnoseRequest*, ReusableClosure<PLoadDiagnoseResult>*>;

TEST_F(LocalTabletsChannelTest, test_add_chunk_not_exist_tablet) {
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

    auto non_exist_tablet_id = _tablets[0]->tablet_id() + 1;
    add_chunk_request.add_tablet_ids(non_exist_tablet_id);

    bool close_channel = false;
    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_EQ(TStatusCode::INTERNAL_ERROR, add_chunk_response.status().status_code()) << add_chunk_response.status();
    ASSERT_TRUE(close_channel); // set_eos(true)
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

TEST_F(LocalTabletsChannelTest, test_primary_replica_profile) {
    _create_tablets(1);
    auto& tablet = _tablets[0];
    // open as a primary replica of 1 replica
    ReplicaInfo replica_info{tablet->tablet_id(), {_nodes[0]}};
    _open_channel(_nodes[0].node_id(), {replica_info});

    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);

    int chunk_size = 16;
    auto chunk = _generate_data(chunk_size, tablet->tablet_schema());
    ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
    add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

    for (int i = 0; i < chunk_size; i++) {
        add_chunk_request.add_tablet_ids(tablet->tablet_id());
        add_chunk_request.add_partition_ids(_partition_id);
    }

    bool close_channel;
    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK)
            << add_chunk_response.status().error_msgs(0);
    ASSERT_TRUE(close_channel);

    _tablets_channel->update_profile();
    auto* profile = _root_profile->get_child(fmt::format("Index (id={})", _index_id));
    ASSERT_NE(nullptr, profile);
    ASSERT_EQ(1, profile->get_counter("OpenRpcCount")->value());
    ASSERT_TRUE(profile->get_counter("OpenRpcTime")->value() > 0);
    ASSERT_EQ(1, profile->get_counter("AddChunkRpcCount")->value());
    ASSERT_TRUE(profile->get_counter("AddChunkRpcTime")->value() > 0);
    ASSERT_TRUE(profile->get_counter("SubmitWriteTaskTime")->value() > 0);
    ASSERT_TRUE(profile->get_counter("SubmitCommitTaskTime")->value() > 0);
    ASSERT_EQ(0, profile->get_counter("WaitDrainSenderTime")->value());
    ASSERT_EQ(chunk.num_rows(), profile->get_counter("AddRowNum")->value());
    auto* primary_replicas_profile = profile->get_child("PrimaryReplicas");
    ASSERT_NE(nullptr, primary_replicas_profile);
    ASSERT_EQ(1, primary_replicas_profile->get_counter("TabletsNum")->value());
}

TEST_F(LocalTabletsChannelTest, test_secondary_replica_profile) {
    _create_tablets(1);
    // open as a secondary replica of 3 replicas
    ReplicaInfo replica_info{_tablets[0]->tablet_id(), _nodes};
    _open_channel(_nodes[1].node_id(), {replica_info});
    _tablets_channel->update_profile();
    auto* profile = _root_profile->get_child(fmt::format("Index (id={})", _index_id));
    ASSERT_NE(nullptr, profile);
    ASSERT_EQ(1, profile->get_counter("OpenRpcCount")->value());
    ASSERT_EQ(0, profile->get_counter("AddChunkRpcCount")->value());
    auto* secondary_replicas_profile = profile->get_child("SecondaryReplicas");
    ASSERT_NE(nullptr, secondary_replicas_profile);
    ASSERT_EQ(1, secondary_replicas_profile->get_counter("TabletsNum")->value());
}

using RpcTabletWriterCancelTuple =
        std::tuple<PTabletWriterCancelRequest*, google::protobuf::Closure*, brpc::Controller*>;

void LocalTabletsChannelTest::test_cancel_secondary_replica_base(bool is_empty_tablet) {
    _create_tablets(1);
    auto& tablet = _tablets[0];
    // open as a primary replica of 3 replicas
    ReplicaInfo replica_info{_tablets[0]->tablet_id(), _nodes};
    _open_channel(_nodes[0].node_id(), {replica_info});

    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sink_id(_sink_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_txn_id(_txn_id);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);
    add_chunk_request.set_wait_all_sender_close(true);

    auto chunk = _generate_data(1, tablet->tablet_schema());
    ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
    if (!is_empty_tablet) {
        add_chunk_request.mutable_chunk()->Swap(&chunk_pb);
        add_chunk_request.add_tablet_ids(tablet->tablet_id());
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
        EXPECT_EQ(tablet->tablet_id(), request->tablet_ids().Get(0));
        EXPECT_EQ(request->reason(),
                  is_empty_tablet ? "" : "primary replica on host [] failed to sync data to secondary replica");
        google::protobuf::Closure* closure = std::get<1>(*rpc_tuple);
        closure->Run();
        num_cancel += 1;
    });

    bool close_channel;
    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(is_empty_tablet ? nullptr : &chunk, add_chunk_request, &add_chunk_response,
                                &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK)
            << add_chunk_response.status().error_msgs(0);
    ASSERT_TRUE(close_channel);
    ASSERT_EQ(2, num_cancel);
}

TEST_F(LocalTabletsChannelTest, test_cancel_empty_secondary_replica) {
    test_cancel_secondary_replica_base(true);
}

TEST_F(LocalTabletsChannelTest, test_cancel_failed_secondary_replica) {
    test_cancel_secondary_replica_base(false);
}

TEST_F(LocalTabletsChannelTest, test_cancel_secondary_replica_rpc_fail) {
    _create_tablets(1);
    // open as a primary replica of 3 replicas
    ReplicaInfo replica_info{_tablets[0]->tablet_id(), _nodes};
    _open_channel(_nodes[0].node_id(), {replica_info});

    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sink_id(_sink_id);
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

    bool close_channel;
    PTabletWriterAddBatchResult add_chunk_response;
    _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK)
            << add_chunk_response.status().error_msgs(0);
    ASSERT_TRUE(close_channel);
    ASSERT_EQ(2, num_cancel);
}

using RpcReplicaStatusPair = std::pair<PLoadReplicaStatusRequest*, ReusableClosure<PLoadReplicaStatusResult>*>;

struct RpcStep {
    // expected number of tablets in the request
    int num_tablets;
    // mock the response of the request
    bool mock_response{true};
    bool rpc_fail{false};
    std::string rpc_fail_msg;
    std::vector<LoadReplicaStatePB> replica_states;
    std::vector<std::string> messages;
};

struct TabletExpectedState {
    State writer_state;
    Status error_status;
};

struct SecondaryReplicasWaiterTestCase {
    int64_t timeout_ms{60000};
    std::vector<RpcStep> steps;
    std::vector<TabletExpectedState> final_states;
};

void LocalTabletsChannelTest::test_secondary_replicas_waiter_base(SecondaryReplicasWaiterTestCase& test_case) {
    _create_tablets(3);
    // open as the secondary replica of 3 replicas
    std::vector<ReplicaInfo> replica_infos;
    for (auto& tablet : _tablets) {
        replica_infos.push_back(ReplicaInfo{tablet->tablet_id(), _nodes});
    }
    _open_channel(_nodes[1].node_id(), replica_infos);
    std::unordered_map<int64_t, AsyncDeltaWriter*> writer_map;
    for (auto& [tablet_id, writer] : _tablets_channel->TEST_delta_writers()) {
        writer_map[tablet_id] = writer.get();
    }
    ASSERT_EQ(replica_infos.size(), writer_map.size());
    for (auto& replica : replica_infos) {
        auto writer = writer_map.find(replica.tablet_id);
        ASSERT_NE(writer, writer_map.end());
    }

    auto old_check_interval_on_success = config::load_replica_status_check_interval_ms_on_success;
    auto old_check_interval_on_failure = config::load_replica_status_check_interval_ms_on_failure;
    std::vector<ReusableClosure<PLoadReplicaStatusResult>*> closures_to_release;
    DeferOp defer([&] {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearCallBack("LocalTabletsChannel::rpc::get_load_replica_status");
        config::load_replica_status_check_interval_ms_on_success = old_check_interval_on_success;
        config::load_replica_status_check_interval_ms_on_failure = old_check_interval_on_failure;
        for (auto closure : closures_to_release) {
            closure->Run();
        }
    });

    config::load_replica_status_check_interval_ms_on_success = 50;
    config::load_replica_status_check_interval_ms_on_failure = 20;
    int num_rpc_request = 0;
    std::vector<int64_t> tablet_ids;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("LocalTabletsChannel::rpc::get_load_replica_status", [&](void* arg) {
        RpcReplicaStatusPair* rpc_pair = (RpcReplicaStatusPair*)arg;
        PLoadReplicaStatusRequest* request = (PLoadReplicaStatusRequest*)rpc_pair->first;
        EXPECT_EQ(print_id(_load_id), print_id(request->load_id()));
        EXPECT_EQ(_txn_id, request->txn_id());
        EXPECT_EQ(_index_id, request->index_id());
        EXPECT_EQ(1, request->node_id());
        EXPECT_EQ(_sink_id, request->sink_id());
        EXPECT_TRUE(num_rpc_request < test_case.steps.size());
        auto& step = test_case.steps[num_rpc_request];
        EXPECT_EQ(step.num_tablets, request->tablet_ids().size());
        if (num_rpc_request == 0) {
            tablet_ids.insert(tablet_ids.end(), request->tablet_ids().begin(), request->tablet_ids().end());
        }
        num_rpc_request += 1;
        ReusableClosure<PLoadReplicaStatusResult>* response =
                (ReusableClosure<PLoadReplicaStatusResult>*)rpc_pair->second;
        if (!step.mock_response) {
            closures_to_release.push_back(response);
            return;
        }
        if (step.rpc_fail) {
            response->cntl.SetFailed(step.rpc_fail_msg);
        } else {
            EXPECT_EQ(step.replica_states.size(), request->tablet_ids().size());
            for (int i = 0; i < step.replica_states.size(); i++) {
                auto replica_state = response->result.add_replica_statuses();
                replica_state->set_tablet_id(request->tablet_ids(i));
                replica_state->set_state(step.replica_states[i]);
                replica_state->set_message(step.messages[i]);
            }
        }
        response->Run();
    });

    auto t = std::thread([&]() {
        PTabletWriterAddChunkRequest add_chunk_request;
        add_chunk_request.mutable_id()->CopyFrom(_load_id);
        add_chunk_request.set_index_id(_index_id);
        add_chunk_request.set_sender_id(0);
        add_chunk_request.set_txn_id(_txn_id);
        add_chunk_request.set_sink_id(_sink_id);
        add_chunk_request.set_eos(true);
        add_chunk_request.set_packet_seq(0);
        add_chunk_request.set_wait_all_sender_close(true);
        add_chunk_request.set_timeout_ms(test_case.timeout_ms);

        bool close_channel;
        PTabletWriterAddBatchResult add_chunk_response;
        _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response, &close_channel);
        EXPECT_TRUE(close_channel);
        EXPECT_EQ(add_chunk_response.status().status_code(), TStatusCode::OK)
                << add_chunk_response.status().error_msgs(0);
    });
    t.join();
    ASSERT_EQ(test_case.final_states.size(), tablet_ids.size());
    for (int i = 0; i < tablet_ids.size(); i++) {
        auto tablet_id = tablet_ids[i];
        auto iter = writer_map.find(tablet_id);
        ASSERT_NE(iter, writer_map.end());
        auto writer = iter->second;
        ASSERT_NE(nullptr, writer);
        ASSERT_EQ(test_case.final_states[i].writer_state, writer->writer()->get_state());
        ASSERT_EQ(test_case.final_states[i].error_status.to_string(false),
                  writer->writer()->get_err_status().to_string(false));
    }
}

TEST_F(LocalTabletsChannelTest, test_secondary_replicas_waiter) {
    SecondaryReplicasWaiterTestCase test_case;

    RpcStep step1;
    step1.num_tablets = 3;
    step1.rpc_fail = false;
    step1.replica_states = {LoadReplicaStatePB::NOT_PRESENT, LoadReplicaStatePB::IN_PROCESSING,
                            LoadReplicaStatePB::IN_PROCESSING};
    step1.messages = {"not found", "", ""};
    test_case.steps.push_back(step1);

    RpcStep step2;
    step2.num_tablets = 2;
    step2.rpc_fail = true;
    step2.rpc_fail_msg = "rpc artificial failure 1";
    test_case.steps.push_back(step2);

    RpcStep step3;
    step3.num_tablets = 2;
    step3.rpc_fail = true;
    step3.rpc_fail_msg = "rpc artificial failure 2";
    test_case.steps.push_back(step3);

    RpcStep step4;
    step4.num_tablets = 2;
    step4.rpc_fail = false;
    step4.replica_states = {LoadReplicaStatePB::FAILED, LoadReplicaStatePB::FAILED};
    step4.messages = {"artificial failure 1", "artificial failure 2"};
    test_case.steps.push_back(step4);

    test_case.final_states = {
            TabletExpectedState{
                    kAborted,
                    Status::Cancelled("already failed on primary replica, status: NOT_PRESENT, message: not found")},
            TabletExpectedState{
                    kAborted,
                    Status::Cancelled(
                            "already failed on primary replica, status: FAILED, message: artificial failure 1")},
            TabletExpectedState{
                    kAborted,
                    Status::Cancelled(
                            "already failed on primary replica, status: FAILED, message: artificial failure 2")}};
    test_secondary_replicas_waiter_base(test_case);
}

TEST_F(LocalTabletsChannelTest, test_secondary_repclias_waiter_rpc_fail) {
    SecondaryReplicasWaiterTestCase test_case;
    for (int i = 0; i < 3; i++) {
        RpcStep step;
        step.num_tablets = 3;
        step.rpc_fail = true;
        step.rpc_fail_msg = "rpc artificial failure";
        test_case.steps.push_back(step);
    }
    test_case.final_states = {
            TabletExpectedState{kAborted,
                                Status::Cancelled("can't get status from primary, rpc error: rpc artificial failure")},
            TabletExpectedState{kAborted,
                                Status::Cancelled("can't get status from primary, rpc error: rpc artificial failure")},
            TabletExpectedState{kAborted,
                                Status::Cancelled("can't get status from primary, rpc error: rpc artificial failure")}};
    test_secondary_replicas_waiter_base(test_case);
}

TEST_F(LocalTabletsChannelTest, test_secondary_repclias_waiter_timeout) {
    SecondaryReplicasWaiterTestCase test_case;
    test_case.timeout_ms = 300;
    RpcStep step;
    step.num_tablets = 3;
    step.mock_response = false;
    test_case.steps.push_back(step);
    test_case.final_states = {TabletExpectedState{kWriting, Status::OK()}, TabletExpectedState{kWriting, Status::OK()},
                              TabletExpectedState{kWriting, Status::OK()}};
    test_secondary_replicas_waiter_base(test_case);
}

TEST_F(LocalTabletsChannelTest, test_get_replica_status) {
    _create_tablets(4);
    // open as a primary replica of 3 replicas
    std::vector<ReplicaInfo> replica_infos;
    for (int i = 0; i < 3; i++) {
        auto& tablet = _tablets[i];
        replica_infos.push_back(ReplicaInfo{tablet->tablet_id(), _nodes});
    }
    _open_channel(_nodes[0].node_id(), replica_infos);

    PLoadReplicaStatusRequest request;
    request.mutable_load_id()->CopyFrom(_load_id);
    request.set_txn_id(_txn_id);
    request.set_index_id(_index_id);
    request.set_sink_id(_sink_id);
    request.set_node_id(_nodes[1].node_id());
    for (auto& tablet : _tablets) {
        request.add_tablet_ids(tablet->tablet_id());
    }

    // LoadChannelMgr does not have the load channel
    PLoadReplicaStatusResult result1;
    _load_channel_mgr->get_load_replica_status(nullptr, &request, &result1, nullptr);
    ASSERT_EQ(4, result1.replica_statuses_size());
    for (int i = 0; i < 4; i++) {
        auto& replica_status = result1.replica_statuses().at(i);
        ASSERT_EQ(_tablets[i]->tablet_id(), replica_status.tablet_id());
        ASSERT_EQ(LoadReplicaStatePB::NOT_PRESENT, replica_status.state());
        ASSERT_EQ("can't find load channel", replica_status.message());
    }

    // LoadChannel does not have the tablets channel
    PLoadReplicaStatusResult result2;
    _load_channel->get_load_replica_status(_nodes[1].host(), &request, &result2);
    ASSERT_EQ(4, result2.replica_statuses_size());
    for (int i = 0; i < 4; i++) {
        auto& replica_status = result2.replica_statuses().at(i);
        ASSERT_EQ(_tablets[i]->tablet_id(), replica_status.tablet_id());
        ASSERT_EQ(LoadReplicaStatePB::NOT_PRESENT, replica_status.state());
        ASSERT_EQ("can't find local tablets channel", replica_status.message());
    }

    // 3 IN_PROCESSING, 1 NOT_PRESENT
    PLoadReplicaStatusResult result3;
    _tablets_channel->get_load_replica_status(_nodes[1].host(), &request, &result3);
    ASSERT_EQ(4, result3.replica_statuses_size());
    for (int i = 0; i < 3; i++) {
        auto& replica_status = result3.replica_statuses(i);
        ASSERT_EQ(_tablets[i]->tablet_id(), replica_status.tablet_id());
        ASSERT_EQ(LoadReplicaStatePB::IN_PROCESSING, replica_status.state());
        ASSERT_EQ("primary replica state is kWriting", replica_status.message());
    }
    ASSERT_EQ(_tablets[3]->tablet_id(), result3.replica_statuses(3).tablet_id());
    ASSERT_EQ(LoadReplicaStatePB::NOT_PRESENT, result3.replica_statuses(3).state());
    ASSERT_EQ("can't find delta writer", result3.replica_statuses(3).message());

    // 3 FAILED, 1 NOT_PRESENT
    _tablets_channel->abort({_tablets[0]->tablet_id()}, "artificial failure");
    PTabletWriterAddChunkRequest add_chunk_request;
    add_chunk_request.mutable_id()->CopyFrom(_load_id);
    add_chunk_request.set_index_id(_index_id);
    add_chunk_request.set_sink_id(_sink_id);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_txn_id(_txn_id);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);
    add_chunk_request.set_wait_all_sender_close(true);
    auto chunk = _generate_data(2, _tablets[0]->tablet_schema());
    ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
    add_chunk_request.mutable_chunk()->Swap(&chunk_pb);
    for (int i = 1; i < 3; i++) {
        add_chunk_request.add_tablet_ids(_tablets[i]->tablet_id());
        add_chunk_request.add_partition_ids(_partition_id);
    }
    bool close_channel;
    PTabletWriterAddBatchResult add_chunk_response;
    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearCallBack("LocalTabletsChannel::rpc::tablet_writer_cancel");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("LocalTabletsChannel::rpc::tablet_writer_cancel", [&](void* arg) {
        RpcTabletWriterCancelTuple* rpc_tuple = (RpcTabletWriterCancelTuple*)arg;
        google::protobuf::Closure* closure = std::get<1>(*rpc_tuple);
        closure->Run();
    });
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);
    ASSERT_TRUE(close_channel);

    PLoadReplicaStatusResult result4;
    _tablets_channel->get_load_replica_status(_nodes[1].host(), &request, &result4);
    ASSERT_EQ(4, result4.replica_statuses_size());
    ASSERT_EQ(_tablets[0]->tablet_id(), result4.replica_statuses(0).tablet_id());
    ASSERT_EQ(LoadReplicaStatePB::FAILED, result4.replica_statuses(0).state());
    ASSERT_EQ("primary replica is aborted, Cancelled: artificial failure", result4.replica_statuses(0).message());
    for (int i = 1; i < 3; i++) {
        auto& replica_status = result4.replica_statuses(i);
        ASSERT_EQ(_tablets[i]->tablet_id(), replica_status.tablet_id());
        ASSERT_EQ(LoadReplicaStatePB::FAILED, replica_status.state());
        ASSERT_TRUE(replica_status.message().find("primary replica is committed, but replica failed") !=
                    std::string::npos);
    }
    ASSERT_EQ(_tablets[3]->tablet_id(), result4.replica_statuses(3).tablet_id());
    ASSERT_EQ(LoadReplicaStatePB::NOT_PRESENT, result4.replica_statuses(3).state());
    ASSERT_EQ("can't find delta writer", result4.replica_statuses(3).message());
}

} // namespace starrocks
