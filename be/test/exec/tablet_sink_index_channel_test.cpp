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

#include "exec/tablet_sink_index_channel.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "exec/tablet_info.h"
#include "exec/tablet_sink.h"
#include "runtime/descriptor_helper.h"
#include "storage/chunk_helper.h"
#include "util/thrift_util.h"

namespace starrocks {

class TabletSinkIndexChannelTest : public testing::Test {
public:
    void SetUp() override {
        _db_id = 1;
        _table_id = 2;
        _txn_id = 3;
        _exec_env = ExecEnv::GetInstance();
        _object_pool = std::make_unique<ObjectPool>();
        _desc_tbl = _build_descriptor_table();
        _data_sink = _build_data_sink();
    }

    void test_load_channel_profile_base(RuntimeState* runtime_state, const PLoadChannelProfileConfig& expect_config);

    void test_load_diagnose_base(const std::string& error_text, int64_t rpc_timeout_sec, int expected_num_profile,
                                 int expected_num_stack_trace);

protected:
    std::unique_ptr<RuntimeState> _build_runtime_state(TQueryOptions& query_options) {
        TUniqueId fragment_id;
        TQueryGlobals query_globals;
        return std::make_unique<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
    }

    TDescriptorTable _build_descriptor_table() {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).column_name("c1").column_pos(1).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("c2").column_pos(2).build());
        tuple_builder.build(&dtb);
        return dtb.desc_tbl();
    }

    TDataSink _build_data_sink() {
        TOlapTableSink table_sink;
        table_sink.load_id.hi = 0;
        table_sink.load_id.lo = 0;
        table_sink.db_id = _db_id;
        table_sink.db_name = "test";
        table_sink.table_id = _table_id;
        table_sink.table_name = "test";
        table_sink.txn_id = _txn_id;
        table_sink.num_replicas = 1;
        table_sink.keys_type = TKeysType::DUP_KEYS;
        table_sink.tuple_id = _desc_tbl.tupleDescriptors[0].id;

        TOlapTableSchemaParam& schema = table_sink.schema;
        schema.db_id = _db_id;
        schema.table_id = _table_id;
        schema.version = 0;
        schema.tuple_desc = _desc_tbl.tupleDescriptors[0];
        schema.slot_descs = _desc_tbl.slotDescriptors;
        schema.indexes.resize(1);
        schema.indexes[0].id = 0;
        schema.indexes[0].columns = {"c1", "c2"};

        TOlapTablePartitionParam& partition = table_sink.partition;
        partition.db_id = _db_id;
        partition.table_id = _table_id;
        partition.version = 0;
        partition.distributed_columns.push_back("c1");
        partition.partitions.resize(1);
        partition.partitions[0].id = 0;
        partition.partitions[0].indexes.resize(1);
        partition.partitions[0].indexes[0].index_id = 0;
        partition.partitions[0].indexes[0].tablet_ids.push_back(0);

        TOlapTableLocationParam& location = table_sink.location;
        location.db_id = _db_id;
        location.table_id = _table_id;
        location.version = 0;
        location.tablets.resize(1);
        location.tablets[0].tablet_id = 0;
        location.tablets[0].node_ids.push_back(0);
        location.tablets[0].node_ids.push_back(1);
        location.tablets[0].node_ids.push_back(2);

        TNodesInfo& nodes_info = table_sink.nodes_info;
        nodes_info.version = 0;
        nodes_info.nodes.resize(3);
        for (int i = 0; i < 3; i++) {
            nodes_info.nodes[i].id = i;
            nodes_info.nodes[i].option = 0;
            nodes_info.nodes[i].host = fmt::format("10.128.8.{}", i);
            nodes_info.nodes[i].async_internal_port = 8060;
        }

        TDataSink data_sink;
        data_sink.__set_olap_table_sink(table_sink);
        return data_sink;
    }

    void _serialize_load_profile(int64_t node_id, std::string* result) {
        auto profile = std::make_shared<RuntimeProfile>("LoadChannel");
        profile->add_info_string("LoadId", print_id(_data_sink.olap_table_sink.load_id));
        profile->add_info_string("TxnId", std::to_string(_txn_id));
        auto sub_profile =
                profile->create_child(fmt::format("Channel (host={})", fmt::format("10.128.8.{}", node_id)), true);
        ADD_COUNTER(sub_profile, "IndexNum", TUnit::UNIT)->update(1);
        TRuntimeProfileTree thrift_profile;
        profile->to_thrift(&thrift_profile);
        uint8_t* buf = nullptr;
        uint32_t len = 0;
        ThriftSerializer ser(false, 4096);
        ASSERT_OK(ser.serialize(&thrift_profile, &len, &buf));
        result->append((char*)buf, len);
    }

    int64_t _db_id;
    int64_t _table_id;
    int64_t _txn_id;

    ExecEnv* _exec_env;
    std::unique_ptr<ObjectPool> _object_pool;
    TDescriptorTable _desc_tbl;
    TDataSink _data_sink;
};

void TabletSinkIndexChannelTest::test_load_channel_profile_base(RuntimeState* runtime_state,
                                                                const PLoadChannelProfileConfig& expect_config) {
    DescriptorTbl* desc_tbl = nullptr;
    ASSERT_OK(
            DescriptorTbl::create(runtime_state, _object_pool.get(), _desc_tbl, &desc_tbl, config::vector_chunk_size));
    runtime_state->set_desc_tbl(desc_tbl);
    auto sink = std::make_unique<OlapTableSink>(_object_pool.get(), std::vector<TExpr>(), nullptr, runtime_state);
    ASSERT_OK(sink->init(_data_sink, runtime_state));
    ASSERT_OK(sink->prepare(runtime_state));
    auto actual_config = sink->load_channel_profile_config();
    ASSERT_EQ(expect_config.has_enable_profile(), actual_config.has_enable_profile());
    if (expect_config.has_enable_profile()) {
        ASSERT_EQ(expect_config.enable_profile(), actual_config.enable_profile());
    }
    ASSERT_EQ(expect_config.has_big_query_profile_threshold_ns(), actual_config.has_big_query_profile_threshold_ns());
    if (expect_config.has_big_query_profile_threshold_ns()) {
        ASSERT_EQ(expect_config.big_query_profile_threshold_ns(), actual_config.big_query_profile_threshold_ns());
    }
    ASSERT_EQ(expect_config.has_runtime_profile_report_interval_ns(),
              actual_config.has_runtime_profile_report_interval_ns());
    if (expect_config.has_runtime_profile_report_interval_ns()) {
        ASSERT_EQ(expect_config.runtime_profile_report_interval_ns(),
                  actual_config.runtime_profile_report_interval_ns());
    }
}

TEST_F(TabletSinkIndexChannelTest, non_pipeline_load_channel_profile) {
    {
        // not set enable_profile and load_profile_collect_second
        TQueryOptions query_options;
        auto runtime_state = _build_runtime_state(query_options);
        PLoadChannelProfileConfig expect_config;
        expect_config.set_enable_profile(false);
        expect_config.set_big_query_profile_threshold_ns(-1);
        expect_config.set_runtime_profile_report_interval_ns(std::numeric_limits<int64_t>::max());
        test_load_channel_profile_base(runtime_state.get(), expect_config);
    }

    {
        // only set load_profile_collect_second
        TQueryOptions query_options;
        query_options.__set_load_profile_collect_second(10);
        auto runtime_state = _build_runtime_state(query_options);
        PLoadChannelProfileConfig expect_config;
        expect_config.set_enable_profile(false);
        expect_config.set_big_query_profile_threshold_ns(-1);
        expect_config.set_runtime_profile_report_interval_ns(std::numeric_limits<int64_t>::max());
        test_load_channel_profile_base(runtime_state.get(), expect_config);
    }

    {
        // only set enable_profile
        TQueryOptions query_options;
        query_options.__set_enable_profile(true);
        auto runtime_state = _build_runtime_state(query_options);
        PLoadChannelProfileConfig expect_config;
        expect_config.set_enable_profile(true);
        expect_config.set_big_query_profile_threshold_ns(-1);
        expect_config.set_runtime_profile_report_interval_ns(std::numeric_limits<int64_t>::max());
        test_load_channel_profile_base(runtime_state.get(), expect_config);
    }

    {
        // set both enable_profile and load_profile_collect_second
        TQueryOptions query_options;
        query_options.__set_enable_profile(true);
        query_options.__set_load_profile_collect_second(10);
        auto runtime_state = _build_runtime_state(query_options);
        PLoadChannelProfileConfig expect_config;
        expect_config.set_enable_profile(false);
        expect_config.set_big_query_profile_threshold_ns(10 * 1e9);
        expect_config.set_runtime_profile_report_interval_ns(std::numeric_limits<int64_t>::max());
        test_load_channel_profile_base(runtime_state.get(), expect_config);
    }
}

TEST_F(TabletSinkIndexChannelTest, pipeline_load_channel_profile) {
    TQueryOptions query_options;
    pipeline::QueryContext query_ctx;
    query_ctx.set_enable_profile();
    query_ctx.set_big_query_profile_threshold(10, TTimeUnit::SECOND);
    query_ctx.set_runtime_profile_report_interval(5);
    auto runtime_state = _build_runtime_state(query_options);
    runtime_state->set_query_ctx(&query_ctx);
    PLoadChannelProfileConfig expect_config;
    expect_config.set_enable_profile(true);
    expect_config.set_big_query_profile_threshold_ns(10 * 1e9);
    expect_config.set_runtime_profile_report_interval_ns(5 * 1e9);
    test_load_channel_profile_base(runtime_state.get(), expect_config);
}

using RpcOpenPair = std::pair<PTabletWriterOpenRequest*, RefCountClosure<PTabletWriterOpenResult>*>;
using RpcAddChunkTuple =
        std::tuple<int64_t, PTabletWriterAddChunksRequest*, ReusableClosure<PTabletWriterAddBatchResult>*>;
using RpcLoadDisagnoseTuple = std::tuple<int64_t, PLoadDiagnoseRequest*, RefCountClosure<PLoadDiagnoseResult>*>;

void TabletSinkIndexChannelTest::test_load_diagnose_base(const std::string& error_text, int64_t rpc_timeout_sec,
                                                         int expected_num_profile, int expected_num_stack_trace) {
    TQueryOptions query_options;
    query_options.__set_query_timeout(2 * rpc_timeout_sec);
    auto runtime_state = _build_runtime_state(query_options);
    DescriptorTbl* desc_tbl = nullptr;
    ASSERT_OK(DescriptorTbl::create(runtime_state.get(), _object_pool.get(), _desc_tbl, &desc_tbl,
                                    config::vector_chunk_size));
    runtime_state->set_desc_tbl(desc_tbl);
    auto sink = std::make_unique<OlapTableSink>(_object_pool.get(), std::vector<TExpr>(), nullptr, runtime_state.get());
    ASSERT_OK(sink->init(_data_sink, runtime_state.get()));
    ASSERT_OK(sink->prepare(runtime_state.get()));

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::open_send");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::open_join");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::add_chunk_send");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::add_chunk_join");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::load_diagnose_send");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::load_diagnose_join");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::open_send", [&](void* arg) {
        RpcOpenPair* rpc_pair = (RpcOpenPair*)arg;
        RefCountClosure<PTabletWriterOpenResult>* closure = rpc_pair->second;
        closure->result.mutable_status()->set_status_code(TStatusCode::OK);
        closure->Run();
    });
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::open_join", [&](void* arg) {
        RefCountClosure<PTabletWriterOpenResult>* closure = (RefCountClosure<PTabletWriterOpenResult>*)arg;
        EXPECT_FALSE(closure->cntl.Failed());
        EXPECT_EQ(TStatusCode::OK, closure->result.status().status_code());
    });
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::add_chunk_send", [&](void* arg) {
        RpcAddChunkTuple* rpc_tuple = (RpcAddChunkTuple*)arg;
        ReusableClosure<PTabletWriterAddBatchResult>* closure = std::get<2>(*rpc_tuple);
        closure->cntl.SetFailed(error_text);
        closure->Run();
    });
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::add_chunk_join", [&](void* arg) {
        std::pair<ReusableClosure<PTabletWriterAddBatchResult>*, bool*>* rpc_pair =
                (std::pair<ReusableClosure<PTabletWriterAddBatchResult>*, bool*>*)arg;
        ReusableClosure<PTabletWriterAddBatchResult>* closure = rpc_pair->first;
        EXPECT_TRUE(closure->cntl.Failed());
        EXPECT_TRUE(closure->cntl.ErrorText().find(error_text) != std::string::npos);
        *rpc_pair->second = true;
    });

    int32_t num_profile = 0;
    int32_t num_stack_trace = 0;
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::load_diagnose_send", [&](void* arg) {
        RpcLoadDisagnoseTuple* rpc_tuple = (RpcLoadDisagnoseTuple*)arg;
        PLoadDiagnoseRequest* request = std::get<1>(*rpc_tuple);
        RefCountClosure<PLoadDiagnoseResult>* closure = std::get<2>(*rpc_tuple);
        if (request->has_profile() && request->profile()) {
            closure->result.mutable_profile_status()->set_status_code(TStatusCode::OK);
            _serialize_load_profile(std::get<0>(*rpc_tuple), closure->result.mutable_profile_data());
            num_profile += 1;
        }
        if (request->has_stack_trace() && request->stack_trace()) {
            closure->result.mutable_stack_trace_status()->set_status_code(TStatusCode::OK);
            num_stack_trace += 1;
        }
        closure->Run();
    });
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::load_diagnose_join", [&](void* arg) {
        RefCountClosure<PLoadDiagnoseResult>* closure = (RefCountClosure<PLoadDiagnoseResult>*)arg;
        EXPECT_EQ(TStatusCode::OK, closure->result.profile_status().status_code());
    });

    ASSERT_OK(sink->open(runtime_state.get()));
    auto tuple_desc = runtime_state->desc_tbl().get_tuple_descriptor(_desc_tbl.tupleDescriptors[0].id);
    ChunkUniquePtr chunk = ChunkHelper::new_chunk(*tuple_desc, 1);
    chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(1));
    chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(1L));
    ASSERT_OK(sink->send_chunk(runtime_state.get(), chunk.get()));
    ASSERT_FALSE(sink->close(runtime_state.get(), Status::OK()).ok());
    ASSERT_EQ(expected_num_stack_trace, num_stack_trace);
    ASSERT_EQ(expected_num_profile, num_profile);
    ASSERT_EQ(expected_num_profile, runtime_state->load_channel_profile()->num_children());
}

TEST_F(TabletSinkIndexChannelTest, load_diagnose) {
    // not diagnose because the error is no rpc timeout
    test_load_diagnose_base("artificial failure", 30, false, false);
    // only diagnose profile. it's a small rpc timeout which is less than
    // config::load_diagnose_rpc_timeout_profile_threshold_ms and
    // config::load_diagnose_rpc_timeout_stack_trace_threshold_ms
    test_load_diagnose_base("[E1008]Reached timeout 30000ms@10.128.8.78:8060", 30, 1, 0);
    // not diagnose profile. it's a small rpc timeout, and only trigger profile every 20 times
    test_load_diagnose_base("[E1008]Reached timeout 30000ms@10.128.8.78:8060", 30, 0, 0);
    // diagnose both profile and stack trace because the timeout is larger than
    // config::load_diagnose_rpc_timeout_stack_trace_threshold_ms
    test_load_diagnose_base("[E1008]Reached timeout 1200000ms@10.128.8.78:8060", 1200, 3, 3);
}

TEST_F(TabletSinkIndexChannelTest, primary_replica_node_not_connected) {
    TQueryOptions query_options;
    query_options.__set_batch_size(4096);
    query_options.__set_query_timeout(3600);
    auto runtime_state = _build_runtime_state(query_options);
    DescriptorTbl* desc_tbl = nullptr;
    ASSERT_OK(DescriptorTbl::create(runtime_state.get(), _object_pool.get(), _desc_tbl, &desc_tbl,
                                    config::vector_chunk_size));
    runtime_state->set_desc_tbl(desc_tbl);
    auto sink = std::make_unique<OlapTableSink>(_object_pool.get(), std::vector<TExpr>(), nullptr, runtime_state.get());
    ASSERT_OK(sink->init(_data_sink, runtime_state.get()));
    ASSERT_OK(sink->prepare(runtime_state.get()));

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::open_send");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::open_join");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::add_chunk_send");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::add_chunk_join");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::open_send", [&](void* arg) {
        RpcOpenPair* rpc_pair = (RpcOpenPair*)arg;
        RefCountClosure<PTabletWriterOpenResult>* closure = rpc_pair->second;
        closure->result.mutable_status()->set_status_code(TStatusCode::OK);
        closure->Run();
    });
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::open_join", [&](void* arg) {
        RefCountClosure<PTabletWriterOpenResult>* closure = (RefCountClosure<PTabletWriterOpenResult>*)arg;
        EXPECT_FALSE(closure->cntl.Failed());
        EXPECT_EQ(TStatusCode::OK, closure->result.status().status_code());
    });
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::add_chunk_send", [&](void* arg) {
        RpcAddChunkTuple* rpc_tuple = (RpcAddChunkTuple*)arg;
        // simulate the case where secondary replicas are waiting for the primary replica,
        // so will not reponse to the cooridnator be
        if (std::get<0>(*rpc_tuple) != 0) {
            return;
        }
        ReusableClosure<PTabletWriterAddBatchResult>* closure = std::get<2>(*rpc_tuple);
        closure->cntl.SetFailed("[R1][E112]Not connected to [10.128.8.0:8060]");
        closure->Run();
    });
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::add_chunk_join", [&](void* arg) {
        std::pair<ReusableClosure<PTabletWriterAddBatchResult>*, bool*>* rpc_pair =
                (std::pair<ReusableClosure<PTabletWriterAddBatchResult>*, bool*>*)arg;
        ReusableClosure<PTabletWriterAddBatchResult>* closure = rpc_pair->first;
        EXPECT_TRUE(closure->cntl.Failed());
        *rpc_pair->second = true;
    });

    ASSERT_OK(sink->open(runtime_state.get()));
    auto tuple_desc = runtime_state->desc_tbl().get_tuple_descriptor(_desc_tbl.tupleDescriptors[0].id);
    ChunkUniquePtr chunk = ChunkHelper::new_chunk(*tuple_desc, 1);
    chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(1));
    ASSERT_OK(sink->send_chunk(runtime_state.get(), chunk.get()));
    Status status = sink->close(runtime_state.get(), Status::OK());
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.message().find("[R1][E112]Not connected to [10.128.8.0:8060]") != std::string::npos);
}

} // namespace starrocks