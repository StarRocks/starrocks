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

#include "data_sink/tablet/tablet_sink_index_channel.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/serde/column_array_serde.h"
#include "column/serde/encode_level.h"
#include "common/config_exec_fwd.h"
#include "common/config_ingest_fwd.h"
#include "common/flexible_partial_update.h"
#include "common/util/thrift_util.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "data_sink/tablet/olap_table_sink.h"
#include "exec/exec_env.h"
#include "exec/pipeline/query_context.h"
#include "runtime/chunk_helper.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage_primitive/tablet_info.h"

namespace starrocks {

class TabletNonPoolCompressCodec final : public BlockCompressionCodec {
public:
    TabletNonPoolCompressCodec() : BlockCompressionCodec(SNAPPY) {}

    Status compress(const Slice& input, Slice* output, bool use_compression_buffer, size_t uncompressed_size,
                    faststring* compressed_body1, raw::RawString* compressed_body2,
                    const BlockCompressionOptions& /*options*/) const override {
        output->data[0] = 'x';
        output->size = 1;
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) const override { return Status::NotSupported("mock"); }

    size_t max_compressed_len(size_t len) const override { return len; }
};

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
        auto runtime_state = std::make_unique<RuntimeState>(fragment_id, query_options, query_globals,
                                                            &_exec_env->query_execution_services(), _exec_env);
        _fragment_dict_states.emplace_back(std::make_unique<FragmentDictState>());
        runtime_state->set_fragment_dict_state(_fragment_dict_states.back().get());
        return runtime_state;
    }

    TDescriptorTable _build_descriptor_table() {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).column_name("c1").column_pos(1).build());
        // c2 is nullable: TSlotDescriptorBuilder leaves isNullable unset, which thrift defaults to
        // false, and append_nulls() on a non-nullable column yields no NULL at all. The all-NULL
        // chunk below needs a column that can actually hold one.
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("c2").column_pos(2).nullable(true).build());
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

    std::unique_ptr<OlapTableSink> _build_prepared_sink(RuntimeState* runtime_state) {
        DescriptorTbl* desc_tbl = nullptr;
        CHECK_OK(DescriptorTbl::create(runtime_state, _object_pool.get(), _desc_tbl, &desc_tbl,
                                       config::vector_chunk_size));
        runtime_state->set_desc_tbl(desc_tbl);
        auto sink = std::make_unique<OlapTableSink>(_object_pool.get(), std::vector<TExpr>(), nullptr, runtime_state);
        CHECK_OK(sink->init(_data_sink, runtime_state));
        CHECK_OK(sink->prepare(runtime_state));
        return sink;
    }

    ChunkUniquePtr _build_test_chunk(RuntimeState* runtime_state) {
        auto tuple_desc = runtime_state->desc_tbl().get_tuple_descriptor(_desc_tbl.tupleDescriptors[0].id);
        ChunkUniquePtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 1);
        chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(1));
        chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(int64_t(1)));
        return chunk;
    }

    // Same shape, but column 1 is entirely NULL. The sink only turns the bit on for a chunk that
    // actually contains an all-NULL column, so this is what exercises the negotiated path.
    ChunkUniquePtr _build_all_null_test_chunk(RuntimeState* runtime_state) {
        auto tuple_desc = runtime_state->desc_tbl().get_tuple_descriptor(_desc_tbl.tupleDescriptors[0].id);
        ChunkUniquePtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 1);
        chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(1));
        chunk->get_column_raw_ptr_by_index(1)->append_nulls(1);
        // Assert the shape really has the property it is built for. A chunk that merely looks
        // all-NULL silently turns these tests into a test of the legacy path.
        CHECK(serde::is_all_null_column(*chunk->get_column_by_index(1)))
                << "column 1 is not all-NULL; the slot is probably not nullable";
        return chunk;
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
    std::vector<std::unique_ptr<FragmentDictState>> _fragment_dict_states;
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
    auto& query_runtime_state = query_ctx.query_runtime_state();
    query_runtime_state.set_enable_profile();
    query_runtime_state.set_big_query_profile_threshold(10, TTimeUnit::SECOND);
    query_runtime_state.set_runtime_profile_report_interval(5);
    auto runtime_state = _build_runtime_state(query_options);
    runtime_state->set_query_ctx(&query_ctx, &query_ctx.query_runtime_state(), query_ctx.object_pool());
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
    ChunkUniquePtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 1);
    chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(1));
    chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(int64_t(1)));
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

TEST_F(TabletSinkIndexChannelTest, serialize_chunk_non_pool_codec_compresses) {
    TQueryOptions query_options;
    auto runtime_state = _build_runtime_state(query_options);
    auto sink = _build_prepared_sink(runtime_state.get());
    NodeChannel channel(sink.get(), 0, false);
    TabletNonPoolCompressCodec codec;
    channel._compress_codec = &codec;
    channel._compress_type = codec.type();

    auto chunk = _build_test_chunk(runtime_state.get());
    ChunkPB chunk_pb;
    ASSERT_OK(channel._serialize_chunk(chunk.get(), &chunk_pb));
    EXPECT_EQ(chunk_pb.compress_type(), CompressionTypePB::SNAPPY);
    EXPECT_EQ(chunk_pb.data(), "x");
    EXPECT_EQ(1, sink->_ts_profile->compressed_bytes_counter->value());
    // The ratio's numerator is what the compressor was fed, not SerializedBytes.
    EXPECT_EQ(static_cast<int64_t>(chunk_pb.uncompressed_size()),
              sink->_ts_profile->compressed_input_bytes_counter->value());
}

// The sink profile carried only timers: SerializeChunkTime said how long a chunk took to
// serialize but never how many bytes left the node, so a change to the wire encoding could only
// be inferred from the timers moving. Pin each counter to the payload it describes.
TEST_F(TabletSinkIndexChannelTest, serialize_chunk_counts_the_bytes_it_puts_on_the_wire) {
    TQueryOptions query_options;
    auto runtime_state = _build_runtime_state(query_options);
    auto sink = _build_prepared_sink(runtime_state.get());
    NodeChannel channel(sink.get(), 0, false);
    auto* ts_profile = sink->_ts_profile;

    auto chunk = _build_test_chunk(runtime_state.get());
    ChunkPB first;
    ASSERT_OK(channel._serialize_chunk(chunk.get(), &first));
    EXPECT_EQ(static_cast<int64_t>(chunk->bytes_usage()), ts_profile->raw_input_bytes_counter->value());
    EXPECT_EQ(static_cast<int64_t>(first.uncompressed_size()), ts_profile->serialized_bytes_counter->value());
    // No codec is configured here, so nothing was compressed and SerializedBytes is what went out.
    // This is the default: load_transmission_compression_type is NO_COMPRESSION. Both compression
    // counters stay 0, which is why the ratio is CompressedInputBytes / CompressedBytes and not
    // SerializedBytes / CompressedBytes -- the latter would divide by zero here.
    EXPECT_EQ(0, ts_profile->compressed_input_bytes_counter->value());
    EXPECT_EQ(0, ts_profile->compressed_bytes_counter->value());

    // One counter per sink, shared by every NodeChannel, so a second send must accumulate.
    ChunkPB second;
    ASSERT_OK(channel._serialize_chunk(chunk.get(), &second));
    EXPECT_EQ(static_cast<int64_t>(2 * chunk->bytes_usage()), ts_profile->raw_input_bytes_counter->value());
    EXPECT_EQ(static_cast<int64_t>(first.uncompressed_size() + second.uncompressed_size()),
              ts_profile->serialized_bytes_counter->value());

    // A counter nobody can find in the profile is not observability. get_counter() is a flat
    // lookup, so it only proves registration; the child map is what decides where the counter
    // shows up in the reported tree, and these belong next to SerializeChunkTime under SendRpcTime.
    auto* profile = ts_profile->runtime_profile;
    const auto& send_rpc_children = profile->_child_counter_map["SendRpcTime"];
    for (const auto& name : {"RawInputBytes", "SerializedBytes", "CompressedInputBytes", "CompressedBytes"}) {
        EXPECT_NE(nullptr, profile->get_counter(name));
        EXPECT_EQ(1, send_rpc_children.count(name)) << name << " is not a child of SendRpcTime";
    }
}
// End-to-end check of the chunk encode level negotiation: what the receiving BE advertises in
// PTabletWriterOpenResult decides whether the sender records per-column encode levels in ChunkPB.
// A BE predating the field leaves it unset, which must come out as "no encoding" -- that peer
// parses the payload at level 0 unconditionally, so an optimistic default would corrupt it.
TEST_F(TabletSinkIndexChannelTest, chunk_encode_level_follows_the_peer_advertisement) {
    struct Case {
        const char* name;
        bool advertise;
        int advertised_level;
        bool config_enabled;
        int expected_level; // -1 means: no encode_level recorded at all
    };
    const std::vector<Case> cases = {
            {"peer advertises the bit", true, serde::ENCODE_ALL_NULL, true, serde::ENCODE_ALL_NULL},
            {"peer predates the field", false, 0, true, -1},
            {"peer advertises nothing", true, 0, true, -1},
            // A peer that only understands other bits must not get ENCODE_ALL_NULL.
            {"peer advertises another bit", true, serde::ENCODE_STRING, true, -1},
            {"disabled locally", true, serde::ENCODE_ALL_NULL, false, -1},
    };

    const bool prev_enabled = config::enable_load_chunk_all_null_encoding;
    DeferOp restore_config([&] { config::enable_load_chunk_all_null_encoding = prev_enabled; });

    for (const auto& c : cases) {
        SCOPED_TRACE(c.name);
        config::enable_load_chunk_all_null_encoding = c.config_enabled;

        TQueryOptions query_options;
        query_options.__set_batch_size(4096);
        query_options.__set_query_timeout(3600);
        auto runtime_state = _build_runtime_state(query_options);
        auto sink = _build_prepared_sink(runtime_state.get());

        std::vector<int> observed_levels;
        bool chunk_seen = false;

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
            if (c.advertise) {
                closure->result.set_supported_chunk_encode_level(c.advertised_level);
            }
            closure->Run();
        });
        SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::open_join", [](void* arg) {});
        SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::add_chunk_send", [&](void* arg) {
            RpcAddChunkTuple* rpc_tuple = (RpcAddChunkTuple*)arg;
            PTabletWriterAddChunksRequest* request = std::get<1>(*rpc_tuple);
            for (int i = 0; i < request->requests_size(); i++) {
                const auto& chunk_pb = request->requests(i).chunk();
                if (chunk_pb.data().empty()) {
                    continue;
                }
                chunk_seen = true;
                for (int j = 0; j < chunk_pb.encode_level_size(); j++) {
                    observed_levels.emplace_back(chunk_pb.encode_level(j));
                }
            }
            ReusableClosure<PTabletWriterAddBatchResult>* closure = std::get<2>(*rpc_tuple);
            closure->result.mutable_status()->set_status_code(TStatusCode::OK);
            closure->Run();
        });
        SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::add_chunk_join", [](void* arg) {
            auto* rpc_pair = (std::pair<ReusableClosure<PTabletWriterAddBatchResult>*, bool*>*)arg;
            *rpc_pair->second = true;
        });

        ASSERT_OK(sink->open(runtime_state.get()));
        auto chunk = _build_all_null_test_chunk(runtime_state.get());
        ASSERT_OK(sink->send_chunk(runtime_state.get(), chunk.get()));
        // close() flushes the buffered chunk, which is what serializes it.
        (void)sink->close(runtime_state.get(), Status::OK());

        ASSERT_TRUE(chunk_seen) << "no serialized chunk reached the rpc";
        if (c.expected_level < 0) {
            // Byte-for-byte the legacy payload: no level recorded, so the receiver parses at 0.
            EXPECT_TRUE(observed_levels.empty());
        } else {
            ASSERT_FALSE(observed_levels.empty());
            for (int level : observed_levels) {
                EXPECT_EQ(c.expected_level, level);
            }
        }
    }
}

// The no-regression guarantee, proven by construction rather than by a cluster stopwatch: a chunk
// with NO all-NULL column must go out byte-for-byte as it did before this change -- no tag byte on
// any nullable column, and no encode_level in ChunkPB at all -- even when the peer has advertised
// the capability. Cluster wall clock cannot resolve the few-percent cost this avoids (its
// cold-start noise floor is ~25%), so the guarantee is asserted on the wire format instead.
TEST_F(TabletSinkIndexChannelTest, no_all_null_column_means_the_payload_is_untouched) {
    const bool prev_enabled = config::enable_load_chunk_all_null_encoding;
    DeferOp restore_config([&] { config::enable_load_chunk_all_null_encoding = prev_enabled; });
    config::enable_load_chunk_all_null_encoding = true;

    std::string payload_all_null, payload_dense;
    int levels_all_null = -1, levels_dense = -1;

    // Same peer, same negotiation, two chunk shapes: one with an all-NULL column, one without.
    for (int with_all_null = 1; with_all_null >= 0; with_all_null--) {
        TQueryOptions query_options;
        query_options.__set_batch_size(4096);
        query_options.__set_query_timeout(3600);
        auto runtime_state = _build_runtime_state(query_options);
        auto sink = _build_prepared_sink(runtime_state.get());

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
            auto* closure = rpc_pair->second;
            closure->result.mutable_status()->set_status_code(TStatusCode::OK);
            closure->result.set_supported_chunk_encode_level(serde::ENCODE_ALL_NULL);
            closure->Run();
        });
        SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::open_join", [](void* arg) {});
        SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::add_chunk_send", [&](void* arg) {
            RpcAddChunkTuple* rpc_tuple = (RpcAddChunkTuple*)arg;
            auto* request = std::get<1>(*rpc_tuple);
            for (int i = 0; i < request->requests_size(); i++) {
                const auto& chunk_pb = request->requests(i).chunk();
                if (chunk_pb.data().empty()) continue;
                if (with_all_null) {
                    payload_all_null = chunk_pb.data().substr(0, chunk_pb.serialized_size());
                    levels_all_null = chunk_pb.encode_level_size();
                } else {
                    payload_dense = chunk_pb.data().substr(0, chunk_pb.serialized_size());
                    levels_dense = chunk_pb.encode_level_size();
                }
            }
            auto* closure = std::get<2>(*rpc_tuple);
            closure->result.mutable_status()->set_status_code(TStatusCode::OK);
            closure->Run();
        });
        SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::add_chunk_join", [](void* arg) {
            auto* rpc_pair = (std::pair<ReusableClosure<PTabletWriterAddBatchResult>*, bool*>*)arg;
            *rpc_pair->second = true;
        });

        ASSERT_OK(sink->open(runtime_state.get()));
        auto chunk = with_all_null ? _build_all_null_test_chunk(runtime_state.get())
                                   : _build_test_chunk(runtime_state.get());
        ASSERT_OK(sink->send_chunk(runtime_state.get(), chunk.get()));
        (void)sink->close(runtime_state.get(), Status::OK());
    }

    // With an all-NULL column the bit is used: per-column levels are recorded.
    EXPECT_GT(levels_all_null, 0);
    // Without one it is not used at all, so the receiver parses at level 0 exactly as before.
    EXPECT_EQ(0, levels_dense);
    ASSERT_FALSE(payload_dense.empty());
    // And the dense payload carries no tag byte, so it is shorter than the encoded shape would be.
    EXPECT_NE(payload_all_null, payload_dense);
}

// The per-chunk gate flips state that OUTLIVES the chunk: _encode_context is built for a chunk
// with an all-NULL column and reset for one without. Driving several chunks through ONE channel
// is what a fresh-channel-per-shape test cannot reach, and it is where a stale context or a
// leftover encode_level would surface -- the review's concern about repeated chunks on a single
// high-frequency channel.
TEST_F(TabletSinkIndexChannelTest, alternating_chunk_shapes_on_one_channel_keep_their_own_encoding) {
    const bool prev_enabled = config::enable_load_chunk_all_null_encoding;
    DeferOp restore_config([&] { config::enable_load_chunk_all_null_encoding = prev_enabled; });
    config::enable_load_chunk_all_null_encoding = true;

    TQueryOptions query_options;
    auto runtime_state = _build_runtime_state(query_options);
    auto sink = _build_prepared_sink(runtime_state.get());
    NodeChannel channel(sink.get(), 0, false);
    // Stand in for a peer that advertised the bit during open().
    channel._chunk_encode_level = serde::ENCODE_ALL_NULL;

    auto all_null_chunk = _build_all_null_test_chunk(runtime_state.get());
    auto dense_chunk = _build_test_chunk(runtime_state.get());

    // A dense chunk serialized on a channel that has never encoded anything: the baseline the
    // legacy path produces. Everything below is compared against it.
    ChunkPB baseline;
    NodeChannel fresh(sink.get(), 0, false);
    ASSERT_OK(fresh._serialize_chunk(dense_chunk.get(), &baseline));
    ASSERT_EQ(0, baseline.encode_level_size());

    // all-NULL, dense, all-NULL again, all on the SAME channel.
    for (int round = 0; round < 2; round++) {
        ChunkPB encoded;
        ASSERT_OK(channel._serialize_chunk(all_null_chunk.get(), &encoded));
        EXPECT_EQ(2, encoded.encode_level_size()) << "round " << round << ": all-NULL chunk lost its levels";
        for (int i = 0; i < encoded.encode_level_size(); i++) {
            EXPECT_EQ(serde::ENCODE_ALL_NULL, encoded.encode_level(i)) << "round " << round;
        }

        ChunkPB dense;
        ASSERT_OK(channel._serialize_chunk(dense_chunk.get(), &dense));
        // No level may survive from the chunk before it...
        EXPECT_EQ(0, dense.encode_level_size()) << "round " << round << ": dense chunk inherited encode levels";
        // ...and the bytes must match what a channel that never encoded anything produces.
        EXPECT_EQ(baseline.data(), dense.data()) << "round " << round << ": dense payload is not the legacy one";
    }
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
    ChunkUniquePtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 1);
    chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(1));
    ASSERT_OK(sink->send_chunk(runtime_state.get(), chunk.get()));
    Status status = sink->close(runtime_state.get(), Status::OK());
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.message().find("[R1][E112]Not connected to [10.128.8.0:8060]") != std::string::npos);
}

// Verify that _send_request() releases protobuf memory via Swap before returning.
// This prevents cross-tracker memory accounting mismatch where protobuf memory allocated
// under process_mem_tracker (via SCOPED(nullptr)) would be freed under instance_mem_tracker.
TEST_F(TabletSinkIndexChannelTest, send_request_releases_protobuf_memory) {
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

    size_t request_size_before_swap = 0;
    size_t request_size_after_swap = 0;

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::open_send");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::open_join");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::add_chunk_send");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::rpc::add_chunk_join");
        SyncPoint::GetInstance()->ClearCallBack("NodeChannel::_send_request::after_swap");
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
        PTabletWriterAddChunksRequest* request = std::get<1>(*rpc_tuple);
        // Capture request size before Swap — should contain serialized chunk data
        request_size_before_swap = request->ByteSizeLong();
        ReusableClosure<PTabletWriterAddBatchResult>* closure = std::get<2>(*rpc_tuple);
        closure->result.mutable_status()->set_status_code(TStatusCode::OK);
        closure->Run();
    });
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::add_chunk_join", [&](void* arg) {
        std::pair<ReusableClosure<PTabletWriterAddBatchResult>*, bool*>* rpc_pair =
                (std::pair<ReusableClosure<PTabletWriterAddBatchResult>*, bool*>*)arg;
        *rpc_pair->second = true;
    });
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::_send_request::after_swap", [&](void* arg) {
        PTabletWriterAddChunksRequest* request = (PTabletWriterAddChunksRequest*)arg;
        // After Swap, request should be empty — protobuf memory released
        request_size_after_swap = request->ByteSizeLong();
    });

    ASSERT_OK(sink->open(runtime_state.get()));
    auto tuple_desc = runtime_state->desc_tbl().get_tuple_descriptor(_desc_tbl.tupleDescriptors[0].id);
    ChunkUniquePtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 1);
    chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(1));
    chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(int64_t(1)));
    ASSERT_OK(sink->send_chunk(runtime_state.get(), chunk.get()));
    // close() flushes the buffered chunk and triggers _send_request with eos=true,
    // which serializes the chunk data and dispatches the RPC.
    (void)sink->close(runtime_state.get(), Status::OK());

    // Verify: before Swap the request had serialized chunk data, after Swap it's cleared
    EXPECT_GT(request_size_before_swap, 0);
    EXPECT_EQ(request_size_after_swap, 0);
}

// Reproduces the race condition fixed in _send_chunk_by_node where it directly
// iterated IndexChannel::_node_channels without holding _node_channels_mutex,
// concurrent with IndexChannel::init (incremental open) modifying the map.
//
// Uses SyncPoint to force the following interleaving:
//   Thread A (_send_chunk_by_node): acquires shared lock, begins iterating _node_channels
//   Thread B (IndexChannel::init):  tries to acquire exclusive lock — must block until A finishes
//
// Under TSAN, this would detect a data race if the shared lock were missing.
TEST_F(TabletSinkIndexChannelTest, ConcurrentSendAndIncrementalInit) {
    TQueryOptions query_options;
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
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->ClearTrace();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    // Mock RPC open: immediately succeed
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::open_send", [&](void* arg) {
        RpcOpenPair* rpc_pair = (RpcOpenPair*)arg;
        RefCountClosure<PTabletWriterOpenResult>* closure = rpc_pair->second;
        closure->result.mutable_status()->set_status_code(TStatusCode::OK);
        closure->Run();
    });
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::open_join", [&](void* arg) {});

    // Mock RPC add_chunk: immediately succeed
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::add_chunk_send", [&](void* arg) {
        RpcAddChunkTuple* rpc_tuple = (RpcAddChunkTuple*)arg;
        ReusableClosure<PTabletWriterAddBatchResult>* closure = std::get<2>(*rpc_tuple);
        closure->result.mutable_status()->set_status_code(TStatusCode::OK);
        closure->Run();
    });
    SyncPoint::GetInstance()->SetCallBack("NodeChannel::rpc::add_chunk_join", [&](void* arg) {
        auto* rpc_pair = (std::pair<ReusableClosure<PTabletWriterAddBatchResult>*, bool*>*)arg;
        *rpc_pair->second = true;
    });

    ASSERT_OK(sink->open(runtime_state.get()));

    // Add a new tablet location so incremental init() can find it.
    // Tablet 100 on node 0 (an existing node, so init will reuse the NodeChannel).
    std::vector<TTabletLocation> new_locations;
    TTabletLocation new_loc;
    new_loc.tablet_id = 100;
    new_loc.node_ids.push_back(0);
    new_locations.push_back(new_loc);
    sink->_location->add_locations(new_locations);

    auto* index_channel = sink->_channels[0].get();

    // Force deterministic interleaving:
    //   _send_chunk_by_node acquires shared lock → init::before_lock can proceed
    //   But init blocks on exclusive lock until the send finishes.
    SyncPoint::GetInstance()->LoadDependency({
            {"TabletSinkSender::_send_chunk_by_node::after_lock", "IndexChannel::init::before_lock"},
    });

    std::atomic<bool> send_done{false};
    std::atomic<bool> init_acquired_lock{false};

    SyncPoint::GetInstance()->SetCallBack("IndexChannel::init::after_lock", [&](void*) {
        init_acquired_lock.store(true);
        // With the fix: shared lock is held during send, so exclusive lock
        // can only be acquired after send releases it.
        EXPECT_TRUE(send_done.load());
    });

    // Thread A: send a chunk (which calls _send_chunk_by_node, holding shared lock)
    auto tuple_desc = runtime_state->desc_tbl().get_tuple_descriptor(_desc_tbl.tupleDescriptors[0].id);
    ChunkUniquePtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 1);
    chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(1));
    chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(int64_t(1)));

    std::thread sender([&]() {
        auto st = sink->send_chunk(runtime_state.get(), chunk.get());
        send_done.store(true);
        // send_chunk may fail because the mock doesn't fully simulate the BE,
        // but that's OK — we only care that the shared lock was held during iteration.
    });

    // Thread B: incremental init (modifies _node_channels under exclusive lock)
    PTabletWithPartition new_tablet;
    new_tablet.set_tablet_id(100);
    new_tablet.set_partition_id(1);
    auto* replica = new_tablet.add_replicas();
    replica->set_host("10.128.8.0");
    replica->set_port(8060);
    replica->set_node_id(0);

    std::thread initializer([&]() {
        // This will try to acquire exclusive _node_channels_mutex.
        // With the fix, it blocks until sender releases the shared lock.
        auto st = index_channel->init(runtime_state.get(), {new_tablet}, true);
        // init may fail because NodeChannel::init requires more setup,
        // but we only care that the lock ordering is correct.
    });

    sender.join();
    initializer.join();

    // Verify the lock ordering was respected: init got the exclusive lock
    // only after send released the shared lock.
    EXPECT_TRUE(init_acquired_lock.load());

    (void)sink->close(runtime_state.get(), Status::OK());
}

static AddChunksRequestBuilder make_two_index_builder(int64_t txn_id, bool flexible_partial_update) {
    AddChunksChannelSpec spec;
    spec.flexible_partial_update = flexible_partial_update;
    for (int64_t index_id : {1, 2}) {
        PerIndexSpec index;
        index.index_id = index_id;
        index.txn_id = txn_id;
        spec.indexes.push_back(index);
    }
    AddChunksRequestBuilder builder;
    builder.init(std::move(spec));
    return builder;
}

// A flexible partial update ships every entry of the load's column-set dictionary once, with the first
// request built after the entry was interned, to every index of the node.
TEST(AddChunksRequestBuilderTest, flexible_partial_update_ships_new_dictionary_entries) {
    constexpr int64_t kTxnId = 730101;
    auto dict = FlexiblePartialUpdateRegistry::instance()->retain(kTxnId);
    DeferOp release([&]() { FlexiblePartialUpdateRegistry::instance()->release(kTxnId); });
    auto builder = make_two_index_builder(kTxnId, true);
    const std::vector<std::vector<int64_t>> tablet_ids(2);
    AddChunksSendOptions opts;

    ASSERT_EQ(0, dict->intern({"k", "v1"}));
    ASSERT_EQ(1, dict->intern({"v2", "k"}));
    auto first = builder.build(tablet_ids, opts);
    ASSERT_EQ(2, first.requests_size());
    for (const auto& request : first.requests()) {
        ASSERT_TRUE(request.has_column_set_dict());
        EXPECT_EQ(0, request.column_set_dict().first_set_id());
        ASSERT_EQ(2, request.column_set_dict().sets_size());
        ASSERT_EQ(2, request.column_set_dict().sets(1).column_names_size());
        EXPECT_EQ("k", request.column_set_dict().sets(1).column_names(0));
        EXPECT_EQ("v2", request.column_set_dict().sets(1).column_names(1));
    }

    // No new entry, nothing to ship.
    auto second = builder.build(tablet_ids, opts);
    EXPECT_FALSE(second.requests(0).has_column_set_dict());
    EXPECT_FALSE(second.requests(1).has_column_set_dict());

    ASSERT_EQ(2, dict->intern({"k"}));
    opts.eos = true;
    auto last = builder.build(tablet_ids, opts);
    for (const auto& request : last.requests()) {
        ASSERT_TRUE(request.has_column_set_dict());
        EXPECT_EQ(2, request.column_set_dict().first_set_id());
        ASSERT_EQ(1, request.column_set_dict().sets_size());
        EXPECT_EQ("k", request.column_set_dict().sets(0).column_names(0));
    }
}

// Any other load never carries the dictionary, even when one exists for its txn.
TEST(AddChunksRequestBuilderTest, other_loads_ship_no_dictionary) {
    constexpr int64_t kTxnId = 730102;
    auto dict = FlexiblePartialUpdateRegistry::instance()->retain(kTxnId);
    DeferOp release([&]() { FlexiblePartialUpdateRegistry::instance()->release(kTxnId); });
    ASSERT_EQ(0, dict->intern({"k", "v1"}));
    auto builder = make_two_index_builder(kTxnId, false);
    const std::vector<std::vector<int64_t>> tablet_ids(2);
    AddChunksSendOptions opts;
    opts.eos = true;
    auto request = builder.build(tablet_ids, opts);
    EXPECT_FALSE(request.requests(0).has_column_set_dict());
    EXPECT_FALSE(request.requests(1).has_column_set_dict());
}

} // namespace starrocks
