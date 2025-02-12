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

#include "exec/tablet_info.h"
#include "exec/tablet_sink.h"
#include "runtime/descriptor_helper.h"
#include "storage/chunk_helper.h"
#include "testutil/assert.h"
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
        partition.partitions[0].num_buckets = 1;
        partition.partitions[0].indexes.resize(1);
        partition.partitions[0].indexes[0].index_id = 0;
        partition.partitions[0].indexes[0].tablets.push_back(0);

        TOlapTableLocationParam& location = table_sink.location;
        location.db_id = _db_id;
        location.table_id = _table_id;
        location.version = 0;
        location.tablets.resize(1);
        location.tablets[0].tablet_id = 0;
        location.tablets[0].node_ids.push_back(0);

        TNodesInfo& nodes_info = table_sink.nodes_info;
        nodes_info.version = 0;
        nodes_info.nodes.resize(1);
        nodes_info.nodes[0].id = 0;
        nodes_info.nodes[0].option = 0;
        nodes_info.nodes[0].host = "10.128.8.78";
        nodes_info.nodes[0].async_internal_port = 8060;

        TDataSink data_sink;
        data_sink.__set_olap_table_sink(table_sink);
        return data_sink;
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

} // namespace starrocks