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

#include "exec/connector_scan_node.h"

#include <gtest/gtest.h>

#include "column/datum_tuple.h"
#include "exec/pipeline/scan/morsel.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "testutil/assert.h"

namespace starrocks {

class ConnectorScanNodeTest : public ::testing::Test {
public:
    void SetUp() override {
        config::enable_system_metrics = false;
        config::enable_metric_calculator = false;

        _mem_tracker = std::make_shared<MemTracker>(-1, "connector scan");
        _exec_env = ExecEnv::GetInstance();
    }
    void TearDown() override {}

protected:
    std::shared_ptr<RuntimeState> create_runtime_state();
    std::shared_ptr<RuntimeState> create_runtime_state(const TQueryOptions& query_options);

    DescriptorTbl* create_table_desc(RuntimeState* runtime_state, const std::vector<TypeDescriptor>& types);

    std::shared_ptr<TPlanNode> create_tplan_node_cloud();
    std::vector<TScanRangeParams> create_scan_ranges_cloud(size_t num);

    std::shared_ptr<TPlanNode> create_tplan_node_hive();
    std::vector<TScanRangeParams> create_scan_ranges_hive(size_t num);

    std::shared_ptr<TPlanNode> create_tplan_node_stream_load();
    std::vector<TScanRangeParams> create_scan_ranges_stream_load(RuntimeState* runtime_state,
                                                                 const std::vector<TypeDescriptor>& types,
                                                                 const UniqueId& load_id);

    std::shared_ptr<MemTracker> _mem_tracker = nullptr;
    ExecEnv* _exec_env = nullptr;
};

std::shared_ptr<RuntimeState> ConnectorScanNodeTest::create_runtime_state() {
    TQueryOptions query_options;
    return create_runtime_state(query_options);
}

std::shared_ptr<RuntimeState> ConnectorScanNodeTest::create_runtime_state(const TQueryOptions& query_options) {
    TUniqueId fragment_id;
    TQueryGlobals query_globals;
    std::shared_ptr<RuntimeState> runtime_state =
            std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
    TUniqueId id;
    runtime_state->init_mem_trackers(id);
    return runtime_state;
}

DescriptorTbl* ConnectorScanNodeTest::create_table_desc(RuntimeState* runtime_state,
                                                        const std::vector<TypeDescriptor>& types) {
    /// Init DescriptorTable
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    for (auto& t : types) {
        TSlotDescriptorBuilder slot_desc_builder;
        slot_desc_builder.type(t).length(t.len).precision(t.precision).scale(t.scale).nullable(true);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
    }
    tuple_desc_builder.build(&desc_tbl_builder);

    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(runtime_state, runtime_state->obj_pool(), desc_tbl_builder.desc_tbl(), &tbl,
                                config::vector_chunk_size)
                  .ok());

    runtime_state->set_desc_tbl(tbl);
    return tbl;
}

std::shared_ptr<TPlanNode> ConnectorScanNodeTest::create_tplan_node_cloud() {
    std::vector<::starrocks::TTupleId> tuple_ids{0};
    std::vector<bool> nullable_tuples{true};

    auto tnode = std::make_shared<TPlanNode>();
    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::LAKE_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
    tnode->__set_nullable_tuples(nullable_tuples);
    tnode->__set_limit(-1);

    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::LAKE;
    tnode->__set_connector_scan_node(connector_scan_node);

    return tnode;
}

std::vector<TScanRangeParams> ConnectorScanNodeTest::create_scan_ranges_cloud(size_t num) {
    std::vector<TScanRangeParams> scan_ranges;

    for (int i = 0; i < num; i++) {
        TInternalScanRange internal_scan_range;
        internal_scan_range.__set_tablet_id(i);
        internal_scan_range.__set_version("1");

        TScanRange scan_range;
        scan_range.__set_internal_scan_range(internal_scan_range);

        TScanRangeParams param;
        param.__set_scan_range(scan_range);
        scan_ranges.push_back(param);
    }

    return scan_ranges;
}

TEST_F(ConnectorScanNodeTest, test_convert_scan_range_to_morsel_queue_factory_cloud) {
    std::shared_ptr<RuntimeState> runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);
    auto tnode = create_tplan_node_cloud();
    auto scan_node = std::make_shared<starrocks::ConnectorScanNode>(runtime_state->obj_pool(), *tnode, *descs);
    ASSERT_OK(scan_node->init(*tnode, runtime_state.get()));

    bool enable_tablet_internal_parallel = false;
    auto tablet_internal_parallel_mode = TTabletInternalParallelMode::type::AUTO;
    std::map<int32_t, std::vector<TScanRangeParams>> no_scan_ranges_per_driver_seq;

    // dop is 1 and not so much morsels
    int pipeline_dop = 1;
    auto scan_ranges = create_scan_ranges_cloud(1);
    ASSIGN_OR_ABORT(auto morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());

    // dop is 2 and not so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_cloud(pipeline_dop * scan_node->io_tasks_per_scan_operator());
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_FALSE(morsel_queue_factory->is_shared());

    // dop is 2 and so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_cloud(pipeline_dop * scan_node->io_tasks_per_scan_operator() + 1);
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());
}

std::shared_ptr<TPlanNode> ConnectorScanNodeTest::create_tplan_node_hive() {
    std::vector<::starrocks::TTupleId> tuple_ids{0};
    std::vector<bool> nullable_tuples{true};

    auto tnode = std::make_shared<TPlanNode>();
    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::HDFS_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
    tnode->__set_nullable_tuples(nullable_tuples);
    tnode->__set_limit(-1);

    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::HIVE;
    tnode->__set_connector_scan_node(connector_scan_node);

    return tnode;
}

std::vector<TScanRangeParams> ConnectorScanNodeTest::create_scan_ranges_hive(size_t num) {
    std::vector<TScanRangeParams> scan_ranges;

    for (int i = 0; i < num; i++) {
        THdfsScanRange hdfs_scan_range;
        hdfs_scan_range.__set_full_path("file");
        hdfs_scan_range.__set_offset(i);
        hdfs_scan_range.__set_length(1);

        TScanRange scan_range;
        scan_range.__set_hdfs_scan_range(hdfs_scan_range);

        TScanRangeParams param;
        param.__set_scan_range(scan_range);
        scan_ranges.push_back(param);
    }

    return scan_ranges;
}

TEST_F(ConnectorScanNodeTest, test_convert_scan_range_to_morsel_queue_factory_hive) {
    std::shared_ptr<RuntimeState> runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);
    auto tnode = create_tplan_node_hive();
    auto scan_node = std::make_shared<starrocks::ConnectorScanNode>(runtime_state->obj_pool(), *tnode, *descs);
    ASSERT_OK(scan_node->init(*tnode, runtime_state.get()));

    bool enable_tablet_internal_parallel = false;
    auto tablet_internal_parallel_mode = TTabletInternalParallelMode::type::AUTO;
    std::map<int32_t, std::vector<TScanRangeParams>> no_scan_ranges_per_driver_seq;

    // dop is 1 and not so much morsels
    int pipeline_dop = 1;
    auto scan_ranges = create_scan_ranges_hive(1);
    ASSIGN_OR_ABORT(auto morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());

    // dop is 2 and not so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_hive(pipeline_dop * scan_node->io_tasks_per_scan_operator());
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());

    // dop is 2 and so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_hive(pipeline_dop * scan_node->io_tasks_per_scan_operator() + 1);
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());
}

std::shared_ptr<TPlanNode> ConnectorScanNodeTest::create_tplan_node_stream_load() {
    std::vector<::starrocks::TTupleId> tuple_ids{0};
    std::vector<bool> nullable_tuples{true};

    auto tnode = std::make_shared<TPlanNode>();
    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::FILE_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
    tnode->__set_nullable_tuples(nullable_tuples);
    tnode->__set_limit(-1);

    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::FILE;
    tnode->__set_connector_scan_node(connector_scan_node);

    return tnode;
}

std::vector<TScanRangeParams> ConnectorScanNodeTest::create_scan_ranges_stream_load(
        RuntimeState* runtime_state, const std::vector<TypeDescriptor>& types, const UniqueId& load_id) {
    ObjectPool* object_pool = runtime_state->obj_pool();
    TBrokerScanRangeParams* params = object_pool->add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->dest_tuple_id = 0;
    params->src_tuple_id = 0;
    for (int i = 0; i < types.size(); i++) {
        params->expr_of_dest_slot[i] = TExpr();
        params->expr_of_dest_slot[i].nodes.emplace_back(TExprNode());
        params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
        params->expr_of_dest_slot[i].nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
        params->expr_of_dest_slot[i].nodes[0].__set_is_nullable(true);
        params->expr_of_dest_slot[i].nodes[0].__set_slot_ref(TSlotRef());
        params->expr_of_dest_slot[i].nodes[0].slot_ref.__set_slot_id(i);
    }
    for (int i = 0; i < types.size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    std::vector<TBrokerRangeDesc>* ranges = object_pool->add(new vector<TBrokerRangeDesc>());
    TBrokerRangeDesc* range = object_pool->add(new TBrokerRangeDesc());
    range->__set_load_id(load_id.to_thrift());
    range->__set_file_type(TFileType::FILE_STREAM);
    range->__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    params->__set_multi_row_delimiter("\n");
    params->__set_multi_column_separator("\t");
    range->__set_path("Invalid Path");
    range->__set_start_offset(0);
    range->__set_size(-1);
    range->__set_num_of_columns_from_file(types.size());
    ranges->push_back(*range);

    TBrokerScanRange* broker_scan_range = object_pool->add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = *ranges;

    TScanRange scan_range;
    scan_range.__set_broker_scan_range(*broker_scan_range);

    TScanRangeParams param;
    param.__set_scan_range(scan_range);

    return std::vector<TScanRangeParams>{param};
}

TEST_F(ConnectorScanNodeTest, test_stream_load_thread_pool) {
    // 1. create StreamLoadPipe
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { _exec_env->load_stream_mgr()->remove(load_id); });
    ASSERT_OK(_exec_env->load_stream_mgr()->put(load_id, pipe));

    // 2. create ConnectorScanNode
    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    query_options.load_job_type = TLoadJobType::STREAM_LOAD;
    std::shared_ptr<RuntimeState> runtime_state = create_runtime_state(query_options);
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_VARCHAR);
    auto* descs = create_table_desc(runtime_state.get(), types);
    auto tnode = create_tplan_node_stream_load();
    auto scan_node = std::make_shared<starrocks::ConnectorScanNode>(runtime_state->obj_pool(), *tnode, *descs);
    DeferOp close_scan([&]() { scan_node->close(runtime_state.get()); });
    std::vector<TScanRangeParams> scan_ranges = create_scan_ranges_stream_load(runtime_state.get(), types, load_id);
    ASSERT_OK(scan_node->set_scan_ranges(scan_ranges));
    ASSERT_OK(scan_node->init(*tnode, runtime_state.get()));
    ASSERT_OK(scan_node->prepare(runtime_state.get()));
    ASSERT_OK(scan_node->open(runtime_state.get()));

    // 3. run the stream load
    std::string csv_data = "1\ttest";
    ASSERT_OK(pipe->append(csv_data.c_str(), csv_data.size()));
    ASSERT_OK(pipe->finish());

    bool done = false;
    auto chunk = std::make_shared<Chunk>();
    ASSERT_OK(scan_node->get_next(runtime_state.get(), &chunk, &done));
    ASSERT_EQ(1, chunk->num_rows());
    DatumTuple tuple = chunk->get(0);
    ASSERT_EQ(1, tuple.get(0).get_int32());
    ASSERT_EQ("test", tuple.get(1).get_slice());
    ASSERT_TRUE(scan_node->use_stream_load_thread_pool());
}

} // namespace starrocks
