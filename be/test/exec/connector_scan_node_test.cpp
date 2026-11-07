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

#include <chrono>
#include <condition_variable>
#include <thread>

#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "connector/connector.h"
#include "connector/lake_connector.h"
#include "exec/exec_node.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/scan/connector_scan_operator.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "storage/rowset/rowid_range_option.h"
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

    auto tnode = std::make_shared<TPlanNode>();
    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::LAKE_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
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
        scan_ranges.emplace_back(param);
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
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());

    // dop is 2 and not so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_cloud(pipeline_dop * scan_node->io_tasks_per_scan_operator());
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode, false));
    ASSERT_FALSE(morsel_queue_factory->is_shared());

    // dop is 2 and not so much morsels but enable shared scan
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode, true));
    ASSERT_TRUE(morsel_queue_factory->is_shared());

    // dop is 2 and so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_cloud(pipeline_dop * scan_node->io_tasks_per_scan_operator() + 1);
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());
}

std::shared_ptr<TPlanNode> ConnectorScanNodeTest::create_tplan_node_hive() {
    std::vector<::starrocks::TTupleId> tuple_ids{0};

    auto tnode = std::make_shared<TPlanNode>();
    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::HDFS_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
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
        scan_ranges.emplace_back(param);
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
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());

    // dop is 2 and not so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_hive(pipeline_dop * scan_node->io_tasks_per_scan_operator());
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());

    // dop is 2 and so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_hive(pipeline_dop * scan_node->io_tasks_per_scan_operator() + 1);
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());
}

std::shared_ptr<TPlanNode> ConnectorScanNodeTest::create_tplan_node_stream_load() {
    std::vector<::starrocks::TTupleId> tuple_ids{0};

    auto tnode = std::make_shared<TPlanNode>();
    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::FILE_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
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
        params->expr_of_dest_slot[i].nodes.emplace_back();
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

// When FE sets catalog_type explicitly, use it.
TEST_F(ConnectorScanNodeTest, test_catalog_type_with_explicit_value) {
    std::shared_ptr<RuntimeState> runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);

    auto tnode = std::make_shared<TPlanNode>();
    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::HDFS_SCAN_NODE);
    std::vector<::starrocks::TTupleId> tuple_ids{0};
    tnode->__set_row_tuples(tuple_ids);
    tnode->__set_limit(-1);
    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::HIVE;
    connector_scan_node.__set_catalog_type("iceberg");
    tnode->__set_connector_scan_node(connector_scan_node);

    auto scan_node = std::make_shared<starrocks::ConnectorScanNode>(runtime_state->obj_pool(), *tnode, *descs);
    ASSERT_EQ("iceberg", scan_node->catalog_type());
}

// When FE does not set catalog_type (rolling upgrade), catalog_type should be empty
// so that BE skips catalog metrics rather than reporting wrong values.
TEST_F(ConnectorScanNodeTest, test_catalog_type_empty_without_fe_value) {
    std::shared_ptr<RuntimeState> runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);

    auto tnode = create_tplan_node_hive();
    auto scan_node = std::make_shared<starrocks::ConnectorScanNode>(runtime_state->obj_pool(), *tnode, *descs);
    ASSERT_TRUE(scan_node->catalog_type().empty());
}

// ExecFactory tests: verify make_connector_scan_node correctly copies catalog_type
// and each scan node case routes to the right connector.

// HDFS_SCAN_NODE with pre-set catalog_type: covers make_connector_scan_node copy path
// (exec_factory.cpp lines 163, 167) and the HDFS case (line 275).
TEST_F(ConnectorScanNodeTest, test_exec_factory_hdfs_preserves_catalog_type) {
    auto runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);

    TPlanNode tnode;
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::HDFS_SCAN_NODE);
    std::vector<TTupleId> tuple_ids{0};
    tnode.__set_row_tuples(tuple_ids);
    tnode.__set_limit(-1);
    TConnectorScanNode csn;
    csn.connector_name = connector::Connector::HIVE;
    csn.__set_catalog_type("hive");
    tnode.__set_connector_scan_node(csn);

    TPlan plan;
    plan.nodes.push_back(tnode);

    ExecNode* root = nullptr;
    ASSERT_OK(ExecNode::create_tree(runtime_state.get(), runtime_state->obj_pool(), plan, *descs, &root));
    ASSERT_NE(nullptr, root);
    auto* scan = static_cast<ConnectorScanNode*>(root);
    ASSERT_EQ("hive", scan->catalog_type());
}

// LAKE_SCAN_NODE without catalog_type: BE should default to "default"
// (exec_factory.cpp lines 311-314).
TEST_F(ConnectorScanNodeTest, test_exec_factory_lake_defaults_to_default_catalog_type) {
    auto runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);

    TPlanNode tnode;
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::LAKE_SCAN_NODE);
    std::vector<TTupleId> tuple_ids{0};
    tnode.__set_row_tuples(tuple_ids);
    tnode.__set_limit(-1);
    // No connector_scan_node set — BE must default catalog_type to "default".

    TPlan plan;
    plan.nodes.push_back(tnode);

    ExecNode* root = nullptr;
    ASSERT_OK(ExecNode::create_tree(runtime_state.get(), runtime_state->obj_pool(), plan, *descs, &root));
    ASSERT_NE(nullptr, root);
    auto* scan = static_cast<ConnectorScanNode*>(root);
    ASSERT_EQ("default", scan->catalog_type());
}

// LAKE_SCAN_NODE with pre-set catalog_type from FE: BE must not overwrite it.
// (exec_factory.cpp line 311, 312 branch not taken since catalog_type already set).
TEST_F(ConnectorScanNodeTest, test_exec_factory_lake_preserves_catalog_type_when_fe_sets_it) {
    auto runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);

    TPlanNode tnode;
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::LAKE_SCAN_NODE);
    std::vector<TTupleId> tuple_ids{0};
    tnode.__set_row_tuples(tuple_ids);
    tnode.__set_limit(-1);
    TConnectorScanNode csn;
    csn.connector_name = connector::Connector::LAKE;
    csn.__set_catalog_type("default");
    tnode.__set_connector_scan_node(csn);

    TPlan plan;
    plan.nodes.push_back(tnode);

    ExecNode* root = nullptr;
    ASSERT_OK(ExecNode::create_tree(runtime_state.get(), runtime_state->obj_pool(), plan, *descs, &root));
    ASSERT_NE(nullptr, root);
    auto* scan = static_cast<ConnectorScanNode*>(root);
    ASSERT_EQ("default", scan->catalog_type());
}

// JDBC_SCAN_NODE: covers exec_factory.cpp line 305.
TEST_F(ConnectorScanNodeTest, test_exec_factory_jdbc_scan_node) {
    auto runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);

    TPlanNode tnode;
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::JDBC_SCAN_NODE);
    std::vector<TTupleId> tuple_ids{0};
    tnode.__set_row_tuples(tuple_ids);
    tnode.__set_limit(-1);
    TConnectorScanNode csn;
    csn.connector_name = connector::Connector::JDBC;
    csn.__set_catalog_type("jdbc");
    tnode.__set_connector_scan_node(csn);

    TPlan plan;
    plan.nodes.push_back(tnode);

    ExecNode* root = nullptr;
    ASSERT_OK(ExecNode::create_tree(runtime_state.get(), runtime_state->obj_pool(), plan, *descs, &root));
    ASSERT_NE(nullptr, root);
    auto* scan = static_cast<ConnectorScanNode*>(root);
    ASSERT_EQ("jdbc", scan->catalog_type());
}

// ES_HTTP_SCAN_NODE: covers exec_factory.cpp line 293.
TEST_F(ConnectorScanNodeTest, test_exec_factory_es_scan_node) {
    auto runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);

    TPlanNode tnode;
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::ES_HTTP_SCAN_NODE);
    std::vector<TTupleId> tuple_ids{0};
    tnode.__set_row_tuples(tuple_ids);
    tnode.__set_limit(-1);
    TConnectorScanNode csn;
    csn.connector_name = connector::Connector::ES;
    csn.__set_catalog_type("elasticsearch");
    tnode.__set_connector_scan_node(csn);

    TPlan plan;
    plan.nodes.push_back(tnode);

    ExecNode* root = nullptr;
    ASSERT_OK(ExecNode::create_tree(runtime_state.get(), runtime_state->obj_pool(), plan, *descs, &root));
    ASSERT_NE(nullptr, root);
    auto* scan = static_cast<ConnectorScanNode*>(root);
    ASSERT_EQ("elasticsearch", scan->catalog_type());
}

// MYSQL_SCAN_NODE: covers exec_factory.cpp line 281.
TEST_F(ConnectorScanNodeTest, test_exec_factory_mysql_scan_node) {
    auto runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);

    TPlanNode tnode;
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::MYSQL_SCAN_NODE);
    std::vector<TTupleId> tuple_ids{0};
    tnode.__set_row_tuples(tuple_ids);
    tnode.__set_limit(-1);
    TConnectorScanNode csn;
    csn.connector_name = connector::Connector::MYSQL;
    csn.__set_catalog_type("mysql");
    tnode.__set_connector_scan_node(csn);

    TPlan plan;
    plan.nodes.push_back(tnode);

    ExecNode* root = nullptr;
    ASSERT_OK(ExecNode::create_tree(runtime_state.get(), runtime_state->obj_pool(), plan, *descs, &root));
    ASSERT_NE(nullptr, root);
    auto* scan = static_cast<ConnectorScanNode*>(root);
    ASSERT_EQ("mysql", scan->catalog_type());
}

namespace {
// Only leaf I/O is controlled here. Queue pickup, source retention, split publication,
// buffer routing, status propagation and EOS all use the production implementation.
struct GroupedLeafIO {
    std::mutex mutex;
    std::condition_variable cv;
    bool release_roots = false;
    bool release_children = false;
    Status root_status = Status::OK();
    Status child_status = Status::OK();
    int roots_entered = 0;
    int children_entered = 0;
    int opened = 0;
    int closed = 0;
    int child_sequence[2] = {};
    std::vector<std::pair<int, TScanRange>> inputs;
    std::vector<std::weak_ptr<RowidRangeOption>> child_ranges;

    void release_all() {
        std::lock_guard lock(mutex);
        release_roots = release_children = true;
        cv.notify_all();
    }
};

class GroupedLeafDataSource final : public connector::DataSource {
public:
    GroupedLeafDataSource(std::shared_ptr<GroupedLeafIO> io, TScanRange range)
            : _io(std::move(io)), _range(std::move(range)) {}
    std::string name() const override { return "GroupedLifecycleLeaf"; }
    Status open(RuntimeState*) override {
        std::lock_guard lock(_io->mutex);
        ++_io->opened;
        int driver = runtime_membership_filter_eval_context.driver_sequence;
        _io->inputs.emplace_back(driver, _range);
        if (_split_context != nullptr) _marker = 100 * driver + ++_io->child_sequence[driver];
        return Status::OK();
    }
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override {
        const bool child = _split_context != nullptr;
        if (_read) return Status::EndOfFile("");
        _read = true;
        std::unique_lock lock(_io->mutex);
        ++(child ? _io->children_entered : _io->roots_entered);
        _io->cv.notify_all();
        if (!_io->cv.wait_for(lock, std::chrono::seconds(10),
                              [&] { return child ? _io->release_children : _io->release_roots; })) {
            return Status::InternalError("test leaf I/O latch timed out");
        }
        if (state->is_cancelled()) return Status::Cancelled("held grouped read cancelled");
        const auto& status = child ? _io->child_status : _io->root_status;
        if (status.is_mem_limit_exceeded()) {
            // Fail a real query memory check at the controlled leaf allocation boundary.
            auto* tracker = state->query_ctx()->mem_tracker().get();
            const auto bytes = tracker->limit() + 1;
            tracker->consume(bytes);
            DeferOp release([&] { tracker->release(bytes); });
            return tracker->check_mem_limit("grouped leaf allocation");
        }
        RETURN_IF_ERROR(status);
        if (!child) return Status::EndOfFile("");
        auto column = Int64Column::create();
        column->append(_marker);
        *chunk = std::make_shared<Chunk>();
        (*chunk)->append_column(std::move(column), 0);
        _rows = 1;
        return Status::OK();
    }
    void get_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* tasks) override {
        if (_split_context != nullptr || _published) return;
        _published = true;
        std::lock_guard lock(_io->mutex);
        for (int i = 0; i < 2; ++i) {
            auto child = std::make_unique<pipeline::LakeSplitContext>();
            child->rowid_range = std::make_shared<RowidRangeOption>();
            _io->child_ranges.emplace_back(child->rowid_range);
            tasks->emplace_back(std::move(child));
        }
    }
    void close(RuntimeState*) override {
        std::lock_guard lock(_io->mutex);
        ++_io->closed;
        _io->cv.notify_all();
    }
    int64_t raw_rows_read() const override { return _rows; }
    int64_t num_rows_read() const override { return _rows; }
    int64_t num_bytes_read() const override { return _rows * sizeof(int64_t); }
    int64_t cpu_time_spent() const override { return 0; }

private:
    std::shared_ptr<GroupedLeafIO> _io;
    TScanRange _range;
    bool _read = false;
    bool _published = false;
    int64_t _marker = 0;
    int64_t _rows = 0;
};

class GroupedLeafProvider final : public connector::DataSourceProvider {
public:
    GroupedLeafProvider(std::shared_ptr<GroupedLeafIO> io, ConnectorScanNode* node, const TPlanNode& plan)
            : _io(std::move(io)), _lake(node, plan) {}
    connector::DataSourcePtr create_data_source(const TScanRange& range) override {
        return std::make_unique<GroupedLeafDataSource>(_io, range);
    }
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override {
        return state->desc_tbl().get_tuple_descriptor(0);
    }
    bool always_shared_scan() const override { return false; }
    StatusOr<pipeline::MorselQueuePtr> convert_scan_range_to_morsel_queue(const std::vector<TScanRangeParams>& ranges,
                                                                          int node, int32_t dop, bool parallel,
                                                                          TTabletInternalParallelMode::type mode,
                                                                          size_t total,
                                                                          size_t scan_parallelism) override {
        return _lake.convert_scan_range_to_morsel_queue(ranges, node, dop, parallel, mode, total, scan_parallelism);
    }

private:
    std::shared_ptr<GroupedLeafIO> _io;
    connector::LakeDataSourceProvider _lake;
};

class GroupedLeafConnector final : public connector::Connector {
public:
    explicit GroupedLeafConnector(std::shared_ptr<GroupedLeafIO> io) : _io(std::move(io)) {}
    connector::ConnectorType connector_type() const override { return connector::LAKE; }
    connector::DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* node,
                                                                 const TPlanNode& plan) const override {
        return std::make_unique<GroupedLeafProvider>(_io, node, plan);
    }

private:
    std::shared_ptr<GroupedLeafIO> _io;
};
} // namespace

class GroupedConnectorLifecycleTest : public ConnectorScanNodeTest {
protected:
    void init(int groups = 1) {
        TQueryOptions options;
        options.__set_connector_io_tasks_per_scan_operator(2);
        // Initialize the query/instance tracker hierarchy only once on the older RuntimeState API.
        state = std::make_shared<RuntimeState>(TUniqueId(), options, TQueryGlobals(), _exec_env);
        state->set_chunk_size(1);
        query = std::make_shared<pipeline::QueryContext>();
        // Production query initialization receives a finite process/warehouse parent limit.
        _mem_tracker = std::make_shared<MemTracker>(2L << 30, "grouped lifecycle parent");
        query->init_mem_tracker(1L << 30, _mem_tracker.get());
        ASSERT_EQ(1L << 30, query->get_static_query_mem_limit());
        query->extend_query_lifetime();
        state->set_query_ctx(query.get());
        state->init_mem_trackers(query->mem_tracker());
        state->set_enable_pipeline_engine(true);
        auto* desc = create_table_desc(state.get(), {TypeDescriptor(TYPE_BIGINT)});
        io = std::make_shared<GroupedLeafIO>();
        // Registry entries own the leaf state; use a distinct name for each fixture.
        static std::atomic<int> sequence = 0;
        auto connector_name = "grouped_lifecycle_" + std::to_string(sequence++);
        connector::ConnectorManager::default_instance()->put(connector_name,
                                                             std::make_unique<GroupedLeafConnector>(io));
        auto plan = create_tplan_node_cloud();
        plan->__set_node_type(TPlanNodeType::LAKE_GROUPED_SCAN_NODE);
        plan->connector_scan_node.__set_connector_name(connector_name);
        TLakeScanNode lake;
        lake.__set_tuple_id(0);
        plan->__set_lake_scan_node(lake);
        node = std::make_shared<ConnectorScanNode>(state->obj_pool(), *plan, *desc);
        ASSERT_OK(node->init(*plan, state.get()));
        std::map<int32_t, std::vector<TScanRangeParams>> ranges;
        for (int group = 0; group < groups; ++group) {
            auto range = create_scan_ranges_cloud(1)[0];
            range.scan_range.internal_scan_range.__set_tablet_id(77); // Same source, distinct group envelope.
            range.scan_range.internal_scan_range.__set_bucket_sequence(group);
            TGroupedRangeTask task;
            task.__set_group_id(group);
            TTuple bound;
            for (int i = 0; i < 3; ++i) {
                TScalarType scalar;
                scalar.__set_type(TPrimitiveType::BIGINT);
                TTypeNode type_node;
                type_node.__set_type(TTypeNodeType::SCALAR);
                type_node.__set_scalar_type(scalar);
                TTypeDesc type;
                type.__set_types({type_node});
                type.__isset.types = true;
                TVariant value;
                value.__set_type(type);
                value.__set_variant_type(TVariantType::NORMAL_VALUE);
                value.__set_value("0");
                bound.values.emplace_back(std::move(value));
            }
            TTabletRange clip;
            if (group == 0) {
                clip.__set_upper_bound(bound);
                clip.__set_upper_bound_included(false);
            } else {
                clip.__set_lower_bound(bound);
                clip.__set_lower_bound_included(true);
            }
            task.__set_key_range(clip);
            task.__set_sort_key_unique_ids({100, 101, 102});
            range.scan_range.__set_grouped_range_task(task);
            roots.emplace_back(range.scan_range);
            ranges[group] = {range};
        }
        TScanRangeParams padding;
        padding.__set_empty(true);
        ranges[groups] = {padding};
        ASSIGN_OR_ABORT(queues,
                        node->convert_scan_range_to_morsel_queue_factory({}, ranges, node->id(), groups + 1, true, true,
                                                                         TTabletInternalParallelMode::AUTO, false));
        ASSERT_FALSE(queues->is_shared());
        ASSERT_FALSE(queues->could_local_shuffle());
        ASSERT_FALSE(queues->enable_random_append_split_morsel());
        factory = std::make_shared<pipeline::ConnectorScanOperatorFactory>(
                1, node.get(), state.get(), groups + 1,
                std::make_unique<pipeline::DynamicChunkBufferLimiter>(64, 64, 1L << 26, 1));
        factory->set_morsel_queue_factory(queues.get());
        factory->set_degree_of_parallelism(groups + 1);
        auto* arb = query->connector_scan_operator_mem_share_arbitrator();
        factory->set_mem_share_arb(arb);
        factory->set_scan_mem_limit(arb->scan_mem_limit);
        factory->set_data_source_mem_bytes(connector::DataSourceProvider::MIN_DATA_SOURCE_MEM_BYTES);
        factory->set_chunk_source_mem_bytes(connector::DataSourceProvider::MIN_DATA_SOURCE_MEM_BYTES + sizeof(int64_t));
        ASSERT_OK(factory->prepare(state.get()));
        std::unique_ptr<ThreadPool> pool;
        ASSERT_OK(ThreadPoolBuilder("grouped_lifecycle").set_min_threads(0).set_max_threads(4).build(&pool));
        executor = std::make_unique<workgroup::ScanExecutor>(
                std::move(pool), std::make_unique<workgroup::PriorityScanTaskQueue>(100), &metrics);
        executor->initialize(4);
        for (int driver = 0; driver <= groups; ++driver) {
            auto op = std::static_pointer_cast<pipeline::ConnectorScanOperator>(factory->create(groups + 1, driver));
            op->add_morsel_queue(queues->create(driver));
            op->set_scan_executor(executor.get());
            op->set_query_ctx(query);
            ASSERT_OK(op->prepare(state.get()));
            ASSERT_TRUE(op->enable_adaptive_io_tasks());
            ops.emplace_back(std::move(op));
            EXPECT_FALSE(queues->create(driver)->has_more_scan_ranges());
            EXPECT_FALSE(queues->create(driver)->has_more_from_split());
        }
        EXPECT_TRUE(queues->create(groups)->empty());
        EXPECT_TRUE(ops.back()->is_finished()); // Padding has no synthetic root or producer.
    }
    void TearDown() override {
        if (io != nullptr) io->release_all();
        if (executor != nullptr) executor->close(); // Join every real I/O task before destroying its operator.
        for (auto& op : ops) {
            ASSERT_OK(op->set_finishing(state.get()));
            op->close(state.get());
        }
        ops.clear();
        if (factory != nullptr) factory->close(state.get());
        if (io != nullptr) EXPECT_EQ(io->opened, io->closed);
        if (factory != nullptr) EXPECT_TRUE(factory->get_active_inputs().empty());
        if (query != nullptr) {
            EXPECT_EQ(0, query->connector_scan_mem_tracker()->consumption());
            EXPECT_EQ(0, query->mem_tracker()->consumption());
        }
        factory.reset();
        queues.reset();
        if (io != nullptr) {
            for (const auto& child : io->child_ranges) EXPECT_TRUE(child.expired());
        }
        ConnectorScanNodeTest::TearDown();
    }
    bool wait_io(const std::function<bool()>& predicate) {
        std::unique_lock lock(io->mutex);
        return io->cv.wait_for(lock, std::chrono::seconds(5), predicate);
    }
    bool wait_idle() {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            if (metrics.running_tasks.value() == 0 && metrics.pending_tasks.value() == 0) return true;
            std::this_thread::yield();
        }
        return false;
    }
    void release_roots() {
        std::lock_guard lock(io->mutex);
        io->release_roots = true;
        io->cv.notify_all();
    }
    bool drain(int driver, std::vector<int64_t>* rows) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            auto& op = ops[driver];
            if (op->is_finished()) return true;
            if (op->has_output()) {
                auto result = op->pull_chunk(state.get());
                if (!result.ok()) return false;
                auto chunk = std::move(result).value();
                if (chunk != nullptr && !chunk->is_empty()) {
                    rows->emplace_back(chunk->get_column_by_index(0)->get(0).get_int64());
                }
            }
            std::this_thread::yield();
        }
        return false;
    }
    void check_failure(const Status& failure, bool after_publication) {
        init();
        ASSERT_FALSE(HasFatalFailure());
        ASSERT_TRUE(ops[0]->pull_chunk(state.get()).ok());
        ASSERT_TRUE(wait_io([&] { return io->roots_entered == 1; }));
        EXPECT_TRUE(queues->create(0)->empty());
        EXPECT_FALSE(ops[0]->is_finished());
        if (after_publication) {
            release_roots();
            ASSERT_TRUE(wait_io([&] { return io->closed == 1; }));
            ASSERT_TRUE(wait_idle());
            EXPECT_FALSE(queues->create(0)->empty());
            std::lock_guard lock(io->mutex);
            io->child_status = failure;
        } else {
            std::lock_guard lock(io->mutex);
            io->root_status = failure;
        }
        io->release_all();
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        Status observed;
        while (std::chrono::steady_clock::now() < deadline) {
            if (ops[0]->has_output()) {
                auto result = ops[0]->pull_chunk(state.get());
                if (!result.ok()) {
                    observed = result.status();
                    break;
                }
            }
            std::this_thread::yield();
        }
        EXPECT_EQ(failure.code(), observed.code()) << observed;
        if (failure.is_mem_limit_exceeded()) {
            EXPECT_NE(std::string::npos, observed.to_string().find("grouped leaf allocation"));
            EXPECT_NE(std::string::npos, observed.to_string().find("query_mem_limit"));
        }
        ASSERT_OK(ops[0]->set_finishing(state.get()));
        EXPECT_TRUE(ops[0]->is_finished());
        ASSERT_TRUE(wait_io([&] { return io->opened == io->closed; }));
    }

    std::shared_ptr<RuntimeState> state;
    pipeline::QueryContextPtr query;
    std::shared_ptr<GroupedLeafIO> io;
    std::shared_ptr<ConnectorScanNode> node;
    std::unique_ptr<pipeline::MorselQueueFactory> queues;
    std::shared_ptr<pipeline::ConnectorScanOperatorFactory> factory;
    pipeline::ScanExecutorMetrics metrics;
    std::unique_ptr<workgroup::ScanExecutor> executor;
    std::vector<std::shared_ptr<pipeline::ConnectorScanOperator>> ops;
    std::vector<TScanRange> roots;
};

TEST_F(GroupedConnectorLifecycleTest, GroupedLastRootCanPublishChildrenBeforeEos) {
    init();
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(ops[0]->pull_chunk(state.get()).ok());
    ASSERT_TRUE(wait_io([&] { return io->roots_entered == 1; }));
    EXPECT_TRUE(queues->create(0)->empty());
    EXPECT_FALSE(queues->create(0)->has_more());
    EXPECT_FALSE(ops[0]->is_finished());
    EXPECT_FALSE(factory->get_active_inputs().empty());
    release_roots();
    ASSERT_TRUE(wait_io([&] { return io->closed == 1; }));
    ASSERT_TRUE(wait_idle());
    EXPECT_FALSE(queues->create(0)->empty());
    EXPECT_FALSE(ops[0]->is_finished());
    ASSERT_TRUE(ops[0]->pull_chunk(state.get()).ok());
    ASSERT_TRUE(wait_io([&] { return io->children_entered == 2; }));
    EXPECT_TRUE(queues->create(0)->empty());
    EXPECT_FALSE(ops[0]->is_finished());
    io->release_all();
    ASSERT_TRUE(wait_io([&] { return io->closed == 3; }));
    ASSERT_TRUE(wait_idle());
    EXPECT_TRUE(factory->get_active_inputs().empty());
    EXPECT_FALSE(factory->get_chunk_buffer().empty(0));
    EXPECT_FALSE(ops[0]->is_finished()); // Buffered rows are part of the EOS contract.
    std::vector<int64_t> rows;
    ASSERT_TRUE(drain(0, &rows));
    std::sort(rows.begin(), rows.end());
    EXPECT_EQ((std::vector<int64_t>{1, 2}), rows);
    EXPECT_EQ(3, ops[0]->unique_metrics()->get_counter("MorselsCount")->value());
    EXPECT_EQ(2, ops[0]->unique_metrics()->get_counter("PeakIOTasks")->value());
}

TEST_F(GroupedConnectorLifecycleTest, GroupedChildrenRetainDriverAndEnvelope) {
    init(2);
    ASSERT_FALSE(HasFatalFailure());
    for (int driver = 0; driver < 2; ++driver) ASSERT_TRUE(ops[driver]->pull_chunk(state.get()).ok());
    ASSERT_TRUE(wait_io([&] { return io->roots_entered == 2; }));
    release_roots();
    ASSERT_TRUE(wait_io([&] { return io->closed == 2; }));
    ASSERT_TRUE(wait_idle());
    for (int driver = 0; driver < 2; ++driver) ASSERT_TRUE(ops[driver]->pull_chunk(state.get()).ok());
    ASSERT_TRUE(wait_io([&] { return io->children_entered == 4; }));
    io->release_all();
    ASSERT_TRUE(wait_io([&] { return io->closed == 6; }));
    ASSERT_TRUE(wait_idle());
    for (int driver = 0; driver < 2; ++driver) {
        std::vector<int64_t> rows;
        ASSERT_TRUE(drain(driver, &rows));
        std::sort(rows.begin(), rows.end());
        EXPECT_EQ((std::vector<int64_t>{100 * driver + 1, 100 * driver + 2}), rows);
    }
    for (const auto& [driver, range] : io->inputs) EXPECT_EQ(roots[driver], range);
    EXPECT_TRUE(queues->create(2)->empty());
}

TEST_F(GroupedConnectorLifecycleTest, GroupedHeldRootErrorCleansUp) {
    check_failure(Status::InternalError("held grouped root failed"), false);
}
TEST_F(GroupedConnectorLifecycleTest, GroupedPublishedChildrenErrorCleansUp) {
    check_failure(Status::InternalError("grouped child failed"), true);
}
TEST_F(GroupedConnectorLifecycleTest, GroupedHeldRootMemoryLimitCleansUp) {
    check_failure(Status::MemoryLimitExceeded("held grouped root exceeded query memory"), false);
}
TEST_F(GroupedConnectorLifecycleTest, GroupedPublishedChildrenMemoryLimitCleansUp) {
    check_failure(Status::MemoryLimitExceeded("grouped child exceeded query memory"), true);
}
TEST_F(GroupedConnectorLifecycleTest, GroupedCancelHeldRoot) {
    init();
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(ops[0]->pull_chunk(state.get()).ok());
    ASSERT_TRUE(wait_io([&] { return io->roots_entered == 1; }));
    state->set_is_cancelled(true);
    ASSERT_OK(ops[0]->set_finishing(state.get()));
    io->release_all();
    ASSERT_TRUE(wait_io([&] { return io->closed == 1; }));
    ASSERT_TRUE(wait_idle());
    EXPECT_TRUE(ops[0]->is_finished());
    EXPECT_TRUE(factory->get_active_inputs().empty());
    EXPECT_TRUE(queues->create(0)->empty());
}

TEST_F(GroupedConnectorLifecycleTest, GroupedCancelQueuedChildren) {
    init();
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(ops[0]->pull_chunk(state.get()).ok());
    ASSERT_TRUE(wait_io([&] { return io->roots_entered == 1; }));
    release_roots();
    ASSERT_TRUE(wait_io([&] { return io->closed == 1; }));
    ASSERT_TRUE(wait_idle());
    EXPECT_FALSE(queues->create(0)->empty());
    state->set_is_cancelled(true);
    ASSERT_OK(ops[0]->set_finishing(state.get()));
    EXPECT_TRUE(ops[0]->is_finished());
    EXPECT_TRUE(factory->get_active_inputs().empty());
    EXPECT_EQ(1, io->opened); // Queued children never acquire readers.
}

TEST_F(GroupedConnectorLifecycleTest, GroupedCancelRunningChildren) {
    init();
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(ops[0]->pull_chunk(state.get()).ok());
    ASSERT_TRUE(wait_io([&] { return io->roots_entered == 1; }));
    release_roots();
    ASSERT_TRUE(wait_io([&] { return io->closed == 1; }));
    ASSERT_TRUE(wait_idle());
    ASSERT_TRUE(ops[0]->pull_chunk(state.get()).ok());
    ASSERT_TRUE(wait_io([&] { return io->children_entered == 2; }));
    state->set_is_cancelled(true);
    ASSERT_OK(ops[0]->set_finishing(state.get()));
    io->release_all();
    ASSERT_TRUE(wait_io([&] { return io->closed == 3; }));
    ASSERT_TRUE(wait_idle());
    EXPECT_TRUE(ops[0]->is_finished());
    EXPECT_TRUE(factory->get_active_inputs().empty());
    EXPECT_TRUE(factory->get_chunk_buffer().empty(0));
}

TEST_F(GroupedConnectorLifecycleTest, GroupedLimitFinishesWithHeldRoot) {
    init();
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(ops[0]->pull_chunk(state.get()).ok());
    ASSERT_TRUE(wait_io([&] { return io->roots_entered == 1; }));
    ASSERT_OK(ops[0]->set_finishing(state.get()));
    EXPECT_TRUE(ops[0]->is_finished());
    EXPECT_TRUE(factory->get_active_inputs().empty());
    io->release_all();
    ASSERT_TRUE(wait_io([&] { return io->closed == 1; }));
    ASSERT_TRUE(wait_idle());
    EXPECT_TRUE(factory->get_chunk_buffer().empty(0));
    EXPECT_EQ(1, io->opened);
}

TEST_F(GroupedConnectorLifecycleTest, GroupedLimitFinishesWithQueuedChildren) {
    init();
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(ops[0]->pull_chunk(state.get()).ok());
    ASSERT_TRUE(wait_io([&] { return io->roots_entered == 1; }));
    release_roots();
    ASSERT_TRUE(wait_io([&] { return io->closed == 1; }));
    ASSERT_TRUE(wait_idle());
    // A downstream LIMIT uses set_finishing without consuming the queued children.
    EXPECT_FALSE(queues->create(0)->empty());
    ASSERT_OK(ops[0]->set_finishing(state.get()));
    EXPECT_TRUE(ops[0]->is_finished());
    EXPECT_TRUE(factory->get_active_inputs().empty());
    EXPECT_TRUE(factory->get_chunk_buffer().empty(0));
    EXPECT_EQ(1, io->opened);
}

} // namespace starrocks
