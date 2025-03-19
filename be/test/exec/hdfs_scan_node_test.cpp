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

// #include "exec/hdfs_scan_node.h"
#include <gtest/gtest.h>

#include <memory>

#include "column/column_helper.h"
#include "exec/connector_scan_node.h"
#include "exec/pipeline/fragment_context.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/storage_engine.h"
#include "util/disk_info.h"
#include "util/mem_info.h"

//TODO: test multi thread
//TODO: test runtime filter
namespace starrocks {
class HdfsScanNodeTest : public ::testing::Test {
public:
    void SetUp() override {
        config::enable_system_metrics = false;
        config::enable_metric_calculator = false;

        _exec_env = ExecEnv::GetInstance();

        _create_runtime_state();
        _pool = _runtime_state->obj_pool();
    }
    void TearDown() override {}

private:
    void _create_runtime_state();
    std::shared_ptr<TPlanNode> _create_tplan_node();
    std::shared_ptr<TPlanNode> _create_tplan_node_for_filter_partition();
    DescriptorTbl* _create_table_desc();
    DescriptorTbl* _create_table_desc_for_filter_partition();
    std::vector<TScanRangeParams> _create_scan_ranges();
    std::vector<TScanRangeParams> _create_scan_ranges_for_filter_partition();
    static ChunkPtr _create_chunk();

    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    HdfsTableDescriptor* _table_desc = nullptr;
    ObjectPool* _pool = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker = nullptr;
    ExecEnv* _exec_env = nullptr;
    // num rows: 4
    std::string _file = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet1";
    int64_t _file_size = 729;
    // num rows: 11
    std::string _file_2 = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet2";
    int64_t _file_2_size = 850;
};

ChunkPtr HdfsScanNodeTest::_create_chunk() {
    ChunkPtr chunk = std::make_shared<Chunk>();

    auto col1 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true);
    auto col2 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    auto col3 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true);
    auto col4 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME), true);
    chunk->append_column(std::move(col1), 0);
    chunk->append_column(std::move(col2), 1);
    chunk->append_column(std::move(col3), 2);
    chunk->append_column(std::move(col4), 3);

    return chunk;
}

std::vector<TScanRangeParams> HdfsScanNodeTest::_create_scan_ranges() {
    THdfsScanRange hdfs_scan_range;
    hdfs_scan_range.__set_file_length(_file_size);
    hdfs_scan_range.__set_offset(4);
    hdfs_scan_range.__set_length(_file_size);
    hdfs_scan_range.__set_partition_id(0);
    hdfs_scan_range.__set_relative_path(_file);
    hdfs_scan_range.__set_file_format(THdfsFileFormat::PARQUET);

    TScanRange scan_range;
    scan_range.__set_hdfs_scan_range(hdfs_scan_range);

    TScanRangeParams param;
    param.__set_scan_range(scan_range);

    return std::vector<TScanRangeParams>{param};
}

std::vector<TScanRangeParams> HdfsScanNodeTest::_create_scan_ranges_for_filter_partition() {
    // scan range 0
    THdfsScanRange hdfs_scan_range;
    hdfs_scan_range.__set_file_length(_file_size);
    hdfs_scan_range.__set_offset(4);
    hdfs_scan_range.__set_length(_file_size);
    hdfs_scan_range.__set_partition_id(0);
    hdfs_scan_range.__set_relative_path(_file);
    hdfs_scan_range.__set_file_format(THdfsFileFormat::PARQUET);

    TScanRange scan_range;
    scan_range.__set_hdfs_scan_range(hdfs_scan_range);

    TScanRangeParams param;
    param.__set_scan_range(scan_range);

    // scan range 1
    THdfsScanRange hdfs_scan_range2;
    hdfs_scan_range2.__set_file_length(_file_2_size);
    hdfs_scan_range2.__set_offset(4);
    hdfs_scan_range2.__set_length(_file_2_size);
    hdfs_scan_range2.__set_partition_id(1);
    hdfs_scan_range2.__set_relative_path(_file_2);
    hdfs_scan_range2.__set_file_format(THdfsFileFormat::PARQUET);

    TScanRange scan_range2;
    scan_range2.__set_hdfs_scan_range(hdfs_scan_range2);

    TScanRangeParams param2;
    param2.__set_scan_range(scan_range2);

    return std::vector<TScanRangeParams>{param, param2};
}

void HdfsScanNodeTest::_create_runtime_state() {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
    TUniqueId id;
    _mem_tracker = std::make_shared<MemTracker>(-1, "olap scanner test");
    _runtime_state->init_mem_trackers(id);
    pipeline::FragmentContext* fragment_context = _runtime_state->obj_pool()->add(new pipeline::FragmentContext());
    fragment_context->set_pred_tree_params({true, true});
    _runtime_state->set_fragment_ctx(fragment_context);
}

std::shared_ptr<TPlanNode> HdfsScanNodeTest::_create_tplan_node() {
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

std::shared_ptr<TPlanNode> HdfsScanNodeTest::_create_tplan_node_for_filter_partition() {
    std::vector<::starrocks::TTupleId> tuple_ids{0};

    auto tnode = std::make_shared<TPlanNode>();

    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::HDFS_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
    tnode->__set_limit(-1);

    // partition_conjuncts
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::BINARY_PRED;
    node0.opcode = TExprOpcode::EQ;
    node0.child_type = TPrimitiveType::INT;
    node0.num_children = 2;
    node0.__isset.opcode = true;
    node0.__isset.child_type = true;
    node0.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(TPrimitiveType::INT);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = 0;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    TExprNode node2;
    node2.node_type = TExprNodeType::INT_LITERAL;
    node2.type = gen_type_desc(TPrimitiveType::INT);
    node2.num_children = 0;
    TIntLiteral int_literal;
    int_literal.value = 1;
    node2.__set_int_literal(int_literal);
    node2.is_nullable = false;
    nodes.emplace_back(node2);

    TExpr t_expr;
    t_expr.nodes = nodes;

    tnode->hdfs_scan_node.__set_partition_conjuncts({t_expr});

    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::HIVE;
    tnode->__set_connector_scan_node(connector_scan_node);

    return tnode;
}

DescriptorTbl* HdfsScanNodeTest::_create_table_desc() {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot1 = slot_desc_builder.type(LogicalType::TYPE_INT).column_name("col1").column_pos(0).nullable(true).build();
    auto slot2 =
            slot_desc_builder.type(LogicalType::TYPE_BIGINT).column_name("col2").column_pos(1).nullable(true).build();
    auto slot3 = slot_desc_builder.string_type(255).column_name("col3").column_pos(2).nullable(true).build();
    auto slot4 =
            slot_desc_builder.type(LogicalType::TYPE_DATETIME).column_name("col4").column_pos(3).nullable(true).build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.add_slot(slot2);
    tuple_desc_builder.add_slot(slot3);
    tuple_desc_builder.add_slot(slot4);
    tuple_desc_builder.build(&table_desc_builder);
    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(_runtime_state.get(), _pool, table_desc_builder.desc_tbl(), &tbl,
                                config::vector_chunk_size)
                  .ok());

    THdfsPartition partition;
    std::map<int64_t, THdfsPartition> p_map;
    p_map[0] = partition;

    THdfsTable t_hdfs_table;
    t_hdfs_table.__set_partitions(p_map);
    TTableDescriptor tdesc;
    tdesc.__set_hdfsTable(t_hdfs_table);
    _table_desc = _pool->add(new HdfsTableDescriptor(tdesc, _pool));
    tbl->get_tuple_descriptor(0)->set_table_desc(_table_desc);

    return tbl;
}

DescriptorTbl* HdfsScanNodeTest::_create_table_desc_for_filter_partition() {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot1 = slot_desc_builder.type(LogicalType::TYPE_INT).column_name("col1").column_pos(0).nullable(true).build();
    auto slot2 =
            slot_desc_builder.type(LogicalType::TYPE_BIGINT).column_name("col2").column_pos(1).nullable(true).build();
    auto slot3 = slot_desc_builder.string_type(255).column_name("col3").column_pos(2).nullable(true).build();
    auto slot4 =
            slot_desc_builder.type(LogicalType::TYPE_DATETIME).column_name("col4").column_pos(3).nullable(true).build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.add_slot(slot2);
    tuple_desc_builder.add_slot(slot3);
    tuple_desc_builder.add_slot(slot4);
    tuple_desc_builder.build(&table_desc_builder);
    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(_runtime_state.get(), _pool, table_desc_builder.desc_tbl(), &tbl,
                                config::vector_chunk_size)
                  .ok());

    // hdfs table
    THdfsTable t_hdfs_table;

    // hdfs table columns
    TColumn col1;
    col1.column_name = "col1";
    t_hdfs_table.columns.emplace_back(col1);
    TColumn col2;
    col2.column_name = "col2";
    t_hdfs_table.columns.emplace_back(col2);
    TColumn col3;
    col3.column_name = "col3";
    t_hdfs_table.columns.emplace_back(col3);
    TColumn col4;
    col4.column_name = "col4";
    t_hdfs_table.columns.emplace_back(col4);

    // hdfs table partition columns
    t_hdfs_table.partition_columns.emplace_back(col1);

    // hdfs table partitions
    std::map<int64_t, THdfsPartition> p_map;

    // partition 0
    {
        THdfsPartition partition;
        std::vector<TExprNode> nodes;
        TExprNode node;
        node.node_type = TExprNodeType::INT_LITERAL;
        node.type = gen_type_desc(TPrimitiveType::INT);
        node.num_children = 0;
        TIntLiteral int_literal;
        int_literal.value = 0;
        node.__set_int_literal(int_literal);
        node.is_nullable = false;
        node.has_nullable_child = false;
        nodes.emplace_back(node);
        TExpr t_expr;
        t_expr.nodes = nodes;
        partition.__set_partition_key_exprs({t_expr});
        p_map[0] = partition;
    }

    // partition 1
    {
        THdfsPartition partition1;
        std::vector<TExprNode> nodes;
        TExprNode node;
        node.node_type = TExprNodeType::INT_LITERAL;
        node.type = gen_type_desc(TPrimitiveType::INT);
        node.num_children = 0;
        TIntLiteral int_literal;
        int_literal.value = 1;
        node.__set_int_literal(int_literal);
        node.is_nullable = false;
        node.has_nullable_child = false;
        nodes.emplace_back(node);
        TExpr t_expr;
        t_expr.nodes = nodes;
        partition1.__set_partition_key_exprs({t_expr});
        p_map[1] = partition1;
    }

    t_hdfs_table.__set_partitions(p_map);
    TTableDescriptor tdesc;
    tdesc.__set_hdfsTable(t_hdfs_table);
    _table_desc = _pool->add(new HdfsTableDescriptor(tdesc, _pool));
    _table_desc->create_key_exprs(_runtime_state.get(), _pool);
    tbl->get_tuple_descriptor(0)->set_table_desc(_table_desc);

    return tbl;
}

TEST_F(HdfsScanNodeTest, TestBasic) {
    {
        auto tnode = _create_tplan_node();
        auto* descs = _create_table_desc();
        _runtime_state->set_desc_tbl(descs);
        auto hdfs_scan_node = std::make_shared<ConnectorScanNode>(_pool, *tnode, *descs);

        Status status = hdfs_scan_node->init(*tnode, _runtime_state.get());
        ASSERT_TRUE(status.ok());

        status = hdfs_scan_node->prepare(_runtime_state.get());
        ASSERT_TRUE(status.ok());

        auto scan_ranges = _create_scan_ranges();
        status = hdfs_scan_node->set_scan_ranges(scan_ranges);
        ASSERT_TRUE(status.ok());

        status = hdfs_scan_node->open(_runtime_state.get());
        ASSERT_TRUE(status.ok());

        auto chunk = _create_chunk();
        bool eos = false;

        status = hdfs_scan_node->get_next(_runtime_state.get(), &chunk, &eos);
        ASSERT_TRUE(status.ok());
        ASSERT_FALSE(eos);
        ASSERT_EQ(chunk->num_rows(), 4);

        hdfs_scan_node->close(_runtime_state.get());
    }

    // test filter partition
    {
        auto tnode = _create_tplan_node_for_filter_partition();
        auto* descs = _create_table_desc_for_filter_partition();
        _runtime_state->set_desc_tbl(descs);
        auto hdfs_scan_node = std::make_shared<ConnectorScanNode>(_pool, *tnode, *descs);

        Status status = hdfs_scan_node->init(*tnode, _runtime_state.get());
        ASSERT_TRUE(status.ok());

        status = hdfs_scan_node->prepare(_runtime_state.get());
        ASSERT_TRUE(status.ok());

        auto scan_ranges = _create_scan_ranges_for_filter_partition();
        status = hdfs_scan_node->set_scan_ranges(scan_ranges);
        ASSERT_TRUE(status.ok());

        status = hdfs_scan_node->open(_runtime_state.get());
        ASSERT_TRUE(status.ok());

        auto chunk = _create_chunk();
        bool eos = false;

        status = hdfs_scan_node->get_next(_runtime_state.get(), &chunk, &eos);
        ASSERT_TRUE(status.ok());
        ASSERT_FALSE(eos);
        ASSERT_EQ(chunk->num_rows(), 11);

        chunk->reset();
        status = hdfs_scan_node->get_next(_runtime_state.get(), &chunk, &eos);
        ASSERT_TRUE(eos);

        hdfs_scan_node->close(_runtime_state.get());
    }
}
} // namespace starrocks
