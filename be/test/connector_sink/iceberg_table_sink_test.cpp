// Copyright 2021-present StarRocks, Inc. All rights reserved.  //
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

#include "runtime/iceberg_table_sink.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "base/testutil/assert.h"
#include "exec/pipeline/empty_set_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "runtime/descriptor_helper.h"
#include "runtime/types.h"

namespace starrocks {

class IcebergTableSinkTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>());
        _runtime_state = _fragment_context->runtime_state();
    }

    void TearDown() override {}

    ObjectPool _pool;
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;
    RuntimeState* _runtime_state;
};

TEST_F(IcebergTableSinkTest, decompose_to_pipeline) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot1 = slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c1").column_pos(0).nullable(true).build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.build(&table_desc_builder);
    DescriptorTbl* tbl = nullptr;
    EXPECT_OK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size));
    _runtime_state->set_desc_tbl(tbl);

    TIcebergTable t_iceberg_table;

    TColumn t_column;
    t_column.__set_column_name("c1");
    t_iceberg_table.__set_columns({t_column});

    TSortOrder sort_order;
    sort_order.__set_sort_key_idxes({0});
    sort_order.__set_is_ascs({true});
    sort_order.__set_is_null_firsts({true});
    t_iceberg_table.__set_sort_order(sort_order);

    TTableDescriptor tdesc;
    tdesc.__set_icebergTable(t_iceberg_table);

    IcebergTableDescriptor* ice_table_desc = _pool.add(new IcebergTableDescriptor(tdesc, &_pool));
    tbl->get_tuple_descriptor(0)->set_table_desc(ice_table_desc);
    tbl->_tbl_desc_map[0] = ice_table_desc;

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1, false);

    TDataSink data_sink;
    TIcebergTableSink iceberg_table_sink;
    data_sink.iceberg_table_sink = iceberg_table_sink;

    std::vector<starrocks::TExpr> exprs = {};
    IcebergTableSink sink(&_pool, exprs);
    auto connector = connector::ConnectorManager::default_instance()->get(connector::Connector::ICEBERG);
    auto sink_provider = connector->create_data_sink_provider();
    pipeline::OpFactories prev_operators{std::make_shared<pipeline::EmptySetOperatorFactory>(1, 1)};

    EXPECT_OK(sink.decompose_to_pipeline(prev_operators, data_sink, context.get()));

    pipeline::Pipeline* pl = const_cast<pipeline::Pipeline*>(context->last_pipeline());
    pipeline::OperatorFactory* op_factory = pl->sink_operator_factory();
    auto connector_sink_factory = dynamic_cast<pipeline::ConnectorSinkOperatorFactory*>(op_factory);
    auto sink_ctx = dynamic_cast<connector::IcebergChunkSinkContext*>(connector_sink_factory->_sink_context.get());
    EXPECT_EQ(sink_ctx->sort_ordering->sort_key_idxes.size(), 1);
    EXPECT_EQ(sink_ctx->sort_ordering->sort_descs.descs.size(), 1);
}

// Test case for verifying the path construction logic in IcebergTableSink
TEST_F(IcebergTableSinkTest, path_construction_logic) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot1 = slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c1").column_pos(0).nullable(true).build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.build(&table_desc_builder);
    DescriptorTbl* tbl = nullptr;
    EXPECT_OK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size));
    _runtime_state->set_desc_tbl(tbl);

    TIcebergTable t_iceberg_table;
    TColumn t_column;
    t_column.__set_column_name("c1");
    t_iceberg_table.__set_columns({t_column});
    TTableDescriptor tdesc;
    tdesc.__set_icebergTable(t_iceberg_table);

    IcebergTableDescriptor* ice_table_desc = _pool.add(new IcebergTableDescriptor(tdesc, &_pool));
    tbl->get_tuple_descriptor(0)->set_table_desc(ice_table_desc);
    tbl->_tbl_desc_map[0] = ice_table_desc;

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1, false);

    // Test case 1: data_location is set and not empty, should use data_location
    {
        TDataSink data_sink;
        TIcebergTableSink iceberg_table_sink;
        iceberg_table_sink.__set_location("s3://bucket/table-location");
        iceberg_table_sink.__set_data_location("s3://bucket/data-location");
        data_sink.__set_iceberg_table_sink(iceberg_table_sink);

        std::vector<starrocks::TExpr> exprs = {};
        IcebergTableSink sink(&_pool, exprs);
        pipeline::OpFactories prev_operators{std::make_shared<pipeline::EmptySetOperatorFactory>(1, 1)};

        EXPECT_OK(sink.decompose_to_pipeline(prev_operators, data_sink, context.get()));

        pipeline::Pipeline* pl = const_cast<pipeline::Pipeline*>(context->last_pipeline());
        pipeline::OperatorFactory* op_factory = pl->sink_operator_factory();
        auto connector_sink_factory = dynamic_cast<pipeline::ConnectorSinkOperatorFactory*>(op_factory);
        auto sink_ctx = dynamic_cast<connector::IcebergChunkSinkContext*>(connector_sink_factory->_sink_context.get());

        // Should use data_location when it's set and not empty
        EXPECT_EQ(sink_ctx->path, "s3://bucket/data-location");
    }

    // Test case 2: data_location is not set, should use location + "/data"
    {
        TDataSink data_sink;
        TIcebergTableSink iceberg_table_sink;
        iceberg_table_sink.__set_location("s3://bucket/table-location");
        // data_location is not set
        data_sink.__set_iceberg_table_sink(iceberg_table_sink);

        std::vector<starrocks::TExpr> exprs = {};
        IcebergTableSink sink(&_pool, exprs);
        pipeline::OpFactories prev_operators{std::make_shared<pipeline::EmptySetOperatorFactory>(2, 2)};

        EXPECT_OK(sink.decompose_to_pipeline(prev_operators, data_sink, context.get()));

        pipeline::Pipeline* pl = const_cast<pipeline::Pipeline*>(context->last_pipeline());
        pipeline::OperatorFactory* op_factory = pl->sink_operator_factory();
        auto connector_sink_factory = dynamic_cast<pipeline::ConnectorSinkOperatorFactory*>(op_factory);
        auto sink_ctx = dynamic_cast<connector::IcebergChunkSinkContext*>(connector_sink_factory->_sink_context.get());

        // Should use location + "/data" when data_location is not set
        EXPECT_EQ(sink_ctx->path, "s3://bucket/table-location/data");
    }

    // Test case 3: data_location is set but empty, should use location + "/data"
    {
        TDataSink data_sink;
        TIcebergTableSink iceberg_table_sink;
        iceberg_table_sink.__set_location("s3://bucket/table-location");
        iceberg_table_sink.__set_data_location(""); // Empty data_location
        data_sink.__set_iceberg_table_sink(iceberg_table_sink);

        std::vector<starrocks::TExpr> exprs = {};
        IcebergTableSink sink(&_pool, exprs);
        pipeline::OpFactories prev_operators{std::make_shared<pipeline::EmptySetOperatorFactory>(3, 3)};

        EXPECT_OK(sink.decompose_to_pipeline(prev_operators, data_sink, context.get()));

        pipeline::Pipeline* pl = const_cast<pipeline::Pipeline*>(context->last_pipeline());
        pipeline::OperatorFactory* op_factory = pl->sink_operator_factory();
        auto connector_sink_factory = dynamic_cast<pipeline::ConnectorSinkOperatorFactory*>(op_factory);
        auto sink_ctx = dynamic_cast<connector::IcebergChunkSinkContext*>(connector_sink_factory->_sink_context.get());

        // Should use location + "/data" when data_location is empty
        EXPECT_EQ(sink_ctx->path, "s3://bucket/table-location/data");
    }
}

// Test update_partition_expr_slot_refs_by_map function
TEST_F(IcebergTableSinkTest, update_partition_expr_slot_refs_by_map) {
    std::vector<TExpr> partition_expr;
    std::vector<std::string> partition_source_column_names;
    std::unordered_map<std::string, TExprNode> column_slot_map;

    // Setup: Create a partition expression for column "dt"
    TExpr partition_dt_expr;
    TExprNode slot_ref_node;
    slot_ref_node.node_type = TExprNodeType::SLOT_REF;
    slot_ref_node.__isset.slot_ref = true;
    slot_ref_node.slot_ref.slot_id = 999; // Wrong slot id initially
    partition_dt_expr.nodes.push_back(slot_ref_node);
    partition_expr.push_back(partition_dt_expr);
    partition_source_column_names.push_back("dt");

    // Setup column slot map with correct slot reference for "dt"
    TExprNode correct_dt_node;
    correct_dt_node.node_type = TExprNodeType::SLOT_REF;
    correct_dt_node.__isset.slot_ref = true;
    correct_dt_node.slot_ref.slot_id = 5;
    correct_dt_node.slot_ref.tuple_id = 0;
    column_slot_map["dt"] = correct_dt_node;

    // Create sink instance (need a valid TExpr vector for constructor)
    std::vector<TExpr> output_exprs;
    IcebergTableSink sink(&_pool, output_exprs);

    // Execute the function
    ASSERT_OK(sink.update_partition_expr_slot_refs_by_map(partition_expr, column_slot_map,
                                                          partition_source_column_names));

    // Verify the partition expression was updated with correct slot reference
    ASSERT_EQ(partition_expr.size(), 1);
    ASSERT_EQ(partition_expr[0].nodes.size(), 1);
    EXPECT_EQ(partition_expr[0].nodes[0].slot_ref.slot_id, 5);
    EXPECT_EQ(partition_expr[0].nodes[0].slot_ref.tuple_id, 0);
}

// Test update_partition_expr_slot_refs_by_map with mismatched sizes
TEST_F(IcebergTableSinkTest, update_partition_expr_slot_refs_by_map_mismatched_sizes) {
    std::vector<TExpr> partition_expr;
    std::vector<std::string> partition_source_column_names;
    std::unordered_map<std::string, TExprNode> column_slot_map;

    // Create 2 partition expressions
    TExpr expr1, expr2;
    partition_expr.push_back(expr1);
    partition_expr.push_back(expr2);

    // But only 1 column name
    partition_source_column_names.push_back("dt");

    // Create sink instance
    std::vector<TExpr> output_exprs;
    IcebergTableSink sink(&_pool, output_exprs);

    // Should return error for mismatched sizes
    auto st =
            sink.update_partition_expr_slot_refs_by_map(partition_expr, column_slot_map, partition_source_column_names);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.message().find("Mismatched partition expression and column name counts") != std::string::npos);
}

// Test update_partition_expr_slot_refs_by_map with missing column slot
TEST_F(IcebergTableSinkTest, update_partition_expr_slot_refs_by_map_missing_slot) {
    std::vector<TExpr> partition_expr;
    std::vector<std::string> partition_source_column_names;
    std::unordered_map<std::string, TExprNode> column_slot_map;

    // Setup: Create partition expression for column "dt"
    TExpr partition_dt_expr;
    partition_expr.push_back(partition_dt_expr);
    partition_source_column_names.push_back("dt");

    // Don't add "dt" to column_slot_map (missing slot reference)

    // Create sink instance
    std::vector<TExpr> output_exprs;
    IcebergTableSink sink(&_pool, output_exprs);

    // Should return error for missing slot reference
    auto st =
            sink.update_partition_expr_slot_refs_by_map(partition_expr, column_slot_map, partition_source_column_names);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.message().find("Could not find slot reference for partition column: dt") != std::string::npos);
}

// Test decompose_to_pipeline with ICEBERG_DELETE_SINK type
TEST_F(IcebergTableSinkTest, decompose_to_pipeline_delete_sink) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot1 = slot_desc_builder.type(LogicalType::TYPE_VARCHAR)
                         .column_name("file_path")
                         .column_pos(0)
                         .nullable(true)
                         .build();
    auto slot2 =
            slot_desc_builder.type(LogicalType::TYPE_BIGINT).column_name("pos").column_pos(1).nullable(true).build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.add_slot(slot2);
    tuple_desc_builder.build(&table_desc_builder);
    DescriptorTbl* tbl = nullptr;
    EXPECT_OK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size));
    _runtime_state->set_desc_tbl(tbl);

    TIcebergTable t_iceberg_table;
    TColumn t_column1, t_column2;
    t_column1.__set_column_name("file_path");
    t_column2.__set_column_name("pos");
    t_iceberg_table.__set_columns({t_column1, t_column2});

    TTableDescriptor tdesc;
    tdesc.__set_icebergTable(t_iceberg_table);

    IcebergTableDescriptor* ice_table_desc = _pool.add(new IcebergTableDescriptor(tdesc, &_pool));
    tbl->get_tuple_descriptor(0)->set_table_desc(ice_table_desc);
    tbl->_tbl_desc_map[0] = ice_table_desc;

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1, false);

    TDataSink data_sink;
    data_sink.__set_type(TDataSinkType::ICEBERG_DELETE_SINK); // Set to delete sink type
    TIcebergTableSink iceberg_table_sink;
    iceberg_table_sink.__set_target_table_id(0);
    iceberg_table_sink.__set_tuple_id(0);
    iceberg_table_sink.__set_data_location("s3://bucket/table/data/");
    data_sink.iceberg_table_sink = iceberg_table_sink;

    std::vector<TExpr> exprs;
    // Add 2 expressions to match the slots
    TExpr expr1, expr2;
    exprs.push_back(expr1);
    exprs.push_back(expr2);
    IcebergTableSink sink(&_pool, exprs);
    pipeline::OpFactories prev_operators{std::make_shared<pipeline::EmptySetOperatorFactory>(1, 1)};

    EXPECT_OK(sink.decompose_to_pipeline(prev_operators, data_sink, context.get()));
}

} // namespace starrocks
