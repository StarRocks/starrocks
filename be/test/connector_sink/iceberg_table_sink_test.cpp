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

#include "exec/data_sinks/iceberg_table_sink.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "base/testutil/assert.h"
#include "common/config_exec_fwd.h"
#include "connector/builtin_connector_registry.h"
#include "connector/connector_registry.h"
#include "connector/iceberg_row_delta_sink.h"
#include "exec/pipeline/empty_set_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/sink/connector_sink_operator.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors_ext.h"
#include "runtime/exec_env.h"
#include "types/type_descriptor.h"

namespace starrocks {

class IcebergTableSinkTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>());
        _runtime_state = _fragment_context->runtime_state();
        auto* exec_env = ExecEnv::GetInstance();
        _runtime_state->set_exec_env(exec_env);
        _runtime_state->set_query_execution_services(&exec_env->query_execution_services());
        ASSERT_OK(connector::install_builtin_connectors(connector::ConnectorRegistry::default_instance()));
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

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1);

    TDataSink data_sink;
    TIcebergTableSink iceberg_table_sink;
    data_sink.iceberg_table_sink = iceberg_table_sink;

    std::vector<starrocks::TExpr> exprs = {};
    IcebergTableSink sink(&_pool, exprs);
    auto connector = connector::ConnectorRegistry::default_instance()->get(connector::Connector::ICEBERG);
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

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1);

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

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1);

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

// Test that row lineage columns are appended to column_names and parquet_field_ids
// when output expressions have more columns than the table descriptor (compaction mode)
TEST_F(IcebergTableSinkTest, row_lineage_columns_extended_during_compaction) {
    // Create tuple descriptor with 3 slots: c1 (user col) + _row_id + _last_updated_sequence_number
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot1 = slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c1").column_pos(0).nullable(true).build();
    auto slot2 = slot_desc_builder.type(LogicalType::TYPE_BIGINT)
                         .column_name("_row_id")
                         .column_pos(1)
                         .nullable(true)
                         .build();
    auto slot3 = slot_desc_builder.type(LogicalType::TYPE_BIGINT)
                         .column_name("_last_updated_sequence_number")
                         .column_pos(2)
                         .nullable(true)
                         .build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.add_slot(slot2);
    tuple_desc_builder.add_slot(slot3);
    tuple_desc_builder.build(&table_desc_builder);
    DescriptorTbl* tbl = nullptr;
    EXPECT_OK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size));
    _runtime_state->set_desc_tbl(tbl);

    // Table descriptor only has 1 column (c1) - simulates that toThrift() only sends user columns
    TIcebergTable t_iceberg_table;
    TColumn t_column;
    t_column.__set_column_name("c1");
    t_iceberg_table.__set_columns({t_column});

    // Iceberg schema also only has 1 field (c1)
    TIcebergSchemaField schema_field;
    schema_field.__set_field_id(1);
    schema_field.__set_name("c1");
    TIcebergSchema iceberg_schema;
    iceberg_schema.__set_fields({schema_field});
    t_iceberg_table.__set_iceberg_schema(iceberg_schema);

    TTableDescriptor tdesc;
    tdesc.__set_icebergTable(t_iceberg_table);

    IcebergTableDescriptor* ice_table_desc = _pool.add(new IcebergTableDescriptor(tdesc, &_pool));
    tbl->get_tuple_descriptor(0)->set_table_desc(ice_table_desc);
    tbl->_tbl_desc_map[0] = ice_table_desc;

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1);

    TDataSink data_sink;
    TIcebergTableSink iceberg_table_sink;
    iceberg_table_sink.__set_location("s3://bucket/table-location");
    data_sink.__set_iceberg_table_sink(iceberg_table_sink);

    // 3 output expressions (c1 + _row_id + _last_updated_sequence_number)
    std::vector<TExpr> exprs;
    TExpr expr1, expr2, expr3;
    exprs.push_back(expr1);
    exprs.push_back(expr2);
    exprs.push_back(expr3);
    IcebergTableSink sink(&_pool, exprs);
    pipeline::OpFactories prev_operators{std::make_shared<pipeline::EmptySetOperatorFactory>(1, 1)};

    EXPECT_OK(sink.decompose_to_pipeline(prev_operators, data_sink, context.get()));

    pipeline::Pipeline* pl = const_cast<pipeline::Pipeline*>(context->last_pipeline());
    pipeline::OperatorFactory* op_factory = pl->sink_operator_factory();
    auto connector_sink_factory = dynamic_cast<pipeline::ConnectorSinkOperatorFactory*>(op_factory);
    auto sink_ctx = dynamic_cast<connector::IcebergChunkSinkContext*>(connector_sink_factory->_sink_context.get());

    // Verify column_names was extended with row lineage columns
    ASSERT_EQ(sink_ctx->column_names.size(), 3);
    EXPECT_EQ(sink_ctx->column_names[0], "c1");
    EXPECT_EQ(sink_ctx->column_names[1], "_row_id");
    EXPECT_EQ(sink_ctx->column_names[2], "_last_updated_sequence_number");

    // Verify parquet_field_ids was extended with correct Iceberg reserved field IDs
    ASSERT_EQ(sink_ctx->parquet_field_ids.size(), 3);
    EXPECT_EQ(sink_ctx->parquet_field_ids[0].field_id, 1);          // c1's field_id
    EXPECT_EQ(sink_ctx->parquet_field_ids[1].field_id, 2147483540); // _row_id reserved field ID
    EXPECT_EQ(sink_ctx->parquet_field_ids[2].field_id, 2147483539); // _last_updated_sequence_number reserved field ID
}

// Test that parquet_field_ids is extended even when column_names already includes
// row lineage columns but iceberg_schema still only contains user columns.
TEST_F(IcebergTableSinkTest, row_lineage_field_ids_extended_when_column_names_already_present) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot1 = slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c1").column_pos(0).nullable(true).build();
    auto slot2 = slot_desc_builder.type(LogicalType::TYPE_BIGINT)
                         .column_name("_row_id")
                         .column_pos(1)
                         .nullable(true)
                         .build();
    auto slot3 = slot_desc_builder.type(LogicalType::TYPE_BIGINT)
                         .column_name("_last_updated_sequence_number")
                         .column_pos(2)
                         .nullable(true)
                         .build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.add_slot(slot2);
    tuple_desc_builder.add_slot(slot3);
    tuple_desc_builder.build(&table_desc_builder);
    DescriptorTbl* tbl = nullptr;
    EXPECT_OK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size));
    _runtime_state->set_desc_tbl(tbl);

    // Simulate FE already including row lineage columns in TIcebergTable.columns,
    // while Iceberg schema still only describes user columns.
    TIcebergTable t_iceberg_table;
    TColumn col1;
    col1.__set_column_name("c1");
    TColumn col2;
    col2.__set_column_name("_row_id");
    TColumn col3;
    col3.__set_column_name("_last_updated_sequence_number");
    t_iceberg_table.__set_columns({col1, col2, col3});

    TIcebergSchemaField schema_field;
    schema_field.__set_field_id(1);
    schema_field.__set_name("c1");
    TIcebergSchema iceberg_schema;
    iceberg_schema.__set_fields({schema_field});
    t_iceberg_table.__set_iceberg_schema(iceberg_schema);

    TTableDescriptor tdesc;
    tdesc.__set_icebergTable(t_iceberg_table);

    IcebergTableDescriptor* ice_table_desc = _pool.add(new IcebergTableDescriptor(tdesc, &_pool));
    tbl->get_tuple_descriptor(0)->set_table_desc(ice_table_desc);
    tbl->_tbl_desc_map[0] = ice_table_desc;

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1);

    TDataSink data_sink;
    TIcebergTableSink iceberg_table_sink;
    iceberg_table_sink.__set_location("s3://bucket/table-location");
    data_sink.__set_iceberg_table_sink(iceberg_table_sink);

    std::vector<TExpr> exprs;
    TExpr expr1, expr2, expr3;
    exprs.push_back(expr1);
    exprs.push_back(expr2);
    exprs.push_back(expr3);
    IcebergTableSink sink(&_pool, exprs);
    pipeline::OpFactories prev_operators{std::make_shared<pipeline::EmptySetOperatorFactory>(1, 1)};

    EXPECT_OK(sink.decompose_to_pipeline(prev_operators, data_sink, context.get()));

    pipeline::Pipeline* pl = const_cast<pipeline::Pipeline*>(context->last_pipeline());
    pipeline::OperatorFactory* op_factory = pl->sink_operator_factory();
    auto connector_sink_factory = dynamic_cast<pipeline::ConnectorSinkOperatorFactory*>(op_factory);
    auto sink_ctx = dynamic_cast<connector::IcebergChunkSinkContext*>(connector_sink_factory->_sink_context.get());

    ASSERT_EQ(sink_ctx->column_names.size(), 3);
    ASSERT_EQ(sink_ctx->parquet_field_ids.size(), 3);
    EXPECT_EQ(sink_ctx->parquet_field_ids[0].field_id, 1);
    EXPECT_EQ(sink_ctx->parquet_field_ids[1].field_id, 2147483540);
    EXPECT_EQ(sink_ctx->parquet_field_ids[2].field_id, 2147483539);
}

// Test that sink schema is rebuilt from tuple/output columns when TIcebergTable.columns
// still contains hidden metadata columns that are not written by compaction.
TEST_F(IcebergTableSinkTest, row_lineage_field_ids_ignore_non_written_hidden_columns) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot1 = slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c1").column_pos(0).nullable(true).build();
    auto slot2 = slot_desc_builder.type(LogicalType::TYPE_BIGINT)
                         .column_name("_row_id")
                         .column_pos(1)
                         .nullable(true)
                         .build();
    auto slot3 = slot_desc_builder.type(LogicalType::TYPE_BIGINT)
                         .column_name("_last_updated_sequence_number")
                         .column_pos(2)
                         .nullable(true)
                         .build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.add_slot(slot2);
    tuple_desc_builder.add_slot(slot3);
    tuple_desc_builder.build(&table_desc_builder);
    DescriptorTbl* tbl = nullptr;
    EXPECT_OK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size));
    _runtime_state->set_desc_tbl(tbl);

    // Simulate current FE thrift table schema ordering:
    // user columns first, then hidden metadata columns such as _file/_pos, then row lineage columns.
    TIcebergTable t_iceberg_table;
    TColumn col1;
    col1.__set_column_name("c1");
    TColumn col2;
    col2.__set_column_name("_file");
    TColumn col3;
    col3.__set_column_name("_pos");
    TColumn col4;
    col4.__set_column_name("_row_id");
    TColumn col5;
    col5.__set_column_name("_last_updated_sequence_number");
    t_iceberg_table.__set_columns({col1, col2, col3, col4, col5});

    TIcebergSchemaField schema_field;
    schema_field.__set_field_id(1);
    schema_field.__set_name("c1");
    TIcebergSchema iceberg_schema;
    iceberg_schema.__set_fields({schema_field});
    t_iceberg_table.__set_iceberg_schema(iceberg_schema);

    TTableDescriptor tdesc;
    tdesc.__set_icebergTable(t_iceberg_table);

    IcebergTableDescriptor* ice_table_desc = _pool.add(new IcebergTableDescriptor(tdesc, &_pool));
    tbl->get_tuple_descriptor(0)->set_table_desc(ice_table_desc);
    tbl->_tbl_desc_map[0] = ice_table_desc;

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1);

    TDataSink data_sink;
    TIcebergTableSink iceberg_table_sink;
    iceberg_table_sink.__set_location("s3://bucket/table-location");
    data_sink.__set_iceberg_table_sink(iceberg_table_sink);

    std::vector<TExpr> exprs;
    TExpr expr1, expr2, expr3;
    exprs.push_back(expr1);
    exprs.push_back(expr2);
    exprs.push_back(expr3);
    IcebergTableSink sink(&_pool, exprs);
    pipeline::OpFactories prev_operators{std::make_shared<pipeline::EmptySetOperatorFactory>(1, 1)};

    EXPECT_OK(sink.decompose_to_pipeline(prev_operators, data_sink, context.get()));

    pipeline::Pipeline* pl = const_cast<pipeline::Pipeline*>(context->last_pipeline());
    pipeline::OperatorFactory* op_factory = pl->sink_operator_factory();
    auto connector_sink_factory = dynamic_cast<pipeline::ConnectorSinkOperatorFactory*>(op_factory);
    auto sink_ctx = dynamic_cast<connector::IcebergChunkSinkContext*>(connector_sink_factory->_sink_context.get());

    ASSERT_EQ(sink_ctx->column_names.size(), 3);
    EXPECT_EQ(sink_ctx->column_names[0], "c1");
    EXPECT_EQ(sink_ctx->column_names[1], "_row_id");
    EXPECT_EQ(sink_ctx->column_names[2], "_last_updated_sequence_number");

    ASSERT_EQ(sink_ctx->parquet_field_ids.size(), 3);
    EXPECT_EQ(sink_ctx->parquet_field_ids[0].field_id, 1);
    EXPECT_EQ(sink_ctx->parquet_field_ids[1].field_id, 2147483540);
    EXPECT_EQ(sink_ctx->parquet_field_ids[2].field_id, 2147483539);
}

namespace {
// Build a minimal SLOT_REF TExpr referring to slot_id in tuple 0.
// from_exprs() / column_slot_map population only inspect node_type and slot_ref,
// so this is enough to exercise create_row_delta_sink_context without setting
// up full type descriptors.
TExpr make_slot_ref_expr(int slot_id) {
    TExpr expr;
    TExprNode node;
    node.node_type = TExprNodeType::SLOT_REF;
    node.__set_slot_ref(TSlotRef());
    node.slot_ref.slot_id = slot_id;
    node.slot_ref.tuple_id = 0;
    expr.nodes.push_back(node);
    return expr;
}
} // namespace

// Drives create_row_delta_sink_context() end-to-end via decompose_to_pipeline,
// then exercises IcebergRowDeltaSinkProvider::create_chunk_sink() on the
// resulting context. Covers:
//   - the row-delta dispatch branch in decompose_to_pipeline
//   - IcebergConnector::create_row_delta_sink_provider()
//   - the bulk of create_row_delta_sink_context() (delete sub-context, data
//     sub-context, override_tuple_desc, op_code_index, and the unpartitioned
//     branch)
//   - IcebergRowDeltaSinkProvider::create_chunk_sink() success path, which in
//     turn drives IcebergDeleteSinkProvider and IcebergChunkSinkProvider
TEST_F(IcebergTableSinkTest, decompose_to_pipeline_row_delta) {
    // Tuple layout for a row-delta write: [_file, _pos, c1, op_code]
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto file_slot = slot_desc_builder.type(LogicalType::TYPE_VARCHAR)
                             .column_name("_file")
                             .column_pos(0)
                             .nullable(false)
                             .build();
    auto pos_slot =
            slot_desc_builder.type(LogicalType::TYPE_BIGINT).column_name("_pos").column_pos(1).nullable(false).build();
    auto c1_slot = slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c1").column_pos(2).nullable(true).build();
    auto op_slot = slot_desc_builder.type(LogicalType::TYPE_TINYINT)
                           .column_name("op_code")
                           .column_pos(3)
                           .nullable(false)
                           .build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(file_slot);
    tuple_desc_builder.add_slot(pos_slot);
    tuple_desc_builder.add_slot(c1_slot);
    tuple_desc_builder.add_slot(op_slot);
    tuple_desc_builder.build(&table_desc_builder);
    DescriptorTbl* tbl = nullptr;
    EXPECT_OK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size));
    _runtime_state->set_desc_tbl(tbl);

    // Iceberg schema must include the data column ("c1") so
    // build_top_level_field_id_map() can resolve its parquet_field_id.
    TIcebergTable t_iceberg_table;
    TColumn t_column;
    t_column.__set_column_name("c1");
    t_iceberg_table.__set_columns({t_column});

    TIcebergSchemaField c1_field;
    c1_field.__set_field_id(1);
    c1_field.__set_name("c1");
    TIcebergSchema iceberg_schema;
    iceberg_schema.__set_fields({c1_field});
    t_iceberg_table.__set_iceberg_schema(iceberg_schema);

    TTableDescriptor tdesc;
    tdesc.__set_icebergTable(t_iceberg_table);
    IcebergTableDescriptor* ice_table_desc = _pool.add(new IcebergTableDescriptor(tdesc, &_pool));
    tbl->get_tuple_descriptor(0)->set_table_desc(ice_table_desc);
    tbl->_tbl_desc_map[0] = ice_table_desc;

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1);

    TDataSink data_sink;
    data_sink.__set_type(TDataSinkType::ICEBERG_ROW_DELTA_SINK);
    TIcebergTableSink iceberg_table_sink;
    iceberg_table_sink.__set_location("/path/to/table");
    iceberg_table_sink.__set_data_location("/path/to/table/data");
    iceberg_table_sink.__set_tuple_id(0);
    iceberg_table_sink.__set_target_max_file_size(128LL * 1024 * 1024);
    data_sink.__set_iceberg_table_sink(iceberg_table_sink);

    std::vector<TExpr> exprs = {make_slot_ref_expr(0), make_slot_ref_expr(1), make_slot_ref_expr(2),
                                make_slot_ref_expr(3)};

    IcebergTableSink sink(&_pool, exprs);
    pipeline::OpFactories prev_operators{std::make_shared<pipeline::EmptySetOperatorFactory>(11, 11)};

    EXPECT_OK(sink.decompose_to_pipeline(prev_operators, data_sink, context.get()));

    pipeline::Pipeline* pl = const_cast<pipeline::Pipeline*>(context->last_pipeline());
    pipeline::OperatorFactory* op_factory = pl->sink_operator_factory();
    auto connector_sink_factory = dynamic_cast<pipeline::ConnectorSinkOperatorFactory*>(op_factory);
    ASSERT_NE(connector_sink_factory, nullptr);

    auto* row_delta_ctx =
            dynamic_cast<connector::IcebergRowDeltaSinkContext*>(connector_sink_factory->_sink_context.get());
    ASSERT_NE(row_delta_ctx, nullptr);
    ASSERT_NE(row_delta_ctx->delete_sink_ctx, nullptr);
    ASSERT_NE(row_delta_ctx->data_sink_ctx, nullptr);
    EXPECT_EQ(row_delta_ctx->op_code_index, 3);

    // delete sub-context: tagged "delete", carries the full output_exprs (one
    // per slot) so IcebergDeleteSinkProvider's strict 1:1 DCHECK holds, and
    // its column_slot_map covers every non-op_code slot (here _file, _pos, c1).
    EXPECT_EQ(row_delta_ctx->delete_sink_ctx->writer_tag, "delete");
    EXPECT_EQ(row_delta_ctx->delete_sink_ctx->output_exprs.size(), 4);
    EXPECT_EQ(row_delta_ctx->delete_sink_ctx->column_slot_map.size(), 3);
    EXPECT_TRUE(row_delta_ctx->delete_sink_ctx->column_slot_map.count("_file") == 1);
    EXPECT_TRUE(row_delta_ctx->delete_sink_ctx->column_slot_map.count("_pos") == 1);

    // data sub-context: tagged "data", path resolves to data_location when set,
    // and override_tuple_desc has only data columns (op_code excluded).
    EXPECT_EQ(row_delta_ctx->data_sink_ctx->writer_tag, "data");
    EXPECT_EQ(row_delta_ctx->data_sink_ctx->path, "/path/to/table/data");
    ASSERT_NE(row_delta_ctx->data_sink_ctx->override_tuple_desc, nullptr);
    EXPECT_EQ(row_delta_ctx->data_sink_ctx->override_tuple_desc->slots().size(), 1);
    EXPECT_EQ(row_delta_ctx->data_sink_ctx->column_names.size(), 1);
    EXPECT_EQ(row_delta_ctx->data_sink_ctx->column_names[0], "c1");
    EXPECT_EQ(row_delta_ctx->data_sink_ctx->parquet_field_ids[0].field_id, 1);

    // Now drive IcebergRowDeltaSinkProvider::create_chunk_sink() success path.
    connector::IcebergRowDeltaSinkProvider provider;
    auto sink_or = provider.create_chunk_sink(connector_sink_factory->_sink_context, /*driver_id=*/0);
    ASSERT_OK(sink_or.status());
    auto created_sink = std::move(sink_or).value();
    ASSERT_NE(created_sink, nullptr);
    EXPECT_NE(dynamic_cast<connector::IcebergRowDeltaSink*>(created_sink.get()), nullptr);
}

// Tuple too short to be a row-delta layout. With only [_file, _pos]
// the layout invariant `data_column_count >= 0` fails and the function
// returns InternalError without touching downstream sub-context setup.
TEST_F(IcebergTableSinkTest, row_delta_invalid_column_layout) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto file_slot = slot_desc_builder.type(LogicalType::TYPE_VARCHAR)
                             .column_name("_file")
                             .column_pos(0)
                             .nullable(false)
                             .build();
    auto pos_slot =
            slot_desc_builder.type(LogicalType::TYPE_BIGINT).column_name("_pos").column_pos(1).nullable(false).build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(file_slot);
    tuple_desc_builder.add_slot(pos_slot);
    tuple_desc_builder.build(&table_desc_builder);
    DescriptorTbl* tbl = nullptr;
    EXPECT_OK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size));
    _runtime_state->set_desc_tbl(tbl);

    TIcebergTable t_iceberg_table;
    TIcebergSchema iceberg_schema;
    t_iceberg_table.__set_iceberg_schema(iceberg_schema);
    TTableDescriptor tdesc;
    tdesc.__set_icebergTable(t_iceberg_table);
    IcebergTableDescriptor* ice_table_desc = _pool.add(new IcebergTableDescriptor(tdesc, &_pool));
    tbl->get_tuple_descriptor(0)->set_table_desc(ice_table_desc);
    tbl->_tbl_desc_map[0] = ice_table_desc;

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1);

    TDataSink data_sink;
    data_sink.__set_type(TDataSinkType::ICEBERG_ROW_DELTA_SINK);
    TIcebergTableSink iceberg_table_sink;
    iceberg_table_sink.__set_location("/path/to/table");
    iceberg_table_sink.__set_tuple_id(0);
    data_sink.__set_iceberg_table_sink(iceberg_table_sink);

    std::vector<TExpr> exprs = {make_slot_ref_expr(0), make_slot_ref_expr(1)};

    IcebergTableSink sink(&_pool, exprs);
    pipeline::OpFactories prev_operators{std::make_shared<pipeline::EmptySetOperatorFactory>(12, 12)};

    auto status = sink.decompose_to_pipeline(prev_operators, data_sink, context.get());
    EXPECT_FALSE(status.ok());
    EXPECT_THAT(std::string(status.message()), testing::HasSubstr("Invalid row delta column layout"));
}

} // namespace starrocks
