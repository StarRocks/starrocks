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

#include "connector/iceberg_delete_sink.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/column_evaluator.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"
#include "util/thrift_util.h"

namespace starrocks::connector {

class IcebergDeleteSinkTest : public ::testing::Test {
protected:
    void SetUp() override {
        _pool = std::make_unique<ObjectPool>();
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        TUniqueId fragment_id;
        fragment_id.hi = 0;
        fragment_id.lo = 0;
        _runtime_state =
                std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, ExecEnv::GetInstance());
        // Initialize mem trackers for the runtime state
        _runtime_state->init_instance_mem_tracker();

        // Create and set up a basic descriptor table for the runtime state
        // Need to ensure tuple with ID 0 exists since context->tuple_desc_id is set to 0
        TDescriptorTableBuilder desc_tbl_builder;
        // Create a tuple
        TSlotDescriptorBuilder slot_builder;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(slot_builder.type(TYPE_VARCHAR).column_name("file_path").is_materialized(true).build());
        tuple_builder.add_slot(slot_builder.type(TYPE_BIGINT).column_name("pos").is_materialized(true).build());
        tuple_builder.build(&desc_tbl_builder);
        TDescriptorTable t_desc_tbl = desc_tbl_builder.desc_tbl();
        DescriptorTbl* desc_tbl = nullptr;
        ASSERT_TRUE(DescriptorTbl::create(_runtime_state.get(), _pool.get(), t_desc_tbl, &desc_tbl,
                                          config::vector_chunk_size)
                            .ok());
        _runtime_state->set_desc_tbl(desc_tbl);

        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_runtime_state(std::move(_runtime_state));
        // Get the runtime state back from fragment context after moving
        _runtime_state = _fragment_context->runtime_state_ptr();
        _fragment_context->set_fragment_instance_id(fragment_id);
        _mem_tracker = std::make_unique<MemTracker>(MemTrackerType::QUERY_POOL, -1, "IcebergDeleteSinkTest");
    }

    std::shared_ptr<IcebergDeleteSinkContext> create_delete_sink_context() {
        auto context = std::make_shared<IcebergDeleteSinkContext>();
        context->path = "s3://test-bucket/test-table/";
        context->tuple_desc_id = 0;
        context->fragment_context = _fragment_context.get();
        context->max_file_size = 128 * 1024 * 1024;

        // Set up output expressions to match the tuple descriptor (2 slots: file_path and pos)
        TExpr file_expr;
        TExpr pos_expr;
        context->output_exprs = {file_expr, pos_expr};

        // Create mock column evaluators for file_path and pos columns
        context->column_evaluators.push_back(create_mock_column_evaluator());
        context->column_evaluators.push_back(create_mock_column_evaluator());

        // Set up column slot map for _file and _pos columns
        TExprNode file_node;
        file_node.node_type = TExprNodeType::SLOT_REF;
        file_node.__set_slot_ref(TSlotRef());
        file_node.slot_ref.slot_id = 0;
        context->column_slot_map["_file"] = file_node;

        TExprNode pos_node;
        pos_node.node_type = TExprNodeType::SLOT_REF;
        pos_node.__set_slot_ref(TSlotRef());
        pos_node.slot_ref.slot_id = 1;
        context->column_slot_map["_pos"] = pos_node;

        return context;
    }

    std::unique_ptr<ColumnEvaluator> create_mock_column_evaluator() {
        // Mock column evaluator for testing - using ColumnSlotIdEvaluator which is designed for UT
        TypeDescriptor type_desc = TYPE_VARCHAR_DESC; // Use a proper VARCHAR type descriptor for testing
        return std::make_unique<ColumnSlotIdEvaluator>(0, type_desc);
    }

    std::unique_ptr<ObjectPool> _pool;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;
    std::unique_ptr<MemTracker> _mem_tracker;
};

// Test that IcebergDeleteSinkProvider can create a valid IcebergDeleteSink
TEST_F(IcebergDeleteSinkTest, create_delete_sink) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>();

    // Should create sink successfully
    auto result = provider->create_chunk_sink(context, 0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    ASSERT_NE(sink, nullptr);

    // Verify the sink is of correct type
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);
}

// Test that IcebergDeleteSink::add handles empty chunk correctly
TEST_F(IcebergDeleteSinkTest, add_empty_chunk) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Create empty chunk
    auto chunk = std::make_shared<Chunk>();
    ASSERT_OK(delete_sink->add(chunk));
}

// Test that IcebergDeleteSink::is_finished returns true when no data was written
TEST_F(IcebergDeleteSinkTest, is_finished_no_data) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Should be finished when no data written
    ASSERT_TRUE(delete_sink->is_finished());
}

// Test file path extraction from column slot map
TEST_F(IcebergDeleteSinkTest, column_slot_map_lookup) {
    auto context = std::make_shared<IcebergDeleteSinkContext>();
    context->tuple_desc_id = 0; // Set tuple_desc_id to match the descriptor table
    // Setup output expressions for this context too
    TExpr file_expr;
    TExpr pos_expr;
    context->output_exprs = {file_expr, pos_expr};

    // Setup column slot map
    TExprNode file_node;
    file_node.node_type = TExprNodeType::SLOT_REF;
    file_node.__set_slot_ref(TSlotRef());
    file_node.slot_ref.slot_id = 5;
    context->column_slot_map["_file"] = file_node;

    TExprNode pos_node;
    pos_node.node_type = TExprNodeType::SLOT_REF;
    pos_node.__set_slot_ref(TSlotRef());
    pos_node.slot_ref.slot_id = 6;
    context->column_slot_map["_pos"] = pos_node;

    // Verify lookup works
    auto file_it = context->column_slot_map.find("_file");
    ASSERT_NE(file_it, context->column_slot_map.end());
    EXPECT_EQ(file_it->second.slot_ref.slot_id, 5);

    auto pos_it = context->column_slot_map.find("_pos");
    ASSERT_NE(pos_it, context->column_slot_map.end());
    EXPECT_EQ(pos_it->second.slot_ref.slot_id, 6);
}

// Test that context has required fields populated
TEST_F(IcebergDeleteSinkTest, context_required_fields) {
    auto context = create_delete_sink_context();

    // Verify all required fields are set
    EXPECT_FALSE(context->path.empty());
    EXPECT_GE(context->tuple_desc_id, 0);
    EXPECT_GT(context->max_file_size, 0);
    EXPECT_FALSE(context->column_evaluators.empty());
    EXPECT_EQ(context->column_slot_map.size(), 2); // _file and _pos

    // Verify required columns are present
    EXPECT_NE(context->column_slot_map.find("_file"), context->column_slot_map.end());
    EXPECT_NE(context->column_slot_map.find("_pos"), context->column_slot_map.end());
}

// Test callback_on_commit function
TEST_F(IcebergDeleteSinkTest, callback_on_commit) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Call the callback function - it should not crash
    CommitResult result_obj;
    delete_sink->callback_on_commit(result_obj);
    // Callback is currently empty (TODO), but should not crash
}

// Test IcebergDeleteSink::add with chunk containing file_path and pos data
TEST_F(IcebergDeleteSinkTest, add_with_data) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Create chunk with file_path and pos columns
    auto file_path_col = BinaryColumn::create();
    file_path_col->append(Slice("/path/to/file1.parquet"));
    file_path_col->append(Slice("/path/to/file2.parquet"));
    
    auto pos_col = Int64Column::create();
    pos_col->append(100);
    pos_col->append(200);

    Chunk chunk;
    chunk.append_column(file_path_col, 0);  // slot_id 0
    chunk.append_column(pos_col, 1);        // slot_id 1

    // Add should succeed
    ASSERT_OK(delete_sink->add(std::make_shared<Chunk>(chunk)));
}

// Test IcebergDeleteSink::finish function
TEST_F(IcebergDeleteSinkTest, finish_function) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Finish should succeed
    ASSERT_OK(delete_sink->finish());
}

// Test error condition: missing _file column in column_slot_map
TEST_F(IcebergDeleteSinkTest, missing_file_column) {
    auto context = std::make_shared<IcebergDeleteSinkContext>();
    context->tuple_desc_id = 0;
    context->fragment_context = _fragment_context.get();
    
    // Only add _pos, not _file
    TExprNode pos_node;
    pos_node.node_type = TExprNodeType::SLOT_REF;
    pos_node.__set_slot_ref(TSlotRef());
    pos_node.slot_ref.slot_id = 1;
    context->column_slot_map["_pos"] = pos_node;

    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    
    // Should fail because _file column is missing
    ASSERT_FALSE(result.ok());
    EXPECT_THAT(result.status().message(), testing::HasSubstr("Could not find _file column"));
}

// Test error condition: missing _pos column in column_slot_map
TEST_F(IcebergDeleteSinkTest, missing_pos_column) {
    auto context = std::make_shared<IcebergDeleteSinkContext>();
    context->tuple_desc_id = 0;
    context->fragment_context = _fragment_context.get();
    
    // Only add _file, not _pos
    TExprNode file_node;
    file_node.node_type = TExprNodeType::SLOT_REF;
    file_node.__set_slot_ref(TSlotRef());
    file_node.slot_ref.slot_id = 0;
    context->column_slot_map["_file"] = file_node;

    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    
    // Should fail because _pos column is missing
    ASSERT_FALSE(result.ok());
    EXPECT_THAT(result.status().message(), testing::HasSubstr("Could not find _pos column"));
}

// Test error condition: not enough column evaluators
TEST_F(IcebergDeleteSinkTest, not_enough_evaluators) {
    auto context = std::make_shared<IcebergDeleteSinkContext>();
    context->tuple_desc_id = 0;
    context->fragment_context = _fragment_context.get();
    
    // Setup required columns
    TExprNode file_node;
    file_node.node_type = TExprNodeType::SLOT_REF;
    file_node.__set_slot_ref(TSlotRef());
    file_node.slot_ref.slot_id = 0;
    context->column_slot_map["_file"] = file_node;
    
    TExprNode pos_node;
    pos_node.node_type = TExprNodeType::SLOT_REF;
    pos_node.__set_slot_ref(TSlotRef());
    pos_node.slot_ref.slot_id = 1;
    context->column_slot_map["_pos"] = pos_node;
    
    // Add only 1 evaluator instead of required 2
    context->column_evaluators.push_back(create_mock_column_evaluator());

    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    
    // Should fail because not enough evaluators
    ASSERT_FALSE(result.ok());
    EXPECT_THAT(result.status().message(), testing::HasSubstr("Not enough column evaluators"));
}

// Test error condition: invalid tuple descriptor ID
TEST_F(IcebergDeleteSinkTest, invalid_tuple_descriptor_id) {
    auto context = std::make_shared<IcebergDeleteSinkContext>();
    context->tuple_desc_id = 999; // Invalid ID
    context->fragment_context = _fragment_context.get();
    
    // Setup required columns
    TExprNode file_node;
    file_node.node_type = TExprNodeType::SLOT_REF;
    file_node.__set_slot_ref(TSlotRef());
    file_node.slot_ref.slot_id = 0;
    context->column_slot_map["_file"] = file_node;
    
    TExprNode pos_node;
    pos_node.node_type = TExprNodeType::SLOT_REF;
    pos_node.__set_slot_ref(TSlotRef());
    pos_node.slot_ref.slot_id = 1;
    context->column_slot_map["_pos"] = pos_node;
    
    // Add required evaluators
    context->column_evaluators.push_back(create_mock_column_evaluator());
    context->column_evaluators.push_back(create_mock_column_evaluator());

    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    
    // Should fail because tuple descriptor ID is invalid
    ASSERT_FALSE(result.ok());
    EXPECT_THAT(result.status().message(), testing::HasSubstr("Failed to find tuple descriptor"));
}

} // namespace starrocks::connector
