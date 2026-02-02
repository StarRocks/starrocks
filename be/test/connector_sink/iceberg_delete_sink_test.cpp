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

#include "base/testutil/assert.h"
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

// Test callback_on_commit function with empty result
TEST_F(IcebergDeleteSinkTest, callback_on_commit_empty) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Call the callback function with empty result
    CommitResult result_obj;
    result_obj.location = "/test/path/delete_file.parquet"; // Provide a valid location to avoid PathUtils failure

    // Get the runtime state before the callback to check sink commit info
    auto runtime_state = _fragment_context->runtime_state_ptr();
    size_t initial_sink_commit_info_count = runtime_state->sink_commit_infos().size();

    delete_sink->callback_on_commit(result_obj);

    // Should not crash and should handle empty result gracefully
    // Since io_status is OK by default, it should add a commit info
    ASSERT_EQ(runtime_state->sink_commit_infos().size(), initial_sink_commit_info_count + 1);

    // Verify the last added sink commit info
    const auto& last_commit_info = runtime_state->sink_commit_infos().back();
    ASSERT_TRUE(last_commit_info.__isset.iceberg_data_file);
    const auto& iceberg_data_file = last_commit_info.iceberg_data_file;
    EXPECT_EQ(iceberg_data_file.path, result_obj.location);
    EXPECT_EQ(iceberg_data_file.file_content, TIcebergFileContent::POSITION_DELETES);
}

// Test callback_on_commit function with complete delete file information
TEST_F(IcebergDeleteSinkTest, callback_on_commit_with_delete_file) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Prepare a complete CommitResult with delete file information
    CommitResult result_obj;
    result_obj.io_status = Status::OK();
    result_obj.location = "/test/path/delete_file.parquet";
    result_obj.format = "parquet";
    result_obj.file_statistics.record_count = 100;
    result_obj.file_statistics.file_size = 1024 * 1024;
    result_obj.extra_data = "partition_null_fingerprint";
    result_obj.referenced_data_file = "s3://test-bucket/test-table/data/data_file.parquet";

    // Set column statistics
    result_obj.file_statistics.column_sizes = {{1, 1000}, {2, 2000}};
    result_obj.file_statistics.value_counts = {{1, 100}, {2, 100}};
    result_obj.file_statistics.null_value_counts = {{1, 0}, {2, 10}};
    result_obj.file_statistics.lower_bounds = {{1, "min_val"}, {2, "min_val2"}};
    result_obj.file_statistics.upper_bounds = {{1, "max_val"}, {2, "max_val2"}};

    // Get the runtime state before the callback to check sink commit info
    auto runtime_state = _fragment_context->runtime_state_ptr();
    size_t initial_sink_commit_info_count = runtime_state->sink_commit_infos().size();

    // Call the callback function
    delete_sink->callback_on_commit(result_obj);

    // Verify that the sink processed the commit result correctly
    // The callback should have updated the runtime state with sink commit info
    ASSERT_EQ(runtime_state->sink_commit_infos().size(), initial_sink_commit_info_count + 1);

    // Verify the last added sink commit info has the expected iceberg data file
    const auto& last_commit_info = runtime_state->sink_commit_infos().back();
    ASSERT_TRUE(last_commit_info.__isset.iceberg_data_file);
    const auto& iceberg_data_file = last_commit_info.iceberg_data_file;
    EXPECT_EQ(iceberg_data_file.path, result_obj.location);
    EXPECT_EQ(iceberg_data_file.format, result_obj.format);
    EXPECT_EQ(iceberg_data_file.record_count, result_obj.file_statistics.record_count);
    EXPECT_EQ(iceberg_data_file.file_size_in_bytes, result_obj.file_statistics.file_size);
    EXPECT_EQ(iceberg_data_file.partition_null_fingerprint, result_obj.extra_data);
    EXPECT_EQ(iceberg_data_file.referenced_data_file, result_obj.referenced_data_file);
    EXPECT_EQ(iceberg_data_file.file_content, TIcebergFileContent::POSITION_DELETES);
}

// Test callback_on_commit function with IO failure
TEST_F(IcebergDeleteSinkTest, callback_on_commit_io_failure) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Prepare a CommitResult with IO failure
    CommitResult result_obj;
    result_obj.io_status = Status::IOError("Write failed");
    result_obj.location = "/test/path/delete_file.parquet";

    // Get the runtime state before the callback to check sink commit info
    auto runtime_state = _fragment_context->runtime_state_ptr();
    size_t initial_sink_commit_info_count = runtime_state->sink_commit_infos().size();

    // Call the callback function - it should handle the failure gracefully
    delete_sink->callback_on_commit(result_obj);

    // Should not crash even with IO failure
    // Since io_status is not OK, no commit info should be added
    ASSERT_EQ(runtime_state->sink_commit_infos().size(), initial_sink_commit_info_count);
}

// Test callback_on_commit function with partial statistics
TEST_F(IcebergDeleteSinkTest, callback_on_commit_partial_stats) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Prepare a CommitResult with partial statistics
    CommitResult result_obj;
    result_obj.io_status = Status::OK();
    result_obj.location = "/test/path/delete_file.parquet";
    result_obj.format = "parquet";
    result_obj.file_statistics.record_count = 50;
    result_obj.file_statistics.file_size = 512 * 1024;
    // Only set some statistics, not all
    result_obj.file_statistics.value_counts = std::map<int, int64_t>{{1, 50}};
    result_obj.file_statistics.null_value_counts = std::map<int, int64_t>{{1, 5}};
    // Lower and upper bounds not set

    // Get the runtime state before the callback to check sink commit info
    auto runtime_state = _fragment_context->runtime_state_ptr();
    size_t initial_sink_commit_info_count = runtime_state->sink_commit_infos().size();

    // Call the callback function
    delete_sink->callback_on_commit(result_obj);

    // Should handle partial statistics correctly
    ASSERT_EQ(runtime_state->sink_commit_infos().size(), initial_sink_commit_info_count + 1);

    // Verify the last added sink commit info has the expected iceberg data file
    const auto& last_commit_info = runtime_state->sink_commit_infos().back();
    ASSERT_TRUE(last_commit_info.__isset.iceberg_data_file);
    const auto& iceberg_data_file = last_commit_info.iceberg_data_file;
    EXPECT_EQ(iceberg_data_file.path, result_obj.location);
    EXPECT_EQ(iceberg_data_file.format, result_obj.format);
    EXPECT_EQ(iceberg_data_file.record_count, result_obj.file_statistics.record_count);
    EXPECT_EQ(iceberg_data_file.file_size_in_bytes, result_obj.file_statistics.file_size);
    EXPECT_EQ(iceberg_data_file.file_content, TIcebergFileContent::POSITION_DELETES);
}

// Test CommitResult set_referenced_data_file method
TEST_F(IcebergDeleteSinkTest, commit_result_set_referenced_data_file) {
    CommitResult result_obj;

    // Test setting referenced data file
    std::string referenced_file = "s3://test-bucket/test-table/data/data_file_001.parquet";
    result_obj.set_referenced_data_file(referenced_file);

    EXPECT_EQ(result_obj.referenced_data_file, referenced_file);

    // Test chaining
    std::string referenced_file2 = "s3://test-bucket/test-table/data/data_file_002.parquet";
    auto& result_ref = result_obj.set_referenced_data_file(referenced_file2);
    EXPECT_EQ(&result_ref, &result_obj); // Should return self reference
    EXPECT_EQ(result_obj.referenced_data_file, referenced_file2);
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

    // Setup output expressions to match tuple descriptor (2 slots)
    TExpr file_expr, pos_expr;
    context->output_exprs = {file_expr, pos_expr};

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
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("Could not find _file column"));
}

// Test error condition: missing _pos column in column_slot_map
TEST_F(IcebergDeleteSinkTest, missing_pos_column) {
    auto context = std::make_shared<IcebergDeleteSinkContext>();
    context->tuple_desc_id = 0;
    context->fragment_context = _fragment_context.get();

    // Setup output expressions to match tuple descriptor (2 slots)
    TExpr file_expr, pos_expr;
    context->output_exprs = {file_expr, pos_expr};

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
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("Could not find _pos column"));
}

// Test error condition: not enough column evaluators
TEST_F(IcebergDeleteSinkTest, not_enough_evaluators) {
    auto context = std::make_shared<IcebergDeleteSinkContext>();
    context->tuple_desc_id = 0;
    context->fragment_context = _fragment_context.get();

    // Setup output expressions to match tuple descriptor (2 slots)
    TExpr file_expr, pos_expr;
    context->output_exprs = {file_expr, pos_expr};

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
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("Not enough column evaluators"));
}

// Test error condition: invalid tuple descriptor ID
TEST_F(IcebergDeleteSinkTest, invalid_tuple_descriptor_id) {
    auto context = std::make_shared<IcebergDeleteSinkContext>();
    context->tuple_desc_id = 999; // Invalid ID
    context->fragment_context = _fragment_context.get();

    // Setup output expressions to match tuple descriptor (2 slots)
    TExpr file_expr, pos_expr;
    context->output_exprs = {file_expr, pos_expr};

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
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("Failed to find tuple descriptor"));
}

// Test that Iceberg delete file has REQUIRED columns for file_path and pos
TEST_F(IcebergDeleteSinkTest, verify_required_columns_in_parquet) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>();
    auto result = provider->create_chunk_sink(context, 0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Verify that the context has the correct tuple descriptor
    auto tuple_desc = _runtime_state->desc_tbl().get_tuple_descriptor(context->tuple_desc_id);
    ASSERT_NE(tuple_desc, nullptr);
    ASSERT_EQ(tuple_desc->slots().size(), 2);

    // Verify that file_path and pos columns are not nullable
    // This is what determines whether Parquet columns will be REQUIRED
    for (auto& slot : tuple_desc->slots()) {
        if (slot->col_name() == "file_path" || slot->col_name() == "pos") {
            EXPECT_FALSE(slot->is_nullable())
                    << "Column " << slot->col_name() << " should be NOT NULL for Iceberg delete files";
        }
    }
}

} // namespace starrocks::connector
