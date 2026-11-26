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

#include "connector/iceberg/iceberg_delete_sink.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/status.h"
#include "common/util/thrift_util.h"
#include "formats/column_evaluator.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"
#include "testutil/column_test_helper.h"
#include "types/datum.h"
#include "types/type_descriptor.h"

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
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals,
                                                        static_cast<ExecEnv*>(nullptr));
        _query_execution_services.runtime = &_runtime_services;
        _runtime_state->set_query_execution_services(&_query_execution_services);
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

        _mem_tracker = std::make_unique<MemTracker>(MemTrackerType::QUERY_POOL, -1, "IcebergDeleteSinkTest");
        _test_path = std::filesystem::temp_directory_path() / "iceberg_delete_sink_test";
        std::filesystem::remove_all(_test_path);
        std::filesystem::create_directories(_test_path);
    }

    void TearDown() override { std::filesystem::remove_all(_test_path); }

    std::shared_ptr<IcebergDeleteSinkContext> create_delete_sink_context() {
        auto context = std::make_shared<IcebergDeleteSinkContext>();
        context->path = _test_path.string() + "/";
        context->tuple_desc_id = 0;
        context->runtime_state = _runtime_state.get();
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
    RuntimeServices _runtime_services;
    QueryExecutionServices _query_execution_services;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::filesystem::path _test_path;
};

// Test that IcebergDeleteSinkProvider can create a valid IcebergDeleteSink
TEST_F(IcebergDeleteSinkTest, create_delete_sink) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);

    // Should create sink successfully
    auto result = provider->create_sink(0);
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
    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);
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
    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);
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
    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Call the callback function with empty result
    CommitResult result_obj;
    // Provide a valid location to avoid PathUtils failure.
    result_obj.file_result.location = "/test/path/delete_file.parquet";

    // Get the runtime state before the callback to check sink commit info
    auto runtime_state = _runtime_state;
    size_t initial_sink_commit_info_count = runtime_state->sink_commit_infos().size();

    delete_sink->callback_on_commit(result_obj);

    // Should not crash and should handle empty result gracefully
    // Since io_status is OK by default, it should add a commit info
    ASSERT_EQ(runtime_state->sink_commit_infos().size(), initial_sink_commit_info_count + 1);

    // Verify the last added sink commit info
    const auto& last_commit_info = runtime_state->sink_commit_infos().back();
    ASSERT_TRUE(last_commit_info.__isset.iceberg_data_file);
    const auto& iceberg_data_file = last_commit_info.iceberg_data_file;
    EXPECT_EQ(iceberg_data_file.path, result_obj.file_result.location);
    EXPECT_EQ(iceberg_data_file.file_content, TIcebergFileContent::POSITION_DELETES);
}

// Test callback_on_commit function with complete delete file information
TEST_F(IcebergDeleteSinkTest, callback_on_commit_with_delete_file) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Prepare a complete CommitResult with delete file information
    CommitResult result_obj;
    result_obj.file_result.io_status = Status::OK();
    result_obj.file_result.location = "/test/path/delete_file.parquet";
    result_obj.file_result.format = "parquet";
    result_obj.file_result.file_statistics.record_count = 100;
    result_obj.file_result.file_statistics.file_size = 1024 * 1024;
    result_obj.partition_null_fingerprint = "partition_null_fingerprint";
    result_obj.referenced_data_file = "s3://test-bucket/test-table/data/data_file.parquet";

    // Set column statistics
    result_obj.file_result.file_statistics.column_sizes = {{1, 1000}, {2, 2000}};
    result_obj.file_result.file_statistics.value_counts = {{1, 100}, {2, 100}};
    result_obj.file_result.file_statistics.null_value_counts = {{1, 0}, {2, 10}};
    result_obj.file_result.file_statistics.lower_bounds = {{1, "min_val"}, {2, "min_val2"}};
    result_obj.file_result.file_statistics.upper_bounds = {{1, "max_val"}, {2, "max_val2"}};

    // Get the runtime state before the callback to check sink commit info
    auto runtime_state = _runtime_state;
    size_t initial_sink_commit_info_count = runtime_state->sink_commit_infos().size();

    // Call the callback function
    delete_sink->callback_on_commit(result_obj);

    // Verify that the sink processed the commit result correctly
    // The callback should have updated the runtime state with sink commit info
    ASSERT_EQ(runtime_state->sink_commit_infos().size(), initial_sink_commit_info_count + 1);
    EXPECT_EQ(runtime_state->num_rows_load_sink(), 100);

    // Verify the last added sink commit info has the expected iceberg data file
    const auto& last_commit_info = runtime_state->sink_commit_infos().back();
    ASSERT_TRUE(last_commit_info.__isset.iceberg_data_file);
    const auto& iceberg_data_file = last_commit_info.iceberg_data_file;
    EXPECT_EQ(iceberg_data_file.path, result_obj.file_result.location);
    EXPECT_EQ(iceberg_data_file.format, result_obj.file_result.format);
    EXPECT_EQ(iceberg_data_file.record_count, result_obj.file_result.file_statistics.record_count);
    EXPECT_EQ(iceberg_data_file.file_size_in_bytes, result_obj.file_result.file_statistics.file_size);
    EXPECT_EQ(iceberg_data_file.partition_null_fingerprint, result_obj.partition_null_fingerprint);
    EXPECT_EQ(iceberg_data_file.referenced_data_file, result_obj.referenced_data_file);
    EXPECT_EQ(iceberg_data_file.file_content, TIcebergFileContent::POSITION_DELETES);
}

// A row-delta delete sub-sink still reports its file but leaves row counting to the composite sink.
TEST_F(IcebergDeleteSinkTest, callback_on_commit_without_row_count) {
    auto context = create_delete_sink_context();
    context->count_loaded_rows = false;
    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);
    ASSERT_TRUE(result.ok());
    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    CommitResult result_obj;
    result_obj.file_result.io_status = Status::OK();
    result_obj.file_result.location = "/test/path/delete_file.parquet";
    result_obj.file_result.format = "parquet";
    result_obj.file_result.file_statistics.record_count = 100;
    delete_sink->callback_on_commit(result_obj);

    EXPECT_EQ(_runtime_state->sink_commit_infos().size(), 1u);
    EXPECT_EQ(_runtime_state->num_rows_load_sink(), 0);
}

// Test callback_on_commit function with IO failure
TEST_F(IcebergDeleteSinkTest, callback_on_commit_io_failure) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Prepare a CommitResult with IO failure
    CommitResult result_obj;
    result_obj.file_result.io_status = Status::IOError("Write failed");
    result_obj.file_result.location = "/test/path/delete_file.parquet";

    // Get the runtime state before the callback to check sink commit info
    auto runtime_state = _runtime_state;
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
    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);
    ASSERT_TRUE(result.ok());

    auto sink = std::move(result).value();
    auto delete_sink = dynamic_cast<IcebergDeleteSink*>(sink.get());
    ASSERT_NE(delete_sink, nullptr);

    // Prepare a CommitResult with partial statistics
    CommitResult result_obj;
    result_obj.file_result.io_status = Status::OK();
    result_obj.file_result.location = "/test/path/delete_file.parquet";
    result_obj.file_result.format = "parquet";
    result_obj.file_result.file_statistics.record_count = 50;
    result_obj.file_result.file_statistics.file_size = 512 * 1024;
    // Only set some statistics, not all
    result_obj.file_result.file_statistics.value_counts = std::map<int, int64_t>{{1, 50}};
    result_obj.file_result.file_statistics.null_value_counts = std::map<int, int64_t>{{1, 5}};
    // Lower and upper bounds not set

    // Get the runtime state before the callback to check sink commit info
    auto runtime_state = _runtime_state;
    size_t initial_sink_commit_info_count = runtime_state->sink_commit_infos().size();

    // Call the callback function
    delete_sink->callback_on_commit(result_obj);

    // Should handle partial statistics correctly
    ASSERT_EQ(runtime_state->sink_commit_infos().size(), initial_sink_commit_info_count + 1);

    // Verify the last added sink commit info has the expected iceberg data file
    const auto& last_commit_info = runtime_state->sink_commit_infos().back();
    ASSERT_TRUE(last_commit_info.__isset.iceberg_data_file);
    const auto& iceberg_data_file = last_commit_info.iceberg_data_file;
    EXPECT_EQ(iceberg_data_file.path, result_obj.file_result.location);
    EXPECT_EQ(iceberg_data_file.format, result_obj.file_result.format);
    EXPECT_EQ(iceberg_data_file.record_count, result_obj.file_result.file_statistics.record_count);
    EXPECT_EQ(iceberg_data_file.file_size_in_bytes, result_obj.file_result.file_statistics.file_size);
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
    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);
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
    context->runtime_state = _runtime_state.get();

    // Setup output expressions to match tuple descriptor (2 slots)
    TExpr file_expr, pos_expr;
    context->output_exprs = {file_expr, pos_expr};

    // Only add _pos, not _file
    TExprNode pos_node;
    pos_node.node_type = TExprNodeType::SLOT_REF;
    pos_node.__set_slot_ref(TSlotRef());
    pos_node.slot_ref.slot_id = 1;
    context->column_slot_map["_pos"] = pos_node;

    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);

    // Should fail because _file column is missing
    ASSERT_FALSE(result.ok());
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("Could not find _file column"));
}

// Test error condition: missing _pos column in column_slot_map
TEST_F(IcebergDeleteSinkTest, missing_pos_column) {
    auto context = std::make_shared<IcebergDeleteSinkContext>();
    context->tuple_desc_id = 0;
    context->runtime_state = _runtime_state.get();

    // Setup output expressions to match tuple descriptor (2 slots)
    TExpr file_expr, pos_expr;
    context->output_exprs = {file_expr, pos_expr};

    // Only add _file, not _pos
    TExprNode file_node;
    file_node.node_type = TExprNodeType::SLOT_REF;
    file_node.__set_slot_ref(TSlotRef());
    file_node.slot_ref.slot_id = 0;
    context->column_slot_map["_file"] = file_node;

    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);

    // Should fail because _pos column is missing
    ASSERT_FALSE(result.ok());
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("Could not find _pos column"));
}

// Test error condition: not enough column evaluators
TEST_F(IcebergDeleteSinkTest, not_enough_evaluators) {
    auto context = std::make_shared<IcebergDeleteSinkContext>();
    context->tuple_desc_id = 0;
    context->runtime_state = _runtime_state.get();

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

    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);

    // Should fail because not enough evaluators
    ASSERT_FALSE(result.ok());
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("Not enough column evaluators"));
}

// Test error condition: invalid tuple descriptor ID
TEST_F(IcebergDeleteSinkTest, invalid_tuple_descriptor_id) {
    auto context = std::make_shared<IcebergDeleteSinkContext>();
    context->tuple_desc_id = 999; // Invalid ID
    context->runtime_state = _runtime_state.get();

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

    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);

    // Should fail because tuple descriptor ID is invalid
    ASSERT_FALSE(result.ok());
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("Failed to find tuple descriptor"));
}

// Test that Iceberg delete file has REQUIRED columns for file_path and pos
TEST_F(IcebergDeleteSinkTest, verify_required_columns_in_parquet) {
    auto context = create_delete_sink_context();
    auto provider = std::make_unique<IcebergDeleteSinkProvider>(context);
    auto result = provider->create_sink(0);
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

namespace {

class RecordingPartitionChunkWriter final : public PartitionChunkWriter {
public:
    RecordingPartitionChunkWriter(std::string partition, std::vector<int8_t> null_list,
                                  const std::shared_ptr<PartitionChunkWriterContext>& ctx,
                                  std::shared_ptr<int64_t> rows_written)
            : PartitionChunkWriter(std::move(partition), std::move(null_list), ctx),
              _rows_written(std::move(rows_written)) {}

    Status init() override { return Status::OK(); }
    Status write(const ChunkPtr& chunk) override {
        *_rows_written += chunk->num_rows();
        return Status::OK();
    }
    Status flush() override { return Status::OK(); }
    Status wait_flush() override { return Status::OK(); }
    Status finish() override { return Status::OK(); }
    bool is_finished() override { return true; }
    int64_t get_written_bytes() override { return 0; }
    int64_t get_flushable_bytes() override { return 0; }

private:
    std::shared_ptr<int64_t> _rows_written;
};

class RecordingPartitionChunkWriterFactory final : public PartitionChunkWriterFactory {
public:
    Status init() override { return Status::OK(); }

    PartitionChunkWriterPtr create(std::string partition,
                                   std::vector<int8_t> partition_field_null_list) const override {
        created_partitions.push_back(partition);
        auto rows = std::make_shared<int64_t>(0);
        rows_per_writer.push_back(rows);
        return std::make_shared<RecordingPartitionChunkWriter>(
                std::move(partition), std::move(partition_field_null_list),
                std::make_shared<PartitionChunkWriterContext>(), std::move(rows));
    }

    mutable std::vector<std::string> created_partitions;
    mutable std::vector<std::shared_ptr<int64_t>> rows_per_writer;
};

} // namespace

// A single referenced data file whose rows evaluate to different partitions (e.g. across a
// timestamptz month boundary or a partition-column UPDATE) must still reuse one writer and
// emit a single position delete file rather than splitting by recomputed partition.
TEST_F(IcebergDeleteSinkTest, same_file_across_different_computed_partitions_uses_single_writer) {
    auto factory = std::make_unique<RecordingPartitionChunkWriterFactory>();
    auto* factory_ptr = factory.get();

    std::vector<std::unique_ptr<ColumnEvaluator>> part_evaluators;
    part_evaluators.push_back(std::make_unique<ColumnSlotIdEvaluator>(2, TypeDescriptor(TYPE_BIGINT)));

    std::unordered_map<std::string, TExprNode> column_slot_map;
    TExprNode file_node;
    file_node.node_type = TExprNodeType::SLOT_REF;
    file_node.__set_slot_ref(TSlotRef());
    file_node.slot_ref.slot_id = 0;
    column_slot_map["_file"] = file_node;

    TExprNode pos_node;
    pos_node.node_type = TExprNodeType::SLOT_REF;
    pos_node.__set_slot_ref(TSlotRef());
    pos_node.slot_ref.slot_id = 1;
    column_slot_map["_pos"] = pos_node;

    IcebergDeleteSink sink({"visit_start_time_month"}, {"month"}, std::move(part_evaluators), std::move(factory),
                           _runtime_state.get(), std::move(column_slot_map));

    auto make_chunk = [](const std::string& file_path, int64_t pos, int64_t months_since_epoch) {
        auto chunk = std::make_shared<Chunk>();
        auto file_col = BinaryColumn::create();
        file_col->append(Slice(file_path));
        auto pos_col = Int64Column::create();
        pos_col->append(pos);
        auto part_col = Int64Column::create();
        part_col->append(months_since_epoch);
        chunk->append_column(std::move(file_col), 0);
        chunk->append_column(std::move(pos_col), 1);
        chunk->append_column(std::move(part_col), 2);
        return chunk;
    };

    const std::string feb_file = "s3://bucket/table/data/visit_start_time_month=2026-02/a.parquet";
    const std::string other_file = "s3://bucket/table/data/visit_start_time_month=2026-02/b.parquet";

    // Row 0 evaluates to 2026-02 (673), row 1 from the same data file evaluates to 2026-03 (674).
    ASSERT_OK(sink.add(make_chunk(feb_file, 10, 673)));
    ASSERT_OK(sink.add(make_chunk(feb_file, 20, 674)));
    ASSERT_EQ(factory_ptr->created_partitions.size(), 1);
    ASSERT_EQ(*factory_ptr->rows_per_writer[0], 2);

    // A distinct referenced data file still gets its own writer.
    ASSERT_OK(sink.add(make_chunk(other_file, 30, 674)));
    ASSERT_EQ(factory_ptr->created_partitions.size(), 2);
    ASSERT_EQ(*factory_ptr->rows_per_writer[1], 1);
}

} // namespace starrocks::connector
