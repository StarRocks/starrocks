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

#include "connector/iceberg_row_delta_sink.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/status.h"
#include "exec/pipeline/fragment_context.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

// A mock ConnectorChunkSink that records chunks passed to add().
class MockChunkSink : public ConnectorChunkSink {
public:
    explicit MockChunkSink(RuntimeState* state)
            : ConnectorChunkSink({}, {}, std::make_unique<NopPartitionChunkWriterFactory>(), state, true) {}

    void callback_on_commit(const CommitResult& result) override {}

    // Skip the base class setup (profile / writer factory / op_mem_mgr wiring) that a
    // mock doesn't need. The outer IcebergRowDeltaSink's init() still drives real
    // assignments against `_op_mem_mgr` before invoking this, which is what the init()
    // test inspects via get_op_mem_mgr().
    Status init() override { return Status::OK(); }

    Status add(const ChunkPtr& chunk) override {
        received_chunks.push_back(chunk);
        total_rows += chunk->num_rows();
        return Status::OK();
    }

    void rollback() override { ++rollback_count; }

    Status finish() override {
        ++finish_count;
        return Status::OK();
    }

    bool is_finished() override { return finished; }

    // Expose protected _op_mem_mgr so tests can assert init() wired it.
    SinkOperatorMemoryManager* get_op_mem_mgr() const { return _op_mem_mgr; }

    std::vector<ChunkPtr> received_chunks;
    int total_rows = 0;
    int rollback_count = 0;
    int finish_count = 0;
    bool finished = false;
};

class IcebergRowDeltaSinkTest : public ::testing::Test {
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
        auto* exec_env = ExecEnv::GetInstance();
        _runtime_state->set_exec_env(exec_env);
        _runtime_state->set_query_execution_services(&exec_env->query_execution_services());
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
        _mem_tracker = std::make_unique<MemTracker>(MemTrackerType::QUERY_POOL, -1, "IcebergRowDeltaSinkTest");
    }

    // Build a chunk with the layout: [_file(VARCHAR), _pos(BIGINT), data_col(VARCHAR), op_code(TINYINT)]
    // Each row gets its own op_code from the provided vector.
    ChunkPtr build_chunk(const std::vector<std::string>& files, const std::vector<int64_t>& positions,
                         const std::vector<std::string>& data_values, const std::vector<int8_t>& op_codes) {
        auto chunk = std::make_shared<Chunk>();

        // _file column (slot 0)
        auto file_col = BinaryColumn::create();
        for (const auto& f : files) {
            file_col->append(f);
        }
        chunk->append_column(file_col, 0);

        // _pos column (slot 1)
        auto pos_col = FixedLengthColumn<int64_t>::create();
        for (auto p : positions) {
            pos_col->append(p);
        }
        chunk->append_column(pos_col, 1);

        // data column (slot 2)
        auto data_col = BinaryColumn::create();
        for (const auto& v : data_values) {
            data_col->append(v);
        }
        chunk->append_column(data_col, 2);

        // op_code column (slot 3)
        auto op_col = FixedLengthColumn<int8_t>::create();
        for (auto op : op_codes) {
            op_col->append(op);
        }
        chunk->append_column(op_col, 3);

        return chunk;
    }

    // Create the IcebergRowDeltaSink with mock sub-sinks.
    // Returns the sink and raw pointers to the mock sinks for verification.
    struct SinkWithMocks {
        std::unique_ptr<IcebergRowDeltaSink> sink;
        MockChunkSink* delete_sink;
        MockChunkSink* data_sink;
    };

    SinkWithMocks create_row_delta_sink_with_mocks(int32_t op_code_index = 3) {
        auto delete_sink = std::make_unique<MockChunkSink>(_runtime_state.get());
        auto data_sink = std::make_unique<MockChunkSink>(_runtime_state.get());
        auto* delete_ptr = delete_sink.get();
        auto* data_ptr = data_sink.get();

        auto row_delta_sink = std::make_unique<IcebergRowDeltaSink>(std::move(delete_sink), std::move(data_sink),
                                                                    op_code_index, nullptr, _runtime_state.get());

        return {std::move(row_delta_sink), delete_ptr, data_ptr};
    }

    std::unique_ptr<ObjectPool> _pool;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;
    std::unique_ptr<MemTracker> _mem_tracker;
};

// Test 1: Verify that IcebergRowDeltaSink can be constructed with mock sub-sinks
TEST_F(IcebergRowDeltaSinkTest, create_row_delta_sink) {
    auto [sink, delete_mock, data_mock] = create_row_delta_sink_with_mocks();

    ASSERT_NE(sink, nullptr);

    // Verify the sink is of the correct type
    auto* row_delta_sink = dynamic_cast<IcebergRowDeltaSink*>(sink.get());
    ASSERT_NE(row_delta_sink, nullptr);

    // Verify that a provider rejects a wrong context type
    auto provider = std::make_unique<IcebergRowDeltaSinkProvider>();
    auto bad_ctx = std::make_shared<IcebergDeleteSinkContext>();
    bad_ctx->fragment_context = _fragment_context.get();
    auto result = provider->create_chunk_sink(bad_ctx, 0);
    ASSERT_FALSE(result.ok());
    EXPECT_THAT(std::string(result.status().message()),
                testing::HasSubstr("context is not IcebergRowDeltaSinkContext"));
}

// Test 2: Verify op_code routing sends rows to correct sub-sinks
TEST_F(IcebergRowDeltaSinkTest, op_code_routing) {
    auto [sink, delete_mock, data_mock] = create_row_delta_sink_with_mocks();

    // Build a chunk with 4 rows, one per op_code: NO_OP(0), DELETE(1), UPDATE(2), INSERT(3)
    auto chunk = build_chunk({"file1.parquet", "file1.parquet", "file2.parquet", ""}, {0, 1, 2, 0},
                             {"val0", "val1", "val2", "val3"}, {0, 1, 2, 3});

    ASSERT_OK(sink->add(chunk));

    // DELETE(1) and UPDATE(2) go to delete sink => 2 rows
    EXPECT_EQ(delete_mock->total_rows, 2);
    // UPDATE(2) and INSERT(3) go to data sink => 2 rows
    EXPECT_EQ(data_mock->total_rows, 2);

    // Verify delete chunk content: rows 1 and 2 (DELETE and UPDATE).
    // filter_chunk_by_rows preserves all columns and slot_ids, so sub-sinks receive
    // the full 4-column chunk (_file, _pos, data, op_code) filtered to matching rows.
    ASSERT_EQ(delete_mock->received_chunks.size(), 1);
    auto& del_chunk = delete_mock->received_chunks[0];
    EXPECT_EQ(del_chunk->num_rows(), 2);
    EXPECT_EQ(del_chunk->num_columns(), 4);

    // Check _file values in delete chunk
    auto* del_file_col = down_cast<const BinaryColumn*>(del_chunk->get_column_by_index(0).get());
    EXPECT_EQ(del_file_col->get_slice(0).to_string(), "file1.parquet");
    EXPECT_EQ(del_file_col->get_slice(1).to_string(), "file2.parquet");

    // Check _pos values in delete chunk
    auto* del_pos_col = down_cast<const FixedLengthColumn<int64_t>*>(del_chunk->get_column_by_index(1).get());
    EXPECT_EQ(del_pos_col->get_data()[0], 1);
    EXPECT_EQ(del_pos_col->get_data()[1], 2);

    // Verify data chunk content: rows 2 and 3 (UPDATE and INSERT).
    // Same structural preservation as the delete sink — all 4 columns retained.
    ASSERT_EQ(data_mock->received_chunks.size(), 1);
    auto& dat_chunk = data_mock->received_chunks[0];
    EXPECT_EQ(dat_chunk->num_rows(), 2);
    EXPECT_EQ(dat_chunk->num_columns(), 4);

    // Check data values at column index 2 (the data column in the original chunk layout)
    auto* dat_col = down_cast<const BinaryColumn*>(dat_chunk->get_column_by_index(2).get());
    EXPECT_EQ(dat_col->get_slice(0).to_string(), "val2");
    EXPECT_EQ(dat_col->get_slice(1).to_string(), "val3");
}

// Test 3: Verify add() with an empty chunk returns OK without invoking sub-sinks
TEST_F(IcebergRowDeltaSinkTest, add_empty_chunk) {
    auto [sink, delete_mock, data_mock] = create_row_delta_sink_with_mocks();

    auto chunk = std::make_shared<Chunk>();
    ASSERT_OK(sink->add(chunk));

    EXPECT_EQ(delete_mock->total_rows, 0);
    EXPECT_EQ(data_mock->total_rows, 0);
    EXPECT_TRUE(delete_mock->received_chunks.empty());
    EXPECT_TRUE(data_mock->received_chunks.empty());
}

// Test 4: Verify all NO_OP rows produce zero rows to both sub-sinks
TEST_F(IcebergRowDeltaSinkTest, all_no_op) {
    auto [sink, delete_mock, data_mock] = create_row_delta_sink_with_mocks();

    auto chunk = build_chunk({"file1.parquet", "file2.parquet", "file3.parquet"}, {0, 1, 2}, {"val0", "val1", "val2"},
                             {0, 0, 0});

    ASSERT_OK(sink->add(chunk));

    EXPECT_EQ(delete_mock->total_rows, 0);
    EXPECT_EQ(data_mock->total_rows, 0);
    EXPECT_TRUE(delete_mock->received_chunks.empty());
    EXPECT_TRUE(data_mock->received_chunks.empty());
}

// Test 5: Verify that an unknown op_code returns an error status
TEST_F(IcebergRowDeltaSinkTest, unknown_op_code) {
    auto [sink, delete_mock, data_mock] = create_row_delta_sink_with_mocks();

    auto chunk = build_chunk({"file1.parquet"}, {0}, {"val0"}, {99});

    auto status = sink->add(chunk);
    EXPECT_FALSE(status.ok());
    EXPECT_THAT(std::string(status.message()), testing::HasSubstr("Unknown op_code"));
}

// Test 6: Verify rollback() fans out to both sub-sinks so cancelled queries
// do not leak uncommitted position-delete or data files.
TEST_F(IcebergRowDeltaSinkTest, rollback_forwards_to_both_sub_sinks) {
    auto [sink, delete_mock, data_mock] = create_row_delta_sink_with_mocks();

    EXPECT_EQ(delete_mock->rollback_count, 0);
    EXPECT_EQ(data_mock->rollback_count, 0);

    sink->rollback();

    EXPECT_EQ(delete_mock->rollback_count, 1);
    EXPECT_EQ(data_mock->rollback_count, 1);
}

// Test 7: Verify init() creates a child SinkOperatorMemoryManager for each sub-sink
// when a SinkMemoryManager is supplied, so memory pressure logic can see both
// sub-sinks' writer lists (OOM-safety wiring described in the commit).
TEST_F(IcebergRowDeltaSinkTest, init_wires_sub_sink_mem_managers) {
    auto query_pool_tracker =
            std::make_unique<MemTracker>(MemTrackerType::QUERY_POOL, -1, "IcebergRowDeltaSinkTest_pool");
    auto query_tracker = std::make_unique<MemTracker>(MemTrackerType::QUERY, -1, "IcebergRowDeltaSinkTest_query");
    SinkMemoryManager mgr(query_pool_tracker.get(), query_tracker.get());

    auto delete_sink = std::make_unique<MockChunkSink>(_runtime_state.get());
    auto data_sink = std::make_unique<MockChunkSink>(_runtime_state.get());
    auto* delete_ptr = delete_sink.get();
    auto* data_ptr = data_sink.get();

    IcebergRowDeltaSink sink(std::move(delete_sink), std::move(data_sink), /*op_code_index=*/3, &mgr,
                             _runtime_state.get());
    // Provide an outer SinkOperatorMemoryManager so the base init() path doesn't
    // dereference a null pointer and so the add_candidates() branch is exercised.
    sink.set_operator_mem_mgr(mgr.create_child_manager());

    ASSERT_OK(sink.init());

    // Each sub-sink should now have its own child manager, distinct from each other
    // and from nullptr. This confirms lines 40–43 of iceberg_row_delta_sink.cpp ran.
    EXPECT_NE(delete_ptr->get_op_mem_mgr(), nullptr);
    EXPECT_NE(data_ptr->get_op_mem_mgr(), nullptr);
    EXPECT_NE(delete_ptr->get_op_mem_mgr(), data_ptr->get_op_mem_mgr());
}

// Test 8: When every row in a chunk routes to the same sub-sink (pure DELETE or
// pure INSERT), add() should forward the original ChunkPtr without copying.
// This covers the `all_to_delete` / `all_to_data` fast path in add().
TEST_F(IcebergRowDeltaSinkTest, add_fast_path_all_delete_skips_copy) {
    auto [sink, delete_mock, data_mock] = create_row_delta_sink_with_mocks();

    auto chunk = build_chunk({"file1.parquet", "file1.parquet", "file2.parquet"}, {0, 1, 2}, {"val0", "val1", "val2"},
                             {1, 1, 1}); // all OP_DELETE

    ASSERT_OK(sink->add(chunk));

    ASSERT_EQ(delete_mock->received_chunks.size(), 1u);
    // Zero-copy: the delete sink received the exact same shared_ptr.
    EXPECT_EQ(delete_mock->received_chunks[0].get(), chunk.get());
    EXPECT_TRUE(data_mock->received_chunks.empty());
}

TEST_F(IcebergRowDeltaSinkTest, add_fast_path_all_insert_skips_copy) {
    auto [sink, delete_mock, data_mock] = create_row_delta_sink_with_mocks();

    auto chunk = build_chunk({"", "", ""}, {0, 0, 0}, {"val0", "val1", "val2"}, {3, 3, 3}); // all OP_INSERT

    ASSERT_OK(sink->add(chunk));

    ASSERT_EQ(data_mock->received_chunks.size(), 1u);
    EXPECT_EQ(data_mock->received_chunks[0].get(), chunk.get());
    EXPECT_TRUE(delete_mock->received_chunks.empty());
}

// Test 9: finish() forwards to both sub-sinks.
TEST_F(IcebergRowDeltaSinkTest, finish_forwards_to_both_sub_sinks) {
    auto [sink, delete_mock, data_mock] = create_row_delta_sink_with_mocks();

    EXPECT_EQ(delete_mock->finish_count, 0);
    EXPECT_EQ(data_mock->finish_count, 0);

    ASSERT_OK(sink->finish());

    EXPECT_EQ(delete_mock->finish_count, 1);
    EXPECT_EQ(data_mock->finish_count, 1);
}

// Test 10: is_finished() is true only when both sub-sinks report finished.
TEST_F(IcebergRowDeltaSinkTest, is_finished_requires_both_sub_sinks) {
    auto [sink, delete_mock, data_mock] = create_row_delta_sink_with_mocks();

    EXPECT_FALSE(sink->is_finished());

    delete_mock->finished = true;
    EXPECT_FALSE(sink->is_finished()); // data sub-sink not yet finished

    data_mock->finished = true;
    EXPECT_TRUE(sink->is_finished());
}

// Test 11: callback_on_commit() is a no-op on the composite sink — sub-sinks
// handle their own commit callbacks.
TEST_F(IcebergRowDeltaSinkTest, callback_on_commit_is_noop) {
    auto [sink, delete_mock, data_mock] = create_row_delta_sink_with_mocks();

    CommitResult result;
    sink->callback_on_commit(result); // must not crash; no observable side effect
    SUCCEED();
}

} // namespace starrocks::connector
