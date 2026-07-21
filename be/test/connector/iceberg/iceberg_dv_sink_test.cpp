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

#include "connector/iceberg/iceberg_dv_sink.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/config_exec_fwd.h"
#include "connector_primitive/sink_memory_manager.h"
#include "exec/exec_env.h"
#include "formats/column_evaluator.h"
#include "formats/io/async_flush_stream_poller.h"
#include "fs/fs.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "types/type_descriptor.h"

namespace starrocks::connector {

class IcebergDvSinkTest : public ::testing::Test {
protected:
    void SetUp() override {
        _pool = std::make_unique<ObjectPool>();
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        TUniqueId fragment_id;
        fragment_id.hi = 0;
        fragment_id.lo = 0;
        // Unit-test binaries do not link the ExecEnv singleton, so build the runtime state with a
        // null ExecEnv and locally-owned services (matching IcebergDeleteSinkTest). The DV sink
        // takes its io poller and memory manager through init(), not from ExecEnv.
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals,
                                                        static_cast<ExecEnv*>(nullptr));
        _query_execution_services.runtime = &_runtime_services;
        _runtime_state->set_query_execution_services(&_query_execution_services);
        _runtime_state->init_instance_mem_tracker();

        // A 2-slot tuple (file_path VARCHAR, pos BIGINT) at id 0, matching column_slot_map slots.
        TDescriptorTableBuilder desc_tbl_builder;
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

        _tmp_dir = "./ut_dir/iceberg_dv_sink_test";
        (void)FileSystem::Default()->delete_dir_recursive(_tmp_dir);
        ASSERT_OK(FileSystem::Default()->create_dir_recursive(_tmp_dir));
    }

    void TearDown() override { (void)FileSystem::Default()->delete_dir_recursive(_tmp_dir); }

    std::shared_ptr<IcebergDvSinkContext> create_dv_sink_context() {
        auto ctx = std::make_shared<IcebergDvSinkContext>();
        ctx->path = _tmp_dir;
        ctx->runtime_state = _runtime_state.get();
        ctx->writer_tag = "delete";

        TExprNode f;
        f.node_type = TExprNodeType::SLOT_REF;
        f.__set_slot_ref(TSlotRef());
        f.slot_ref.slot_id = 0;
        ctx->column_slot_map["_file"] = f;

        TExprNode p;
        p.node_type = TExprNodeType::SLOT_REF;
        p.__set_slot_ref(TSlotRef());
        p.slot_ref.slot_id = 1;
        ctx->column_slot_map["_pos"] = p;
        return ctx;
    }

    static ChunkPtr make_delete_chunk(const std::vector<std::pair<std::string, int64_t>>& rows) {
        auto file_col = BinaryColumn::create();
        auto pos_col = Int64Column::create();
        for (const auto& [f, p] : rows) {
            file_col->append(Slice(f));
            pos_col->append(p);
        }
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(std::move(file_col), 0); // slot_id 0 == _file
        chunk->append_column(std::move(pos_col), 1);  // slot_id 1 == _pos
        return chunk;
    }

    // Drive init the way ConnectorSinkOperator does: pass the io poller, (optional) profile and
    // the sink memory manager, so init()'s _op_mem_mgr->init() has valid targets.
    Status init_sink(IcebergDvSink* dv) { return dv->init(&_poller, /*profile=*/nullptr, &_mgr); }

    std::unique_ptr<ObjectPool> _pool;
    RuntimeServices _runtime_services;
    QueryExecutionServices _query_execution_services;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::string _tmp_dir;
    formats::AsyncFlushStreamPoller _poller;
    connector::SinkMemoryManager _mgr{nullptr, nullptr};
};

TEST_F(IcebergDvSinkTest, create_dv_sink) {
    auto ctx = create_dv_sink_context();
    IcebergDvSinkProvider provider(ctx);
    auto result = provider.create_sink(0);
    ASSERT_TRUE(result.ok()) << result.status();
    auto* dv = dynamic_cast<IcebergDvSink*>(result.value().get());
    ASSERT_NE(dv, nullptr);
}

TEST_F(IcebergDvSinkTest, missing_file_column) {
    auto ctx = create_dv_sink_context();
    ctx->column_slot_map.erase("_file");
    IcebergDvSinkProvider provider(ctx);
    auto result = provider.create_sink(0);
    ASSERT_FALSE(result.ok());
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("_file"));
}

TEST_F(IcebergDvSinkTest, add_groups_by_file) {
    auto ctx = create_dv_sink_context();
    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));
    ASSERT_OK(dv->add(make_delete_chunk({{"fileA", 0}, {"fileB", 5}, {"fileA", 2}, {"fileA", 0}})));
    EXPECT_EQ(dv->num_data_files_for_test(), 2u); // fileA, fileB
}

TEST_F(IcebergDvSinkTest, add_empty_chunk_ok) {
    auto ctx = create_dv_sink_context();
    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));
    ASSERT_OK(dv->add(std::make_shared<Chunk>()));
    EXPECT_EQ(dv->num_data_files_for_test(), 0u);
}

TEST_F(IcebergDvSinkTest, finish_writes_puffin_and_emits_commit_info) {
    auto ctx = create_dv_sink_context();
    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));
    ASSERT_OK(dv->add(make_delete_chunk(
            {{"s3://t/data/fA.parquet", 0}, {"s3://t/data/fA.parquet", 7}, {"s3://t/data/fB.parquet", 3}})));
    ASSERT_OK(dv->finish());
    EXPECT_TRUE(dv->is_finished());

    ASSERT_EQ(_runtime_state->sink_commit_infos().size(), 2u);
    std::map<std::string, TIcebergDataFile> by_ref;
    for (const auto& ci : _runtime_state->sink_commit_infos()) {
        ASSERT_TRUE(ci.__isset.iceberg_data_file);
        const auto& f = ci.iceberg_data_file;
        EXPECT_EQ(f.format, "puffin");
        EXPECT_EQ(f.file_content, TIcebergFileContent::POSITION_DELETES);
        EXPECT_TRUE(f.__isset.content_offset);
        EXPECT_TRUE(f.__isset.content_size_in_bytes);
        by_ref[f.referenced_data_file] = f;
    }
    ASSERT_EQ(by_ref.count("s3://t/data/fA.parquet"), 1u);
    ASSERT_EQ(by_ref.count("s3://t/data/fB.parquet"), 1u);
    EXPECT_EQ(by_ref["s3://t/data/fA.parquet"].record_count, 2); // positions 0, 7
    EXPECT_EQ(by_ref["s3://t/data/fB.parquet"].record_count, 1); // position 3
    // Deleted rows must land in the load counters (DML row accounting).
    EXPECT_EQ(_runtime_state->num_rows_load_sink(), 3);
}

TEST_F(IcebergDvSinkTest, finish_empty_writes_nothing) {
    auto ctx = create_dv_sink_context();
    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));
    ASSERT_OK(dv->finish());
    EXPECT_TRUE(dv->is_finished());
    EXPECT_TRUE(_runtime_state->sink_commit_infos().empty());
}

// A cancelled query (finish() done, FE commit never happens) triggers rollback(), which must
// delete the uncommitted Puffin file instead of leaving it stranded in the table directory.
TEST_F(IcebergDvSinkTest, rollback_removes_puffin_after_finish) {
    auto ctx = create_dv_sink_context();
    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));
    ASSERT_OK(dv->add(make_delete_chunk({{"s3://t/data/fA.parquet", 0}})));
    ASSERT_OK(dv->finish());

    ASSERT_EQ(_runtime_state->sink_commit_infos().size(), 1u);
    const std::string location = _runtime_state->sink_commit_infos()[0].iceberg_data_file.path;
    ASSERT_OK(FileSystem::Default()->path_exists(location));

    dv->rollback();
    EXPECT_TRUE(FileSystem::Default()->path_exists(location).is_not_found());
}

// A DV sink instance can hold data files from different partitions (shuffle is by _file, not by
// partition), so each file's partition must be captured independently. Two files in two
// partitions must yield two commit entries with each file's own partition_path.
TEST_F(IcebergDvSinkTest, partitioned_captures_per_file_partition) {
    auto ctx = create_dv_sink_context();
    ctx->partition_column_names = {"pt"};
    ctx->transform_exprs = {"identity"};
    ctx->partition_evaluators.push_back(std::make_unique<ColumnSlotIdEvaluator>(2, TYPE_INT_DESC));

    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));

    // fileA -> pt=1, fileB -> pt=2, all in one chunk on one sink instance.
    auto file_col = BinaryColumn::create();
    auto pos_col = Int64Column::create();
    auto pt_col = Int32Column::create();
    struct Row {
        std::string f;
        int64_t pos;
        int32_t pt;
    };
    for (const auto& r : std::vector<Row>{{"s3://t/data/fA.parquet", 0, 1},
                                          {"s3://t/data/fA.parquet", 3, 1},
                                          {"s3://t/data/fB.parquet", 9, 2}}) {
        file_col->append(Slice(r.f));
        pos_col->append(r.pos);
        pt_col->append(r.pt);
    }
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(file_col), 0);
    chunk->append_column(std::move(pos_col), 1);
    chunk->append_column(std::move(pt_col), 2);
    ASSERT_OK(dv->add(chunk));
    ASSERT_OK(dv->finish());

    ASSERT_EQ(_runtime_state->sink_commit_infos().size(), 2u);
    std::map<std::string, TIcebergDataFile> by_ref;
    for (const auto& ci : _runtime_state->sink_commit_infos()) {
        by_ref[ci.iceberg_data_file.referenced_data_file] = ci.iceberg_data_file;
    }
    ASSERT_EQ(by_ref.size(), 2u);
    EXPECT_TRUE(by_ref["s3://t/data/fA.parquet"].__isset.partition_path);
    EXPECT_THAT(by_ref["s3://t/data/fA.parquet"].partition_path, testing::HasSubstr("pt=1"));
    EXPECT_THAT(by_ref["s3://t/data/fB.parquet"].partition_path, testing::HasSubstr("pt=2"));
    EXPECT_EQ(by_ref["s3://t/data/fA.parquet"].partition_null_fingerprint, "0");
}

} // namespace starrocks::connector
