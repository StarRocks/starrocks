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
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/config_exec_fwd.h"
#include "connector_primitive/sink_memory_manager.h"
#include "exec/exec_env.h"
#include "formats/column_evaluator.h"
#include "formats/io/async_flush_stream_poller.h"
#include "formats/parquet/file_writer.h"
#include "formats/puffin/iceberg_dv_writer.h"
#include "fs/fs.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"
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

    // --- previous-delete merge helpers ---

    std::shared_ptr<IcebergDvSinkContext> create_dv_sink_context_with_previous(
            std::vector<TIcebergPreviousDeleteFile> previous) {
        auto ctx = create_dv_sink_context();
        ctx->previous_delete_files =
                std::make_shared<const std::vector<TIcebergPreviousDeleteFile>>(std::move(previous));
        return ctx;
    }

    // Writes an "old" single-file DV Puffin and returns its blob coordinates.
    formats::IcebergDvCommitEntry write_old_puffin(const std::string& path, const std::string& ref,
                                                   const std::vector<uint64_t>& positions) {
        formats::IcebergDvWriter writer;
        for (uint64_t p : positions) {
            writer.add(ref, p);
        }
        auto file = FileSystem::Default()->new_writable_file(path).value();
        auto entries = writer.finish(file.get()).value();
        EXPECT_OK(file->close());
        EXPECT_EQ(entries.size(), 1u);
        return entries[0];
    }

    // Writes a 2-column (file_path, pos) parquet position-delete file (posix fs).
    void write_parquet_pd(const std::string& path, const std::vector<std::pair<std::string, int64_t>>& rows) {
        std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_VARCHAR),
                                               TypeDescriptor::from_logical_type(TYPE_BIGINT)};
        std::vector<std::string> names{"file_path", "pos"};
        std::vector<parquet::FileColumnId> field_ids(2);
        field_ids[0].field_id = INT32_MAX - 101;
        field_ids[1].field_id = INT32_MAX - 102;
        auto schema = parquet::ParquetBuildHelper::make_schema(names, type_descs, field_ids).ValueOrDie();
        auto properties = parquet::ParquetBuildHelper::make_properties(parquet::ParquetBuilderOptions()).value();
        auto file = FileSystem::Default()->new_writable_file(path).value();
        auto writer = std::make_shared<parquet::SyncFileWriter>(std::move(file), properties, schema, type_descs,
                                                                _runtime_state.get());
        ASSERT_OK(writer->init());
        auto file_col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
        auto pos_col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
        for (const auto& [f, p] : rows) {
            (void)file_col->append_strings(std::vector<Slice>{Slice(f)});
            pos_col->append_datum(Datum(p));
        }
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(std::move(file_col), chunk->num_columns());
        chunk->append_column(std::move(pos_col), chunk->num_columns());
        ASSERT_OK(writer->write(chunk.get()));
        ASSERT_OK(writer->close());
    }

    // `ref` is the single data file a file-scoped entry applies to, as associated by FE scan
    // planning.
    static TIcebergPreviousDeleteFile make_prev(const std::string& path, const std::string& format,
                                                const std::string& ref, bool file_scoped) {
        TIcebergPreviousDeleteFile prev;
        prev.__set_path(path);
        prev.__set_format(format);
        prev.__set_referenced_data_files({ref});
        prev.__set_file_scoped(file_scoped);
        return prev;
    }

    // A partition-scoped entry arrives once per delete file, carrying every associated data file.
    static TIcebergPreviousDeleteFile make_partition_scoped_prev(const std::string& path, const std::string& format,
                                                                 const std::vector<std::string>& refs) {
        TIcebergPreviousDeleteFile prev;
        prev.__set_path(path);
        prev.__set_format(format);
        prev.__set_file_scoped(false);
        prev.__set_referenced_data_files(refs);
        return prev;
    }

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
        // Partition metadata is attached by the FE commit from the referenced data file
        // itself; the sink no longer reports it.
        EXPECT_FALSE(f.__isset.partition_path);
        by_ref[f.referenced_data_file] = f;
    }
    ASSERT_EQ(by_ref.count("s3://t/data/fA.parquet"), 1u);
    ASSERT_EQ(by_ref.count("s3://t/data/fB.parquet"), 1u);
    EXPECT_EQ(by_ref["s3://t/data/fA.parquet"].record_count, 2); // positions 0, 7
    EXPECT_EQ(by_ref["s3://t/data/fB.parquet"].record_count, 1); // position 3
    // Without previous deletes the statement's contribution equals the merged total.
    EXPECT_EQ(by_ref["s3://t/data/fA.parquet"].added_delete_rows, 2);
    EXPECT_EQ(by_ref["s3://t/data/fB.parquet"].added_delete_rows, 1);
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

// --- previous-delete merge ---

// Anti-revival: the old DV deleted positions {100, 200}; the current statement deletes only
// position 1 (its predicate does not match the old rows). The new DV must still contain the
// union — dropping the old positions would resurrect previously deleted rows.
TEST_F(IcebergDvSinkTest, merges_previous_dv) {
    const std::string old_puffin = _tmp_dir + "/old.puffin";
    auto old_entry = write_old_puffin(old_puffin, "fileA", {100, 200});
    auto prev = make_prev(old_puffin, "puffin", "fileA", /*file_scoped=*/true);
    prev.__set_content_offset(old_entry.content_offset);
    prev.__set_content_size_in_bytes(old_entry.content_size_in_bytes);
    prev.__set_record_count(old_entry.record_count);

    auto ctx = create_dv_sink_context_with_previous({prev});
    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));
    ASSERT_OK(dv->add(make_delete_chunk({{"fileA", 1}})));
    ASSERT_OK(dv->finish());

    ASSERT_EQ(_runtime_state->sink_commit_infos().size(), 1u);
    const auto& ci = _runtime_state->sink_commit_infos()[0];
    // Merged DV contains {1, 100, 200}, but this statement only deleted one row.
    EXPECT_EQ(ci.iceberg_data_file.record_count, 3);
    ASSERT_TRUE(ci.iceberg_data_file.__isset.added_delete_rows);
    EXPECT_EQ(ci.iceberg_data_file.added_delete_rows, 1);
    EXPECT_EQ(_runtime_state->num_rows_load_sink(), 1);
    // The folded old DV is reported for removeDeletes.
    ASSERT_TRUE(ci.__isset.rewritten_delete_files);
    ASSERT_EQ(ci.rewritten_delete_files.size(), 1u);
    EXPECT_EQ(ci.rewritten_delete_files[0].path, old_puffin);
    EXPECT_EQ(ci.rewritten_delete_files[0].content_offset, old_entry.content_offset);
}

// A data file can carry both an old DV and older file-scoped position deletes (e.g. a table
// upgraded from V2 after DVs were introduced): both must be folded in and BOTH reported as
// rewritten so the FE removes each of them.
TEST_F(IcebergDvSinkTest, merges_dv_and_file_scoped_pd_for_same_ref) {
    const std::string old_puffin = _tmp_dir + "/old2.puffin";
    auto old_entry = write_old_puffin(old_puffin, "fileA", {100});
    auto prev_dv = make_prev(old_puffin, "puffin", "fileA", /*file_scoped=*/true);
    prev_dv.__set_content_offset(old_entry.content_offset);
    prev_dv.__set_content_size_in_bytes(old_entry.content_size_in_bytes);
    prev_dv.__set_record_count(old_entry.record_count);
    const std::string pd_path = _tmp_dir + "/old2_pd.parquet";
    write_parquet_pd(pd_path, {{"fileA", 7}});

    auto ctx = create_dv_sink_context_with_previous(
            {prev_dv, make_prev(pd_path, "parquet", "fileA", /*file_scoped=*/true)});
    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));
    ASSERT_OK(dv->add(make_delete_chunk({{"fileA", 1}})));
    ASSERT_OK(dv->finish());

    ASSERT_EQ(_runtime_state->sink_commit_infos().size(), 1u);
    const auto& ci = _runtime_state->sink_commit_infos()[0];
    EXPECT_EQ(ci.iceberg_data_file.record_count, 3); // {1, 7, 100}
    ASSERT_TRUE(ci.__isset.rewritten_delete_files);
    ASSERT_EQ(ci.rewritten_delete_files.size(), 2u);
    EXPECT_EQ(ci.rewritten_delete_files[0].path, old_puffin);
    EXPECT_EQ(ci.rewritten_delete_files[1].path, pd_path);
}

TEST_F(IcebergDvSinkTest, merges_previous_parquet_pd) {
    const std::string pd_path = _tmp_dir + "/old_pd.parquet";
    write_parquet_pd(pd_path, {{"fileA", 7}, {"fileB", 9}});
    auto prev = make_prev(pd_path, "parquet", "fileA", /*file_scoped=*/true);

    auto ctx = create_dv_sink_context_with_previous({prev});
    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));
    ASSERT_OK(dv->add(make_delete_chunk({{"fileA", 1}})));
    ASSERT_OK(dv->finish());

    ASSERT_EQ(_runtime_state->sink_commit_infos().size(), 1u);
    const auto& ci = _runtime_state->sink_commit_infos()[0];
    EXPECT_EQ(ci.iceberg_data_file.record_count, 2); // {1, 7}; fileB's row filtered out
    ASSERT_TRUE(ci.__isset.rewritten_delete_files);
    ASSERT_EQ(ci.rewritten_delete_files.size(), 1u);
    EXPECT_EQ(ci.rewritten_delete_files[0].path, pd_path);
}

TEST_F(IcebergDvSinkTest, untouched_previous_is_skipped) {
    const std::string pd_path = _tmp_dir + "/old_pd.parquet";
    write_parquet_pd(pd_path, {{"fileC", 7}});
    auto prev = make_prev(pd_path, "parquet", "fileC", /*file_scoped=*/true);

    auto ctx = create_dv_sink_context_with_previous({prev});
    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));
    ASSERT_OK(dv->add(make_delete_chunk({{"fileA", 1}}))); // fileC untouched
    ASSERT_OK(dv->finish());

    ASSERT_EQ(_runtime_state->sink_commit_infos().size(), 1u); // only fileA's DV, no fileC entry
    const auto& ci = _runtime_state->sink_commit_infos()[0];
    EXPECT_EQ(ci.iceberg_data_file.referenced_data_file, "fileA");
    EXPECT_EQ(ci.iceberg_data_file.record_count, 1);
    EXPECT_FALSE(ci.__isset.rewritten_delete_files);
}

TEST_F(IcebergDvSinkTest, partition_scoped_pd_merged_but_not_rewritten) {
    const std::string pd_path = _tmp_dir + "/old_partition_pd.parquet";
    write_parquet_pd(pd_path, {{"fileA", 7}, {"fileB", 9}});
    // A partition-scoped file arrives as ONE entry carrying all associated data files; the sink
    // reads it once, filtered to the touched subset.
    auto ctx =
            create_dv_sink_context_with_previous({make_partition_scoped_prev(pd_path, "parquet", {"fileA", "fileB"})});
    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));
    ASSERT_OK(dv->add(make_delete_chunk({{"fileA", 1}}))); // fileB untouched
    ASSERT_OK(dv->finish());

    ASSERT_EQ(_runtime_state->sink_commit_infos().size(), 1u);
    const auto& ci = _runtime_state->sink_commit_infos()[0];
    EXPECT_EQ(ci.iceberg_data_file.record_count, 2); // {1, 7} merged; fileB row not materialized
    EXPECT_FALSE(ci.__isset.rewritten_delete_files); // partition-scoped PD must be kept
}

TEST_F(IcebergDvSinkTest, rewritten_grouped_per_file) {
    const std::string pd_a = _tmp_dir + "/old_a.parquet";
    const std::string pd_b = _tmp_dir + "/old_b.parquet";
    write_parquet_pd(pd_a, {{"fileA", 7}});
    write_parquet_pd(pd_b, {{"fileB", 9}});
    auto ctx = create_dv_sink_context_with_previous({
            make_prev(pd_a, "parquet", "fileA", /*file_scoped=*/true),
            make_prev(pd_b, "parquet", "fileB", /*file_scoped=*/true),
    });
    IcebergDvSinkProvider provider(ctx);
    auto sink = std::move(provider.create_sink(0).value());
    auto* dv = dynamic_cast<IcebergDvSink*>(sink.get());
    ASSERT_NE(dv, nullptr);
    ASSERT_OK(init_sink(dv));
    ASSERT_OK(dv->add(make_delete_chunk({{"fileA", 1}, {"fileB", 2}})));
    ASSERT_OK(dv->finish());

    ASSERT_EQ(_runtime_state->sink_commit_infos().size(), 2u);
    for (const auto& ci : _runtime_state->sink_commit_infos()) {
        ASSERT_TRUE(ci.__isset.rewritten_delete_files);
        ASSERT_EQ(ci.rewritten_delete_files.size(), 1u);
        const std::string& ref = ci.iceberg_data_file.referenced_data_file;
        // Each commit info carries exactly its own file's rewritten entry, not the other's.
        EXPECT_EQ(ci.rewritten_delete_files[0].path, ref == "fileA" ? pd_a : pd_b);
    }
}

} // namespace starrocks::connector
