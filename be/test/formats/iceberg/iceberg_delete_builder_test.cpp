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

#include "formats/iceberg/iceberg_delete_builder.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "formats/column_evaluator.h"
#include "formats/io/async_flush_output_stream.h"
#include "formats/orc/orc_file_writer.h"
#include "formats/parquet/file_writer.h"
#include "formats/parquet/parquet_file_writer.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "testutil/column_test_helper.h"

namespace starrocks::formats {

namespace {

MemTracker* g_iceberg_delete_builder_test_mem_tracker = nullptr;

bool iceberg_delete_builder_test_env_initialized() {
    return true;
}

MemTracker* iceberg_delete_builder_test_mem_tracker() {
    return g_iceberg_delete_builder_test_mem_tracker;
}

} // namespace

class IcebergDeleteBuilderTest : public testing::Test {
protected:
    void SetUp() override {
        g_iceberg_delete_builder_test_mem_tracker = &_mem_tracker;
        CurrentThread::set_mem_tracker_source(iceberg_delete_builder_test_env_initialized,
                                              iceberg_delete_builder_test_mem_tracker);
        tls_mem_tracker = nullptr;

        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = 4096;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        _runtime_state->init_instance_mem_tracker();

        (void)FileSystem::Default()->delete_dir_recursive(_tmp_dir);
        ASSERT_OK(FileSystem::Default()->create_dir_recursive(_tmp_dir));
    }

    void TearDown() override {
        (void)FileSystem::Default()->delete_dir_recursive(_tmp_dir);
        tls_thread_status.set_mem_tracker(nullptr);
        CurrentThread::set_mem_tracker_source(nullptr, nullptr);
        g_iceberg_delete_builder_test_mem_tracker = nullptr;
    }

    static ChunkPtr make_delete_rows_chunk(const std::vector<std::pair<std::string, int64_t>>& rows) {
        std::vector<Slice> file_paths;
        std::vector<int64_t> positions;
        file_paths.reserve(rows.size());
        positions.reserve(rows.size());
        for (const auto& [file_path, pos] : rows) {
            file_paths.emplace_back(file_path);
            positions.push_back(pos);
        }
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnTestHelper::build_column<Slice>(file_paths), 0);
        chunk->append_column(ColumnTestHelper::build_column<int64_t>(positions), 1);
        return chunk;
    }

    // Writes a 2-column (file_path, pos) parquet position-delete file into `fs`.
    void write_parquet_delete_file(MemoryFileSystem& fs, const std::string& path,
                                   const std::vector<std::pair<std::string, int64_t>>& rows) {
        std::vector type_descs{TypeDescriptor::from_logical_type(TYPE_VARCHAR),
                               TypeDescriptor::from_logical_type(TYPE_BIGINT)};
        auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
        auto writer_options = std::make_shared<ParquetWriterOptions>();
        writer_options->column_ids = {FileColumnId{IcebergDeleteFileMeta::get_delete_file_path_slot().id(), {}},
                                      FileColumnId{IcebergDeleteFileMeta::get_delete_file_pos_slot().id(), {}}};
        ASSIGN_OR_ABORT(auto writable_file, fs.new_writable_file(path));
        auto output_stream = std::make_shared<parquet::ParquetOutputStream>(std::move(writable_file));
        ParquetFileWriter writer(path, std::move(output_stream), {"file_path", "pos"}, type_descs,
                                 std::move(column_evaluators), TCompressionType::NO_COMPRESSION,
                                 std::move(writer_options), [] {}, {false, false});
        ASSERT_OK(writer.init());
        auto chunk = make_delete_rows_chunk(rows);
        ASSERT_OK(writer.write(chunk.get()));
        ASSERT_OK(writer.close().io_status);
    }

    // Writes a 2-column (file_path, pos) orc position-delete file under _tmp_dir (default fs).
    void write_orc_delete_file(const std::string& path, const std::vector<std::pair<std::string, int64_t>>& rows) {
        std::vector type_descs{TypeDescriptor::from_logical_type(TYPE_VARCHAR),
                               TypeDescriptor::from_logical_type(TYPE_BIGINT)};
        auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
        ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(path));
        auto stream = std::make_unique<AsyncFlushOutputStream>(std::move(writable_file), nullptr, _runtime_state.get());
        auto orc_stream = std::make_shared<AsyncOrcOutputStream>(stream.get());
        ORCFileWriter writer(path, std::move(orc_stream), {"file_path", "pos"}, type_descs,
                             std::move(column_evaluators), TCompressionType::NO_COMPRESSION,
                             std::make_shared<ORCWriterOptions>(), [] {});
        ASSERT_OK(writer.init());
        auto chunk = make_delete_rows_chunk(rows);
        ASSERT_OK(writer.write(chunk.get()));
        ASSERT_OK(writer.close().io_status);
    }

    const std::string _parquet_delete_path = "/iceberg_position_delete.parquet";
    const std::string _parquet_data_path = "parquet_data_file.parquet";
    MemoryFileSystem _fs;
    MemTracker _mem_tracker{-1, "iceberg_delete_builder_test"};
    std::shared_ptr<RuntimeState> _runtime_state;
    const std::string _tmp_dir = "./ut_dir/iceberg_delete_builder_test";
};

TEST_F(IcebergDeleteBuilderTest, TestParquetBuilder) {
    RuntimeProfile runtime_profile("IcebergDeleteBuilderTest");

    write_parquet_delete_file(_fs, _parquet_delete_path,
                              {{_parquet_data_path, 7}, {"another_data_file.parquet", 9}, {_parquet_data_path, 11}});

    FormatScanContext scan_context;
    scan_context.timezone = "UTC";

    ASSIGN_OR_ABORT(const int64_t delete_file_size, _fs.get_file_size(_parquet_delete_path));
    TIcebergDeleteFile delete_file;
    delete_file.__set_full_path(_parquet_delete_path);
    delete_file.__set_length(delete_file_size);

    IcebergDeleteBuilder builder(IcebergDeleteBuilderContext{
            .scan_context = &scan_context,
            .fs = &_fs,
            .data_file_path = _parquet_data_path,
            .runtime_profile = &runtime_profile,
            .chunk_size = 4096,
    });

    ASSERT_OK(builder.build_parquet(delete_file));
    auto deletion_bitmap = builder.deletion_bitmap();
    ASSERT_NE(nullptr, deletion_bitmap);
    EXPECT_EQ(2, deletion_bitmap->get_cardinality());
    std::vector<uint64_t> deleted_rowids(deletion_bitmap->get_cardinality());
    deletion_bitmap->to_array(deleted_rowids);
    EXPECT_EQ((std::vector<uint64_t>{7, 11}), deleted_rowids);
}

TEST_F(IcebergDeleteBuilderTest, TestOrcBuilder) {
    RuntimeProfile runtime_profile("IcebergDeleteBuilderTest");

    const std::string data_path = "orc_data_file.parquet";
    const std::string delete_path = _tmp_dir + "/iceberg_position_delete.orc";
    write_orc_delete_file(delete_path, {{data_path, 7}, {"another_data_file.parquet", 9}, {data_path, 11}});

    FormatScanContext scan_context;
    scan_context.timezone = "UTC";

    ASSIGN_OR_ABORT(const int64_t delete_file_size, FileSystem::Default()->get_file_size(delete_path));
    TIcebergDeleteFile delete_file;
    delete_file.__set_full_path(delete_path);
    delete_file.__set_length(delete_file_size);

    IcebergDeleteBuilder builder(IcebergDeleteBuilderContext{
            .scan_context = &scan_context,
            .fs = FileSystem::Default(),
            .data_file_path = data_path,
            .runtime_profile = &runtime_profile,
            .chunk_size = 4096,
    });

    ASSERT_OK(builder.build_orc(delete_file));
    auto deletion_bitmap = builder.deletion_bitmap();
    ASSERT_NE(nullptr, deletion_bitmap);
    EXPECT_EQ(2, deletion_bitmap->get_cardinality());
    std::vector<uint64_t> deleted_rowids(deletion_bitmap->get_cardinality());
    deletion_bitmap->to_array(deleted_rowids);
    EXPECT_EQ((std::vector<uint64_t>{7, 11}), deleted_rowids);
}

// The delete file's own format wins over the data file's: an ORC delete file is read as ORC for a Parquet data file.
TEST_F(IcebergDeleteBuilderTest, TestBuildPositionDeleteUsesDeleteFileFormat) {
    RuntimeProfile runtime_profile("IcebergDeleteBuilderTest");

    const std::string data_path = "parquet_data_file.parquet";
    const std::string delete_path = _tmp_dir + "/iceberg_position_delete_mixed.orc";
    write_orc_delete_file(delete_path, {{data_path, 3}, {"another_data_file.parquet", 4}, {data_path, 8}});

    FormatScanContext scan_context;
    scan_context.timezone = "UTC";

    ASSIGN_OR_ABORT(const int64_t delete_file_size, FileSystem::Default()->get_file_size(delete_path));
    TIcebergDeleteFile delete_file;
    delete_file.__set_full_path(delete_path);
    delete_file.__set_length(delete_file_size);
    delete_file.__set_file_format(THdfsFileFormat::ORC);

    IcebergDeleteBuilder builder(IcebergDeleteBuilderContext{
            .scan_context = &scan_context,
            .fs = FileSystem::Default(),
            .data_file_path = data_path,
            .runtime_profile = &runtime_profile,
            .chunk_size = 4096,
    });

    ASSERT_OK(builder.build_position_delete(delete_file, THdfsFileFormat::PARQUET));
    auto deletion_bitmap = builder.deletion_bitmap();
    ASSERT_NE(nullptr, deletion_bitmap);
    std::vector<uint64_t> deleted_rowids(deletion_bitmap->get_cardinality());
    deletion_bitmap->to_array(deleted_rowids);
    EXPECT_EQ((std::vector<uint64_t>{3, 8}), deleted_rowids);
}

// Ranges are cut at every file_path change and at chunk boundaries (chunk size 2 here).
TEST_F(IcebergDeleteBuilderTest, TestReadRowsVisitsRuns) {
    write_parquet_delete_file(_fs, _parquet_delete_path,
                              {{"dataA", 1}, {"dataA", 2}, {"dataB", 3}, {"dataA", 4}, {"dataA", 5}});

    ASSIGN_OR_ABORT(const int64_t delete_file_size, _fs.get_file_size(_parquet_delete_path));
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_parquet_delete_path));

    std::vector<std::pair<std::string, std::vector<int64_t>>> runs;
    ASSERT_OK(IcebergPositionDeleteReader::read_rows(
            file.get(), _parquet_delete_path, delete_file_size, "parquet", 2, "UTC", FormatScannerOptions{}, nullptr,
            IcebergPositionDeleteReader::RangeCallback(
                    [&](const Slice& file_path, const int64_t* pos, size_t num_rows) {
                        runs.emplace_back(file_path.to_string(), std::vector<int64_t>(pos, pos + num_rows));
                    })));

    const std::vector<std::pair<std::string, std::vector<int64_t>>> expected{
            {"dataA", {1, 2}}, {"dataB", {3}}, {"dataA", {4}}, {"dataA", {5}}};
    EXPECT_EQ(expected, runs);
}

// The MOR IO counters of all delete files a builder reads are published once, when the builder
// goes away, with the same totals as publishing them per delete file.
TEST_F(IcebergDeleteBuilderTest, TestBuilderPublishesDeleteFileCountersOnce) {
    const std::string second_delete_path = "/iceberg_position_delete_2.parquet";
    write_parquet_delete_file(_fs, _parquet_delete_path, {{_parquet_data_path, 1}, {_parquet_data_path, 2}});
    write_parquet_delete_file(_fs, second_delete_path, {{_parquet_data_path, 3}});

    FormatScanContext scan_context;
    scan_context.timezone = "UTC";
    auto delete_file = [&](const std::string& path) {
        TIcebergDeleteFile df;
        df.__set_full_path(path);
        df.__set_length(_fs.get_file_size(path).value());
        return df;
    };
    auto make_builder = [&](RuntimeProfile* profile) {
        return std::make_unique<IcebergDeleteBuilder>(IcebergDeleteBuilderContext{
                .scan_context = &scan_context,
                .fs = &_fs,
                .data_file_path = _parquet_data_path,
                .runtime_profile = profile,
                .chunk_size = 4096,
        });
    };
    const std::vector<std::string> names{"MOR_AppIOBytesRead", "MOR_AppIOCounter",  "MOR_FSIOBytesRead",
                                         "MOR_FSIOCounter",    "MOR_SharedIOCount", "MOR_DirectIOCount"};

    RuntimeProfile once("once");
    {
        auto builder = make_builder(&once);
        ASSERT_OK(builder->build_parquet(delete_file(_parquet_delete_path)));
        ASSERT_OK(builder->build_parquet(delete_file(second_delete_path)));
        EXPECT_EQ(nullptr, once.get_counter("MOR_AppIOBytesRead"));
        EXPECT_EQ(3, builder->deletion_bitmap()->get_cardinality());
    }

    RuntimeProfile per_file("per_file");
    for (const auto& path : {_parquet_delete_path, second_delete_path}) {
        auto builder = make_builder(&per_file);
        ASSERT_OK(builder->build_parquet(delete_file(path)));
    }

    for (const auto& name : names) {
        ASSERT_NE(nullptr, once.get_counter(name)) << name;
        ASSERT_NE(nullptr, per_file.get_counter(name)) << name;
        EXPECT_EQ(per_file.get_counter(name)->value(), once.get_counter(name)->value()) << name;
    }
    EXPECT_GT(once.get_counter("MOR_AppIOBytesRead")->value(), 0);
}

// read_positions does not read file_path: rows of other data files come back too.
TEST_F(IcebergDeleteBuilderTest, TestReadPositionsSkipsFilePath) {
    write_parquet_delete_file(_fs, _parquet_delete_path, {{"dataA", 1}, {"dataB", 2}, {"dataA", 3}});
    const std::string orc_path = _tmp_dir + "/iceberg_position_delete.orc";
    write_orc_delete_file(orc_path, {{"dataA", 1}, {"dataB", 2}, {"dataA", 3}});

    auto read = [&](FileSystem* fs, const std::string& path, const std::string& format) {
        ASSIGN_OR_ABORT(const int64_t size, fs->get_file_size(path));
        ASSIGN_OR_ABORT(auto file, fs->new_random_access_file(path));
        std::vector<int64_t> positions;
        CHECK_OK(IcebergPositionDeleteReader::read_positions(
                file.get(), path, size, format, 4096, "UTC", FormatScannerOptions{}, nullptr,
                [&](const int64_t* pos, size_t num_rows) { positions.insert(positions.end(), pos, pos + num_rows); }));
        return positions;
    };
    EXPECT_EQ((std::vector<int64_t>{1, 2, 3}), read(&_fs, _parquet_delete_path, "parquet"));
    EXPECT_EQ((std::vector<int64_t>{1, 2, 3}), read(FileSystem::Default(), orc_path, "orc"));
}

// With references_only_data_file the builder takes every position without comparing file_path.
TEST_F(IcebergDeleteBuilderTest, TestBuilderReadsOnlyPositionsWhenFileScoped) {
    write_parquet_delete_file(_fs, _parquet_delete_path, {{_parquet_data_path, 7}, {_parquet_data_path, 11}});

    FormatScanContext scan_context;
    scan_context.timezone = "UTC";
    for (bool file_scoped : {false, true}) {
        TIcebergDeleteFile delete_file;
        delete_file.__set_full_path(_parquet_delete_path);
        delete_file.__set_length(_fs.get_file_size(_parquet_delete_path).value());
        if (file_scoped) {
            delete_file.__set_references_only_data_file(true);
        }
        IcebergDeleteBuilder builder(IcebergDeleteBuilderContext{
                .scan_context = &scan_context,
                .fs = &_fs,
                .data_file_path = _parquet_data_path,
                .chunk_size = 4096,
        });
        ASSERT_OK(builder.build_parquet(delete_file));
        std::vector<uint64_t> deleted(builder.deletion_bitmap()->get_cardinality());
        builder.deletion_bitmap()->to_array(deleted);
        EXPECT_EQ((std::vector<uint64_t>{7, 11}), deleted) << file_scoped;
    }
}

// Without a format from the FE, the delete file is read in the data file's format.
TEST_F(IcebergDeleteBuilderTest, TestBuildPositionDeleteFallsBackToDataFileFormat) {
    RuntimeProfile runtime_profile("IcebergDeleteBuilderTest");

    write_parquet_delete_file(_fs, _parquet_delete_path, {{_parquet_data_path, 2}, {"another_data_file.parquet", 5}});

    FormatScanContext scan_context;
    scan_context.timezone = "UTC";

    ASSIGN_OR_ABORT(const int64_t delete_file_size, _fs.get_file_size(_parquet_delete_path));
    TIcebergDeleteFile delete_file;
    delete_file.__set_full_path(_parquet_delete_path);
    delete_file.__set_length(delete_file_size);

    IcebergDeleteBuilder builder(IcebergDeleteBuilderContext{
            .scan_context = &scan_context,
            .fs = &_fs,
            .data_file_path = _parquet_data_path,
            .runtime_profile = &runtime_profile,
            .chunk_size = 4096,
    });

    ASSERT_OK(builder.build_position_delete(delete_file, THdfsFileFormat::PARQUET));
    auto deletion_bitmap = builder.deletion_bitmap();
    ASSERT_NE(nullptr, deletion_bitmap);
    std::vector<uint64_t> deleted_rowids(deletion_bitmap->get_cardinality());
    deletion_bitmap->to_array(deleted_rowids);
    EXPECT_EQ((std::vector<uint64_t>{2}), deleted_rowids);
}

TEST_F(IcebergDeleteBuilderTest, TestBuildPositionDeleteRejectsUnsupportedFormat) {
    RuntimeProfile runtime_profile("IcebergDeleteBuilderTest");

    FormatScanContext scan_context;
    scan_context.timezone = "UTC";

    TIcebergDeleteFile delete_file;
    delete_file.__set_full_path(_parquet_delete_path);
    delete_file.__set_length(0);
    delete_file.__set_file_format(THdfsFileFormat::AVRO);

    IcebergDeleteBuilder builder(IcebergDeleteBuilderContext{
            .scan_context = &scan_context,
            .fs = &_fs,
            .data_file_path = _parquet_data_path,
            .runtime_profile = &runtime_profile,
            .chunk_size = 4096,
    });

    auto status = builder.build_position_delete(delete_file, THdfsFileFormat::PARQUET);
    EXPECT_TRUE(status.is_not_supported()) << status;
    EXPECT_EQ(0, builder.deletion_bitmap()->get_cardinality());
}

TEST_F(IcebergDeleteBuilderTest, TestReadRowsVisitsAllRows) {
    write_parquet_delete_file(_fs, _parquet_delete_path, {{"dataA", 1}, {"dataB", 2}, {"dataA", 3}});

    ASSIGN_OR_ABORT(const int64_t delete_file_size, _fs.get_file_size(_parquet_delete_path));
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_parquet_delete_path));

    std::vector<std::pair<std::string, int64_t>> rows;
    ASSERT_OK(IcebergPositionDeleteReader::read_rows(
            file.get(), _parquet_delete_path, delete_file_size, "parquet", 4096, "UTC", FormatScannerOptions{}, nullptr,
            [&](const Slice& file_path, int64_t pos) { rows.emplace_back(file_path.to_string(), pos); }));

    const std::vector<std::pair<std::string, int64_t>> expected{{"dataA", 1}, {"dataB", 2}, {"dataA", 3}};
    EXPECT_EQ(expected, rows);
}

TEST_F(IcebergDeleteBuilderTest, TestReadRowsRejectsUnknownFormat) {
    write_parquet_delete_file(_fs, _parquet_delete_path, {{"dataA", 1}});

    ASSIGN_OR_ABORT(const int64_t delete_file_size, _fs.get_file_size(_parquet_delete_path));
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_parquet_delete_path));

    auto status = IcebergPositionDeleteReader::read_rows(file.get(), _parquet_delete_path, delete_file_size, "avro",
                                                         4096, "UTC", FormatScannerOptions{}, nullptr,
                                                         [](const Slice&, int64_t) {});
    EXPECT_FALSE(status.ok());
}

} // namespace starrocks::formats
