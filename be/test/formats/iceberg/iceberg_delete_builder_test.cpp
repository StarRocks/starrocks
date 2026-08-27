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
