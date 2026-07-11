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
#include <roaring/roaring64.h>
#include <zlib.h>

#include <cstring>
#include <limits>
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
#include "gutil/endian.h"
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

// Builds an Iceberg deletion-vector-v1 blob for the given row positions:
//   [4-byte big-endian length][4-byte magic][roaring64 portable bitmap][4-byte big-endian CRC].
static std::vector<uint8_t> make_dv_blob(const std::vector<uint64_t>& positions) {
    roaring64_bitmap_t* bm = roaring64_bitmap_create();
    for (uint64_t p : positions) {
        roaring64_bitmap_add(bm, p);
    }
    size_t bitmap_size = roaring64_bitmap_portable_size_in_bytes(bm);
    std::vector<char> serialized(bitmap_size);
    roaring64_bitmap_portable_serialize(bm, serialized.data());
    roaring64_bitmap_free(bm);

    const uint32_t length = static_cast<uint32_t>(4 + bitmap_size); // magic + bitmap
    const uint32_t magic = 1681511377;                              // bytes {0xD1,0xD3,0x39,0x64}

    std::vector<uint8_t> blob(4 + 4 + bitmap_size + 4);
    const uint32_t length_be = LittleEndian::IsLittleEndian() ? BigEndian::FromHost32(length) : length;
    memcpy(blob.data(), &length_be, 4);
    const uint32_t magic_le = LittleEndian::IsLittleEndian() ? magic : LittleEndian::FromHost32(magic);
    memcpy(blob.data() + 4, &magic_le, 4);
    memcpy(blob.data() + 8, serialized.data(), bitmap_size);
    const uint32_t crc_be = BigEndian::FromHost32(crc32(0, blob.data() + 4, length));
    memcpy(blob.data() + 8 + bitmap_size, &crc_be, 4);
    return blob;
}

TEST_F(IcebergDeleteBuilderTest, TestDeletionVectorBuilderReadsBlobRange) {
    const std::string path = "/deletes.puffin";
    const auto first_blob = make_dv_blob({1, 2});
    const auto second_blob = make_dv_blob({7, 11});
    ASSIGN_OR_ABORT(auto file, _fs.new_writable_file(path));
    ASSERT_OK(file->append(Slice("PFA1")));
    ASSERT_OK(file->append(Slice(first_blob.data(), first_blob.size())));
    ASSERT_OK(file->append(Slice(second_blob.data(), second_blob.size())));
    ASSERT_OK(file->close());

    TIcebergDeleteFile delete_file;
    delete_file.__set_full_path(path);
    delete_file.__set_length(4 + first_blob.size() + second_blob.size());
    delete_file.__set_content_offset(4 + first_blob.size());
    delete_file.__set_content_size_in_bytes(second_blob.size());
    RuntimeProfile runtime_profile("IcebergDeleteBuilderTest");
    IcebergDeleteBuilder builder(IcebergDeleteBuilderContext{.fs = &_fs, .runtime_profile = &runtime_profile});
    ASSERT_OK(builder.build_deletion_vector(delete_file));
    const auto bitmap = builder.deletion_bitmap();
    ASSERT_EQ(2, bitmap->get_cardinality());
    std::vector<uint64_t> positions(bitmap->get_cardinality());
    bitmap->to_array(positions);
    EXPECT_EQ((std::vector<uint64_t>{7, 11}), positions);
}

TEST_F(IcebergDeleteBuilderTest, TestDeletionVectorBuilderReadsDeltaBin) {
    const std::string path = "/deletion_vector_00000000-0000-0000-0000-000000000001.bin";
    const std::vector<std::vector<uint64_t>> expected_positions{{1, 2}, {7, 11}};
    const std::vector<std::vector<uint8_t>> blobs{make_dv_blob(expected_positions[0]),
                                                  make_dv_blob(expected_positions[1])};
    ASSIGN_OR_ABORT(auto file, _fs.new_writable_file(path));
    const char version = 1;
    ASSERT_OK(file->append(Slice(&version, 1)));
    for (const auto& blob : blobs) {
        ASSERT_OK(file->append(Slice(blob.data(), blob.size())));
    }
    ASSERT_OK(file->close());

    RuntimeProfile runtime_profile("IcebergDeleteBuilderTest");
    int64_t offset = 1;
    for (size_t i = 0; i < blobs.size(); ++i) {
        TIcebergDeleteFile delete_file;
        delete_file.__set_full_path(path);
        delete_file.__set_content_offset(offset);
        delete_file.__set_content_size_in_bytes(blobs[i].size());
        // UniForm exports the end of this DV as file_size_in_bytes, even for a shared .bin file.
        delete_file.__set_length(offset + blobs[i].size());
        IcebergDeleteBuilder builder(IcebergDeleteBuilderContext{.fs = &_fs, .runtime_profile = &runtime_profile});
        ASSERT_OK(builder.build_deletion_vector(delete_file));
        const auto bitmap = builder.deletion_bitmap();
        std::vector<uint64_t> positions(bitmap->get_cardinality());
        bitmap->to_array(positions);
        EXPECT_EQ(expected_positions[i], positions);
        offset += blobs[i].size();
    }
}

TEST_F(IcebergDeleteBuilderTest, TestDeletionVectorBuilderRejectsInvalidRange) {
    const std::string path = "/deletes.puffin";
    const auto blob = make_dv_blob({1, 2});
    ASSIGN_OR_ABORT(auto file, _fs.new_writable_file(path));
    ASSERT_OK(file->append(Slice("PFA1")));
    ASSERT_OK(file->append(Slice(blob.data(), blob.size())));
    ASSERT_OK(file->close());

    TIcebergDeleteFile delete_file;
    delete_file.__set_full_path(path);
    delete_file.__set_length(4 + blob.size());
    IcebergDeleteBuilder builder(IcebergDeleteBuilderContext{.fs = &_fs});
    const int64_t max_offset = std::numeric_limits<int64_t>::max();
    const std::vector<std::pair<int64_t, int64_t>> ranges{{-1, blob.size()}, {4, -1},          {4, 0},
                                                          {4, 19},           {5, blob.size()}, {max_offset, 20}};
    for (const auto& [offset, size] : ranges) {
        delete_file.__set_content_offset(offset);
        delete_file.__set_content_size_in_bytes(size);
        auto status = builder.build_deletion_vector(delete_file);
        EXPECT_TRUE(status.is_invalid_argument()) << "offset=" << offset << ", size=" << size << ": " << status;
    }
    delete_file.__set_length(max_offset);
    delete_file.__set_content_offset(4);
    delete_file.__set_content_size_in_bytes(static_cast<int64_t>(std::numeric_limits<uint32_t>::max()) + 9);
    EXPECT_TRUE(builder.build_deletion_vector(delete_file).is_invalid_argument());
}

TEST(IcebergDeletionVectorBlobTest, ParseValid) {
    std::vector<uint64_t> positions = {0, 3, 7, 100, 1000000, (1ULL << 32) + 7};
    std::vector<uint8_t> blob = make_dv_blob(positions);
    DeletionBitmap bitmap(roaring64_bitmap_create());
    ASSERT_OK(
            IcebergDeleteBuilder::parse_deletion_vector_blob(blob.data(), static_cast<int64_t>(blob.size()), &bitmap));
    ASSERT_EQ(positions.size(), bitmap.get_cardinality());
    std::vector<uint64_t> arr(bitmap.get_cardinality());
    bitmap.to_array(arr);
    EXPECT_EQ(positions, arr);
}

TEST(IcebergDeletionVectorBlobTest, ParseEmpty) {
    // Empty portable bitmap with an independently computed IEEE CRC-32 (0xbf18480c).
    const std::vector<uint8_t> blob = {0x00, 0x00, 0x00, 0x0c, 0xd1, 0xd3, 0x39, 0x64, 0x00, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xbf, 0x18, 0x48, 0x0c};
    DeletionBitmap bitmap(roaring64_bitmap_create());
    ASSERT_OK(
            IcebergDeleteBuilder::parse_deletion_vector_blob(blob.data(), static_cast<int64_t>(blob.size()), &bitmap));
    EXPECT_EQ(0u, bitmap.get_cardinality());
}

TEST(IcebergDeletionVectorBlobTest, ParseRejectsCorruptBitmap) {
    auto blob = make_dv_blob({1});
    // Change a row position while leaving the bitmap structurally valid and the stored CRC unchanged.
    blob[blob.size() - 5] ^= 1;
    DeletionBitmap bitmap(roaring64_bitmap_create());
    bitmap.add_value(99);
    auto status = IcebergDeleteBuilder::parse_deletion_vector_blob(blob.data(), blob.size(), &bitmap);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("CRC-32 mismatch"));
    EXPECT_EQ(1u, bitmap.get_cardinality());
}

TEST(IcebergDeletionVectorBlobTest, ParseRejectsCorruptChecksum) {
    auto blob = make_dv_blob({1, 2});
    blob.back() ^= 1;
    DeletionBitmap bitmap(roaring64_bitmap_create());
    auto status = IcebergDeleteBuilder::parse_deletion_vector_blob(blob.data(), blob.size(), &bitmap);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("CRC-32 mismatch"));
    EXPECT_EQ(0u, bitmap.get_cardinality());
}

TEST(IcebergDeletionVectorBlobTest, ParseRejectsMalformedBitmap) {
    auto blob = make_dv_blob({});
    // Claim one Roaring bitmap but provide no key or bitmap bytes, with a matching checksum.
    blob[8] = 1;
    const uint32_t crc_be = BigEndian::FromHost32(crc32(0, blob.data() + 4, blob.size() - 8));
    memcpy(blob.data() + blob.size() - 4, &crc_be, 4);
    DeletionBitmap bitmap(roaring64_bitmap_create());
    auto status = IcebergDeleteBuilder::parse_deletion_vector_blob(blob.data(), blob.size(), &bitmap);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("Failed to deserialize"));
    EXPECT_EQ(0u, bitmap.get_cardinality());
}

TEST(IcebergDeletionVectorBlobTest, ParseBadMagic) {
    std::vector<uint8_t> blob = make_dv_blob({1, 2, 3});
    blob[4] ^= 0xFF;
    DeletionBitmap bitmap(roaring64_bitmap_create());
    EXPECT_FALSE(
            IcebergDeleteBuilder::parse_deletion_vector_blob(blob.data(), static_cast<int64_t>(blob.size()), &bitmap)
                    .ok());
}

TEST(IcebergDeletionVectorBlobTest, ParseBadLength) {
    std::vector<uint8_t> blob = make_dv_blob({1, 2, 3});
    blob[0] ^= 0xFF;
    DeletionBitmap bitmap(roaring64_bitmap_create());
    EXPECT_FALSE(
            IcebergDeleteBuilder::parse_deletion_vector_blob(blob.data(), static_cast<int64_t>(blob.size()), &bitmap)
                    .ok());
}

TEST(IcebergDeletionVectorBlobTest, ParseTooSmall) {
    std::vector<uint8_t> blob(8, 0);
    DeletionBitmap bitmap(roaring64_bitmap_create());
    EXPECT_FALSE(
            IcebergDeleteBuilder::parse_deletion_vector_blob(blob.data(), static_cast<int64_t>(blob.size()), &bitmap)
                    .ok());
}

TEST(IcebergDeletionVectorBlobTest, MergeAccumulatesAcrossBlobs) {
    DeletionBitmap bitmap(roaring64_bitmap_create());
    std::vector<uint8_t> b1 = make_dv_blob({1, 2});
    std::vector<uint8_t> b2 = make_dv_blob({2, 3, 4});
    ASSERT_OK(IcebergDeleteBuilder::parse_deletion_vector_blob(b1.data(), static_cast<int64_t>(b1.size()), &bitmap));
    ASSERT_OK(IcebergDeleteBuilder::parse_deletion_vector_blob(b2.data(), static_cast<int64_t>(b2.size()), &bitmap));
    EXPECT_EQ(4u, bitmap.get_cardinality()); // {1, 2, 3, 4}
}

} // namespace starrocks::formats
