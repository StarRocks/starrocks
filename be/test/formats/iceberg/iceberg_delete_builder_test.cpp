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

#include <cstring>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "formats/column_evaluator.h"
#include "formats/parquet/file_writer.h"
#include "formats/parquet/parquet_file_writer.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "gutil/endian.h"
#include "runtime/current_thread.h"
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
    }

    void TearDown() override {
        tls_thread_status.set_mem_tracker(nullptr);
        CurrentThread::set_mem_tracker_source(nullptr, nullptr);
        g_iceberg_delete_builder_test_mem_tracker = nullptr;
    }

    const std::string _parquet_delete_path = "/iceberg_position_delete.parquet";
    const std::string _parquet_data_path = "parquet_data_file.parquet";
    MemoryFileSystem _fs;
    MemTracker _mem_tracker{-1, "iceberg_delete_builder_test"};
};

TEST_F(IcebergDeleteBuilderTest, TestParquetBuilder) {
    RuntimeProfile runtime_profile("IcebergDeleteBuilderTest");

    std::vector type_descs{TypeDescriptor::from_logical_type(TYPE_VARCHAR),
                           TypeDescriptor::from_logical_type(TYPE_BIGINT)};
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<ParquetWriterOptions>();
    writer_options->column_ids = {FileColumnId{IcebergDeleteFileMeta::get_delete_file_path_slot().id(), {}},
                                  FileColumnId{IcebergDeleteFileMeta::get_delete_file_pos_slot().id(), {}}};
    ASSIGN_OR_ABORT(auto writable_file, _fs.new_writable_file(_parquet_delete_path));
    auto output_stream = std::make_shared<parquet::ParquetOutputStream>(std::move(writable_file));
    ParquetFileWriter writer(_parquet_delete_path, std::move(output_stream), {"file_path", "pos"}, type_descs,
                             std::move(column_evaluators), TCompressionType::NO_COMPRESSION, std::move(writer_options),
                             [] {}, {false, false});
    ASSERT_OK(writer.init());

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnTestHelper::build_column<Slice>(
                                 {_parquet_data_path, "another_data_file.parquet", _parquet_data_path}),
                         0);
    chunk->append_column(ColumnTestHelper::build_column<int64_t>({7, 9, 11}), 1);
    ASSERT_OK(writer.write(chunk.get()));
    ASSERT_OK(writer.close().io_status);

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
    const uint32_t crc = 0;                                         // parser does not verify the CRC

    std::vector<uint8_t> blob(4 + 4 + bitmap_size + 4);
    const uint32_t length_be = LittleEndian::IsLittleEndian() ? BigEndian::FromHost32(length) : length;
    memcpy(blob.data(), &length_be, 4);
    const uint32_t magic_le = LittleEndian::IsLittleEndian() ? magic : LittleEndian::FromHost32(magic);
    memcpy(blob.data() + 4, &magic_le, 4);
    memcpy(blob.data() + 8, serialized.data(), bitmap_size);
    memcpy(blob.data() + 8 + bitmap_size, &crc, 4);
    return blob;
}

TEST(IcebergDeletionVectorBlobTest, ParseValid) {
    std::vector<uint64_t> positions = {0, 3, 7, 100, 1000000};
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
    std::vector<uint8_t> blob = make_dv_blob({});
    DeletionBitmap bitmap(roaring64_bitmap_create());
    ASSERT_OK(
            IcebergDeleteBuilder::parse_deletion_vector_blob(blob.data(), static_cast<int64_t>(blob.size()), &bitmap));
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
