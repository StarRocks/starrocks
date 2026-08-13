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
#include <zlib.h>

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "cache/datacache.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "cache/scan/cache_input_stream.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "column/chunk.h"
#include "common/config_cache_fwd.h"
#include "formats/column_evaluator.h"
#include "formats/file_input_stream.h"
#include "formats/io/async_flush_output_stream.h"
#include "formats/orc/orc_file_writer.h"
#include "formats/parquet/file_writer.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/puffin/deletion_vector_blob.h"
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

// ===== Iceberg V3 deletion vectors =====
// Migrated from the retired IcebergDeletionVectorReader: the DV read path now lives in
// IcebergDeleteBuilder, so these guard the same IO / cache / profile behaviour there.

namespace {

// Build a complete DV blob, optionally corrupting one framing field.
std::vector<uint8_t> make_dv_blob(const std::vector<uint64_t>& positions, bool corrupt_magic = false) {
    roaring64_bitmap_t* b = roaring64_bitmap_create();
    for (uint64_t p : positions) {
        roaring64_bitmap_add(b, p);
    }
    size_t body_len = roaring64_bitmap_portable_size_in_bytes(b);
    std::vector<char> body(body_len);
    roaring64_bitmap_portable_serialize(b, body.data());
    roaring::api::roaring64_bitmap_free(b);

    int64_t size = 4 + 4 + static_cast<int64_t>(body_len) + 4;
    std::vector<uint8_t> blob(size);
    BigEndian::Store32(blob.data(), static_cast<uint32_t>(size - 8));
    blob[4] = 0xD1;
    blob[5] = 0xD3;
    blob[6] = 0x39;
    blob[7] = corrupt_magic ? 0x00 : 0x64;
    memcpy(blob.data() + 8, body.data(), body_len);
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, reinterpret_cast<const Bytef*>(blob.data() + 4), static_cast<uInt>(4 + body_len));
    BigEndian::Store32(blob.data() + size - 4, static_cast<uint32_t>(crc));
    return blob;
}

} // namespace

class IcebergDvBuilderTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = "./iceberg_dv_builder_test_" + std::to_string(::getpid());
        std::filesystem::create_directories(_test_dir);
    }
    void TearDown() override {
        if (std::filesystem::exists(_test_dir)) {
            std::filesystem::remove_all(_test_dir);
        }
    }

    std::string write_file(const std::string& name, const std::vector<uint8_t>& bytes) {
        std::string path = _test_dir + "/" + name;
        ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(path));
        CHECK_OK(wf->append(Slice(reinterpret_cast<const char*>(bytes.data()), bytes.size())));
        CHECK_OK(wf->close());
        return path;
    }

    // A DV travels as a delete file: full_path/length describe the Puffin file, the nested blob
    // locates the vector inside it.
    static TIcebergDeleteFile make_dv_delete_file(const std::string& puffin_path, int64_t puffin_size, int64_t offset,
                                                  int64_t size, int64_t record_count,
                                                  const std::string& referenced_data_file = kDataFile) {
        TIcebergDeleteFile df;
        df.__set_full_path(puffin_path);
        df.__set_file_content(TIcebergFileContent::POSITION_DELETES);
        df.__set_length(puffin_size);
        TIcebergDeletionVectorBlob blob;
        blob.__set_content_offset(offset);
        blob.__set_content_size_in_bytes(size);
        blob.__set_record_count(record_count);
        blob.__set_referenced_data_file(referenced_data_file);
        df.__set_deletion_vector(blob);
        return df;
    }

    IcebergDeleteBuilderContext make_ctx(RuntimeProfile* profile = nullptr, bool enable_cache = false,
                                         const std::string& candidate_node = "") {
        return IcebergDeleteBuilderContext{
                .scan_context = &_scan_context,
                .fs = FileSystem::Default(),
                .data_file_path = kDataFile,
                .datacache_options =
                        enable_cache ? DataCacheOptions{.enable_datacache = true, .enable_populate_datacache = true}
                                     : DataCacheOptions{},
                .candidate_node = candidate_node,
                .runtime_profile = profile,
                .chunk_size = 4096,
        };
    }

    static int64_t counter_value(RuntimeProfile& profile, const std::string& name) {
        auto* counter = profile.get_counter(name);
        EXPECT_NE(nullptr, counter) << "missing counter " << name;
        return counter == nullptr ? -1 : counter->value();
    }

    static constexpr const char* kDataFile = "data.parquet";
    FormatScanContext _scan_context;
    std::string _test_dir;
};

TEST_F(IcebergDvBuilderTest, BuildsBitmapFromBlob) {
    auto blob = make_dv_blob({3, 7, 42});
    std::string path = write_file("dv.puffin", blob);

    IcebergDeleteBuilder builder(make_ctx());
    auto df = make_dv_delete_file(path, static_cast<int64_t>(blob.size()), 0, static_cast<int64_t>(blob.size()), 3);
    ASSERT_OK(builder.build_deletion_vector(df));
    EXPECT_EQ(3, builder.deletion_bitmap()->get_cardinality());
}

// The blob is read strictly from [content_offset, content_offset + content_size_in_bytes).
TEST_F(IcebergDvBuilderTest, RespectsBlobOffset) {
    auto blob0 = make_dv_blob({1, 2});
    auto blob1 = make_dv_blob({100, 200, 300});
    std::vector<uint8_t> file;
    file.insert(file.end(), blob0.begin(), blob0.end());
    file.insert(file.end(), blob1.begin(), blob1.end());
    std::string path = write_file("dv_multi.puffin", file);

    IcebergDeleteBuilder builder(make_ctx());
    auto df = make_dv_delete_file(path, static_cast<int64_t>(file.size()), static_cast<int64_t>(blob0.size()),
                                  static_cast<int64_t>(blob1.size()), 3);
    ASSERT_OK(builder.build_deletion_vector(df));

    std::vector<uint64_t> actual(builder.deletion_bitmap()->get_cardinality());
    builder.deletion_bitmap()->to_array(actual);
    EXPECT_EQ((std::vector<uint64_t>{100, 200, 300}), actual);
}

// A corrupt blob surfaces as Corruption with the puffin location in the message.
TEST_F(IcebergDvBuilderTest, CorruptBlobIsCorruption) {
    auto blob = make_dv_blob({1, 2}, /*corrupt_magic=*/true);
    std::string path = write_file("dv_bad.puffin", blob);

    IcebergDeleteBuilder builder(make_ctx());
    auto df = make_dv_delete_file(path, static_cast<int64_t>(blob.size()), 0, static_cast<int64_t>(blob.size()), 2);
    auto st = builder.build_deletion_vector(df);
    EXPECT_TRUE(st.is_corruption());
    EXPECT_NE(std::string::npos, st.message().find("dv_bad.puffin"));
    EXPECT_EQ(0, builder.deletion_bitmap()->get_cardinality());
}

TEST_F(IcebergDvBuilderTest, MissingFileIsError) {
    IcebergDeleteBuilder builder(make_ctx());
    auto df = make_dv_delete_file(_test_dir + "/does_not_exist.puffin", 32, 0, 32, 1);
    EXPECT_FALSE(builder.build_deletion_vector(df).ok());
}

// A content range that runs past the Puffin file must fail as Corruption before the read, not
// become a huge allocation.
TEST_F(IcebergDvBuilderTest, OutOfBoundsBlobRangeIsCorruption) {
    auto blob = make_dv_blob({1, 2});
    std::string path = write_file("dv_oob.puffin", blob);
    const auto puffin_size = static_cast<int64_t>(blob.size());

    IcebergDeleteBuilder builder(make_ctx());
    // Claim a blob far larger than the whole Puffin, the shape a corrupt manifest produces.
    auto df = make_dv_delete_file(path, puffin_size, 0, puffin_size + (int64_t{1} << 40), 2);
    auto st = builder.build_deletion_vector(df);
    EXPECT_TRUE(st.is_corruption()) << st.message();
    EXPECT_EQ(0, builder.deletion_bitmap()->get_cardinality());
}

// An offset that sits inside the file but whose range spills past EOF is equally rejected.
TEST_F(IcebergDvBuilderTest, BlobRangeSpillingPastEofIsCorruption) {
    auto blob = make_dv_blob({1, 2});
    std::string path = write_file("dv_spill.puffin", blob);
    const auto puffin_size = static_cast<int64_t>(blob.size());

    IcebergDeleteBuilder builder(make_ctx());
    auto df = make_dv_delete_file(path, puffin_size, puffin_size - 4, 16, 2);
    EXPECT_TRUE(builder.build_deletion_vector(df).is_corruption());
}

// length carries the Puffin size; unset it and the file would be opened as a 0-byte view.
TEST_F(IcebergDvBuilderTest, MissingPuffinLengthIsError) {
    auto blob = make_dv_blob({1, 2});
    std::string path = write_file("dv_no_length.puffin", blob);

    auto df = make_dv_delete_file(path, static_cast<int64_t>(blob.size()), 0, static_cast<int64_t>(blob.size()), 2);
    df.__isset.length = false;

    IcebergDeleteBuilder builder(make_ctx());
    auto st = builder.build_deletion_vector(df);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.message().find("puffin file length"));
}

// An unset referenced_data_file is the assembly bug the check exists for, so it must not pass.
TEST_F(IcebergDvBuilderTest, UnsetReferencedDataFileIsError) {
    auto blob = make_dv_blob({1, 2});
    std::string path = write_file("dv_unset_ref.puffin", blob);

    auto df = make_dv_delete_file(path, static_cast<int64_t>(blob.size()), 0, static_cast<int64_t>(blob.size()), 2);
    df.deletion_vector.__isset.referenced_data_file = false;

    IcebergDeleteBuilder builder(make_ctx());
    EXPECT_FALSE(builder.build_deletion_vector(df).ok());
}

TEST_F(IcebergDvBuilderTest, MissingBlobDescriptorIsError) {
    auto blob = make_dv_blob({1});
    std::string path = write_file("dv_no_desc.puffin", blob);

    TIcebergDeleteFile df;
    df.__set_full_path(path);
    df.__set_length(static_cast<int64_t>(blob.size()));

    IcebergDeleteBuilder builder(make_ctx());
    EXPECT_FALSE(builder.build_deletion_vector(df).ok());
}

// A DV pointing at a different data file means the scan range was assembled wrong.
TEST_F(IcebergDvBuilderTest, ReferencedDataFileMismatchIsError) {
    auto blob = make_dv_blob({1, 2});
    std::string path = write_file("dv_wrong_ref.puffin", blob);

    IcebergDeleteBuilder builder(make_ctx());
    auto df = make_dv_delete_file(path, static_cast<int64_t>(blob.size()), 0, static_cast<int64_t>(blob.size()), 2,
                                  "some_other_file.parquet");
    auto st = builder.build_deletion_vector(df);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.message().find("some_other_file.parquet"));
}

TEST_F(IcebergDvBuilderTest, PublishesDvCounters) {
    auto blob = make_dv_blob({1, 2, 3, 4});
    std::string path = write_file("dv_profile.puffin", blob);

    RuntimeProfile profile("IcebergDvBuilderProfile");
    IcebergDeleteBuilder builder(make_ctx(&profile));
    auto df = make_dv_delete_file(path, static_cast<int64_t>(blob.size()), 0, static_cast<int64_t>(blob.size()), 4);
    ASSERT_OK(builder.build_deletion_vector(df));

    EXPECT_EQ(4, counter_value(profile, "IcebergDVCardinality"));
    EXPECT_EQ(1, counter_value(profile, "IcebergDVBuildCount"));
    // Whole-build timer must be published and cover more than its phases: the bitmap merge and
    // the buffer allocation live outside read/checksum/deserialize.
    const int64_t build_ns = counter_value(profile, "IcebergDVBuildTime");
    EXPECT_GT(build_ns, 0);
    EXPECT_GE(build_ns, counter_value(profile, "IcebergDVReadTime") + counter_value(profile, "IcebergDVChecksumTime") +
                                counter_value(profile, "IcebergDVDeserializeTime"));
    EXPECT_EQ(static_cast<int64_t>(blob.size()), counter_value(profile, "IcebergDVReadBytes"));
    EXPECT_NE(nullptr, profile.get_counter("IcebergDVReadTime"));
    EXPECT_NE(nullptr, profile.get_counter("IcebergDVDeserializeTime"));
    EXPECT_NE(nullptr, profile.get_counter("IcebergDVChecksumTime"));
    // The v2 MOR section belongs to position deletes only.
    EXPECT_EQ(nullptr, profile.get_counter("MOR_AppIOBytesRead"));
}

// A DV and a position-delete file for the same data file must union, not overwrite. Iceberg's
// DeleteFileIndex does not currently hand both to one scan range, so this pins the merge semantics
// independently of that upstream invariant.
TEST_F(IcebergDvBuilderTest, MergesWithPositionDeletes) {
    auto blob = make_dv_blob({2, 3, 4});
    std::string path = write_file("dv_merge.puffin", blob);

    IcebergDeleteBuilder builder(make_ctx());
    // Seed the bitmap the way a position-delete pass would, including a row the DV repeats.
    builder.deletion_bitmap()->add_value(1);
    builder.deletion_bitmap()->add_value(2);

    auto df = make_dv_delete_file(path, static_cast<int64_t>(blob.size()), 0, static_cast<int64_t>(blob.size()), 3);
    ASSERT_OK(builder.build_deletion_vector(df));

    std::vector<uint64_t> actual(builder.deletion_bitmap()->get_cardinality());
    builder.deletion_bitmap()->to_array(actual);
    EXPECT_EQ((std::vector<uint64_t>{1, 2, 3, 4}), actual) << "union, with the duplicate counted once";
}

#ifdef WITH_STARCACHE

class IcebergDvBuilderCacheTest : public IcebergDvBuilderTest {
protected:
    void SetUp() override {
        IcebergDvBuilderTest::SetUp();
        auto cache_options = TestCacheUtils::create_simple_options(config::datacache_block_size, 50 * MB);
        _block_cache = TestCacheUtils::create_cache(cache_options);
        ASSERT_NE(nullptr, _block_cache);
        DataCache::GetInstance()->set_block_cache(_block_cache);
    }

    void TearDown() override {
        DataCache::GetInstance()->set_block_cache(nullptr);
        _block_cache.reset();
        IcebergDvBuilderTest::TearDown();
    }

    std::shared_ptr<BlockCache> _block_cache;
};

TEST_F(IcebergDvBuilderCacheTest, FirstReadPopulatesCache) {
    auto blob = make_dv_blob({3, 7, 42});
    std::string path = write_file("dv_cache_populate.puffin", blob);
    const auto blob_size = static_cast<int64_t>(blob.size());

    RuntimeProfile profile("IcebergDvCachePopulate");
    IcebergDeleteBuilder builder(make_ctx(&profile, /*enable_cache=*/true));
    ASSERT_OK(builder.build_deletion_vector(make_dv_delete_file(path, blob_size, 0, blob_size, 3)));

    EXPECT_EQ(3, builder.deletion_bitmap()->get_cardinality());
    // V3 DVs publish into their own section, never into ICEBERG_V2_MOR.
    EXPECT_EQ(0, counter_value(profile, "IcebergDV_DataCacheReadCounter"));
    EXPECT_EQ(1, counter_value(profile, "IcebergDV_DataCacheWriteCounter"));
    EXPECT_EQ(blob_size, counter_value(profile, "IcebergDV_DataCacheWriteBytes"));
    EXPECT_EQ(nullptr, profile.get_counter("MOR_DataCacheWriteCounter"))
            << "a pure-V3 DV must not report v2 merge-on-read load";
    // No candidate node is configured, so all five peer counters must stay at zero.
    EXPECT_EQ(0, counter_value(profile, "IcebergDV_DataCacheReadPeerCounter"));
    EXPECT_EQ(0, counter_value(profile, "IcebergDV_DataCacheReadPeerBytes"));
    EXPECT_EQ(0, counter_value(profile, "IcebergDV_DataCacheReadPeerTimer"));
    EXPECT_EQ(0, counter_value(profile, "IcebergDV_DataCacheSkipReadPeerCounter"));
    EXPECT_EQ(0, counter_value(profile, "IcebergDV_DataCacheSkipReadPeerBytes"));
}

// Two blobs of one puffin share a cache block, so the second read hits what the first wrote.
// The DV read must register no io_ranges: a tiny blob inside a large Puffin has to pull only the
// blob, not the io_coalesce_read_max_buffer_size span a whole-file range registration would.
// Asserted through real cache traffic so it exercises build_deletion_vector, not a hand-built
// stream — see design doc 5.1.
TEST_F(IcebergDvBuilderCacheTest, DvReadPullsOnlyTheBlobFromALargePuffin) {
    auto blob = make_dv_blob({1, 2, 3});
    const auto blob_size = static_cast<int64_t>(blob.size());
    // 4 MB of padding after the blob; a whole-file range registration would read megabytes.
    std::vector<uint8_t> puffin = blob;
    puffin.resize(blob.size() + 4 * 1024 * 1024, 'p');
    std::string path = write_file("dv_large_puffin.puffin", puffin);

    RuntimeProfile profile("IcebergDvLargePuffin");
    IcebergDeleteBuilder builder(make_ctx(&profile, /*enable_cache=*/true));
    ASSERT_OK(builder.build_deletion_vector(
            make_dv_delete_file(path, static_cast<int64_t>(puffin.size()), 0, blob_size, 3)));
    EXPECT_EQ(3, builder.deletion_bitmap()->get_cardinality());

    EXPECT_EQ(blob_size, counter_value(profile, "IcebergDVReadBytes"));
    // Cache writes are block-aligned, so allow one block of slack but nothing near 4 MB.
    const int64_t written = counter_value(profile, "IcebergDV_DataCacheWriteBytes");
    EXPECT_GT(written, 0);
    EXPECT_LE(written, static_cast<int64_t>(config::datacache_block_size))
            << "DV read pulled " << written << " bytes for a " << blob_size << "-byte blob: io_ranges were registered";
}

// candidate_node must reach CacheInputStream: set_peer_cache_node returns early on an empty
// string, so without a non-empty node the whole peer path is silently inert. If the
// enterprise-only `.candidate_node = ...` line in the scanner is ever dropped (the likeliest
// casualty of an upstream sync), a local miss stops falling back to the peer that cache select
// warmed. Every other DV case runs with an empty node, so only this one would catch it.
TEST_F(IcebergDvBuilderCacheTest, CandidateNodeReachesPeerCachePath) {
    auto blob = make_dv_blob({5, 6});
    std::string path = write_file("dv_peer.puffin", blob);
    const auto blob_size = static_cast<int64_t>(blob.size());

    RuntimeProfile profile("IcebergDvPeer");
    IcebergDeleteBuilder builder(make_ctx(&profile, /*enable_cache=*/true, /*candidate_node=*/"127.0.0.1:8060"));
    auto df = make_dv_delete_file(path, blob_size, 0, blob_size, 2);
    ASSERT_OK(builder.build_deletion_vector(df));
    EXPECT_EQ(2, builder.deletion_bitmap()->get_cardinality());

    // Asserting on the counters alone would not catch a dropped set_peer_cache_node(): they are
    // registered whenever a CacheInputStream exists. Open the same file through the same helper
    // and inspect the parsed peer address instead (the test target builds with
    // -fno-access-control, so the private state is reachable).
    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    std::shared_ptr<SharedBufferedInputStream> shared_stream;
    std::shared_ptr<CacheInputStream> cache_stream;
    ASSIGN_OR_ABORT(auto file, builder.open_cached_file(df, fs_stats, app_stats, shared_stream, cache_stream));
    ASSERT_NE(nullptr, cache_stream);
    EXPECT_EQ("127.0.0.1", cache_stream->_peer_host);
    EXPECT_EQ(8060, cache_stream->_peer_port);
    EXPECT_TRUE(cache_stream->_can_try_peer_cache());
}

// An empty candidate_node must leave the peer path disabled rather than half-configured.
TEST_F(IcebergDvBuilderCacheTest, EmptyCandidateNodeLeavesPeerCacheOff) {
    auto blob = make_dv_blob({5, 6});
    std::string path = write_file("dv_no_peer.puffin", blob);
    const auto blob_size = static_cast<int64_t>(blob.size());

    IcebergDeleteBuilder builder(make_ctx(nullptr, /*enable_cache=*/true));
    auto df = make_dv_delete_file(path, blob_size, 0, blob_size, 2);

    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    std::shared_ptr<SharedBufferedInputStream> shared_stream;
    std::shared_ptr<CacheInputStream> cache_stream;
    ASSIGN_OR_ABORT(auto file, builder.open_cached_file(df, fs_stats, app_stats, shared_stream, cache_stream));
    ASSERT_NE(nullptr, cache_stream);
    EXPECT_FALSE(cache_stream->_can_try_peer_cache());
}

TEST_F(IcebergDvBuilderCacheTest, SecondBlobOfSamePuffinHitsCache) {
    auto blob0 = make_dv_blob({1, 2});
    auto blob1 = make_dv_blob({100, 200, 300});
    std::vector<uint8_t> file;
    file.insert(file.end(), blob0.begin(), blob0.end());
    file.insert(file.end(), blob1.begin(), blob1.end());
    std::string path = write_file("dv_cache_two_blobs.puffin", file);
    const auto file_size = static_cast<int64_t>(file.size());

    RuntimeProfile first_profile("IcebergDvCacheFirst");
    IcebergDeleteBuilder first(make_ctx(&first_profile, true));
    ASSERT_OK(first.build_deletion_vector(
            make_dv_delete_file(path, file_size, 0, static_cast<int64_t>(blob0.size()), 2)));
    EXPECT_EQ(1, counter_value(first_profile, "IcebergDV_DataCacheWriteCounter"));

    RuntimeProfile second_profile("IcebergDvCacheSecond");
    IcebergDeleteBuilder second(make_ctx(&second_profile, true));
    ASSERT_OK(second.build_deletion_vector(make_dv_delete_file(path, file_size, static_cast<int64_t>(blob0.size()),
                                                               static_cast<int64_t>(blob1.size()), 3)));

    std::vector<uint64_t> actual(second.deletion_bitmap()->get_cardinality());
    second.deletion_bitmap()->to_array(actual);
    EXPECT_EQ((std::vector<uint64_t>{100, 200, 300}), actual);
    EXPECT_EQ(1, counter_value(second_profile, "IcebergDV_DataCacheReadCounter"));
    EXPECT_EQ(0, counter_value(second_profile, "IcebergDV_DataCacheWriteCounter"));
}

#endif // WITH_STARCACHE

} // namespace starrocks::formats
