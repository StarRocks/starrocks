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

#include "storage/rows_mapper.h"

#include <filesystem>

#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/data_dir.h"
#include "storage/storage_engine.h"
#include "testutil/assert.h"
#include "util/coding.h"

namespace starrocks {

class RowsMapperTest : public testing::Test {
public:
    RowsMapperTest() {}

protected:
    constexpr static const char* kTestDirectory = "./test_rows_mapper/";

    void SetUp() override { ASSERT_OK(fs::create_directories(kTestDirectory)); }

    void TearDown() override { (void)fs::remove_all(kTestDirectory); }

    DataDir* get_stores() {
        TCreateTabletReq request;
        return StorageEngine::instance()->get_stores_for_create_tablet(request.storage_medium)[0];
    }

    // generate id between [start, end)
    void generate_rssid_rowids(std::vector<uint64_t>* rssid_rowids, uint64_t start, size_t end, uint64_t rssid) {
        for (uint64_t i = start; i < end; i++) {
            rssid_rowids->push_back((rssid << 32) | i);
        }
    }

    // Ensure the parent directory of a file exists
    Status ensure_parent_dir_exists(const std::string& filename) {
        std::filesystem::path file_path(filename);
        std::filesystem::path parent_dir = file_path.parent_path();
        if (!parent_dir.empty() && !fs::path_exist(parent_dir.string())) {
            RETURN_IF_ERROR(fs::create_directories(parent_dir.string()));
        }
        return Status::OK();
    }

    // Create a rows mapper file manually for testing
    Status create_rows_mapper_file(const std::string& filename, const std::vector<uint64_t>& rssid_rowids) {
        RETURN_IF_ERROR(ensure_parent_dir_exists(filename));
        RowsMapperBuilder builder(filename);
        if (!rssid_rowids.empty()) {
            RETURN_IF_ERROR(builder.append(rssid_rowids));
        }
        return builder.finalize();
    }

    // Create a corrupted rows mapper file (wrong file size)
    Status create_corrupted_file_wrong_size(const std::string& filename) {
        RETURN_IF_ERROR(ensure_parent_dir_exists(filename));
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(filename));
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(opts, filename));
        // Write some data but with incorrect format
        std::vector<uint64_t> data = {1, 2, 3};
        RETURN_IF_ERROR(wfile->append(Slice((const char*)data.data(), data.size() * 8)));
        // Write row count (wrong value - doesn't match actual data)
        std::string row_count_str;
        put_fixed64_le(&row_count_str, 100); // Wrong row count
        RETURN_IF_ERROR(wfile->append(row_count_str));
        // Write checksum
        std::string checksum_str;
        put_fixed32_le(&checksum_str, 0);
        RETURN_IF_ERROR(wfile->append(checksum_str));
        return wfile->close();
    }

    // Create a file with wrong checksum
    Status create_file_wrong_checksum(const std::string& filename, const std::vector<uint64_t>& rssid_rowids) {
        RETURN_IF_ERROR(ensure_parent_dir_exists(filename));
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(filename));
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(opts, filename));
        // Write data
        if (!rssid_rowids.empty()) {
            RETURN_IF_ERROR(wfile->append(Slice((const char*)rssid_rowids.data(), rssid_rowids.size() * 8)));
        }
        // Write correct row count
        std::string row_count_str;
        put_fixed64_le(&row_count_str, rssid_rowids.size());
        RETURN_IF_ERROR(wfile->append(row_count_str));
        // Write wrong checksum
        std::string checksum_str;
        put_fixed32_le(&checksum_str, 12345); // Wrong checksum
        RETURN_IF_ERROR(wfile->append(checksum_str));
        return wfile->close();
    }

    // Create a file that's too small
    Status create_too_small_file(const std::string& filename) {
        RETURN_IF_ERROR(ensure_parent_dir_exists(filename));
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(filename));
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(opts, filename));
        // Write only a few bytes (less than minimum required 12 bytes)
        std::string small_data = "small";
        RETURN_IF_ERROR(wfile->append(small_data));
        return wfile->close();
    }
};

// ============================================================================
// RowsMapperBuilder Tests
// ============================================================================

TEST_F(RowsMapperTest, test_write_read) {
    const std::string filename = std::string(kTestDirectory) + "test_write_read.crm";
    RowsMapperBuilder builder(filename);
    std::vector<uint64_t> rssid_rowids;
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_FALSE(fs::path_exist(filename));
    generate_rssid_rowids(&rssid_rowids, 0, 1000, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    rssid_rowids.clear();
    generate_rssid_rowids(&rssid_rowids, 1000, 3000, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // read from file
    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));
    for (uint32_t i = 0; i < 3000; i += 100) {
        std::vector<uint64_t> rows_mapper;
        ASSERT_OK(iterator.next_values(100, &rows_mapper));
        ASSERT_TRUE(rows_mapper.size() == 100);
        for (uint32_t j = 0; j < rows_mapper.size(); j++) {
            ASSERT_TRUE((rows_mapper[j] >> 32) == 11);
            ASSERT_TRUE((rows_mapper[j] & 0xFFFFFFFF) == i + j);
        }
    }
    ASSERT_OK(iterator.status());
    // should eof
    std::vector<uint64_t> rows_mapper;
    ASSERT_TRUE(iterator.next_values(1, &rows_mapper).is_end_of_file());
}

TEST_F(RowsMapperTest, test_write_read_multi_segment) {
    const std::string filename = std::string(kTestDirectory) + "test_write_read_multi_segment.crm";
    RowsMapperBuilder builder(filename);
    std::vector<uint64_t> rssid_rowids;
    // rssid = 11
    generate_rssid_rowids(&rssid_rowids, 0, 1000, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    rssid_rowids.clear();
    // rssid = 43
    generate_rssid_rowids(&rssid_rowids, 1000, 3000, 43);
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // read from file
    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));
    for (uint32_t i = 0; i < 3000; i += 100) {
        std::vector<uint64_t> rows_mapper;
        ASSERT_OK(iterator.next_values(100, &rows_mapper));
        ASSERT_TRUE(rows_mapper.size() == 100);
        for (uint32_t j = 0; j < rows_mapper.size(); j++) {
            if (i + j < 1000) {
                ASSERT_TRUE((rows_mapper[j] >> 32) == 11);
                ASSERT_TRUE((rows_mapper[j] & 0xFFFFFFFF) == i + j);
            } else {
                ASSERT_TRUE((rows_mapper[j] >> 32) == 43);
                ASSERT_TRUE((rows_mapper[j] & 0xFFFFFFFF) == i + j);
            }
        }
    }
    ASSERT_OK(iterator.status());
    // should eof
    std::vector<uint64_t> rows_mapper;
    ASSERT_TRUE(iterator.next_values(1, &rows_mapper).is_end_of_file());
}

TEST_F(RowsMapperTest, test_empty_finalize) {
    // Test finalize without appending any data
    const std::string filename = std::string(kTestDirectory) + "test_empty_finalize.crm";
    RowsMapperBuilder builder(filename);
    ASSERT_OK(builder.finalize());
    // File should not be created
    ASSERT_FALSE(fs::path_exist(filename));
}

TEST_F(RowsMapperTest, test_empty_append_only) {
    // Test appending only empty vectors
    const std::string filename = std::string(kTestDirectory) + "test_empty_append_only.crm";
    RowsMapperBuilder builder(filename);
    std::vector<uint64_t> empty_rssid_rowids;
    ASSERT_OK(builder.append(empty_rssid_rowids));
    ASSERT_OK(builder.append(empty_rssid_rowids));
    ASSERT_OK(builder.finalize());
    // File should not be created since no actual data was appended
    ASSERT_FALSE(fs::path_exist(filename));
}

// ============================================================================
// RowsMapperIterator Tests
// ============================================================================

TEST_F(RowsMapperTest, test_iterator_next_values_zero) {
    // Test next_values with fetch_cnt = 0
    const std::string filename = std::string(kTestDirectory) + "test_iterator_zero.crm";
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(&rssid_rowids, 0, 100, 11);
    ASSERT_OK(create_rows_mapper_file(filename, rssid_rowids));

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));
    std::vector<uint64_t> result;
    // Fetching 0 rows should succeed
    ASSERT_OK(iterator.next_values(0, &result));
    ASSERT_TRUE(result.empty());
}

TEST_F(RowsMapperTest, test_iterator_corrupted_file_wrong_size) {
    // Test opening a file with mismatched row count and file size
    const std::string filename = std::string(kTestDirectory) + "test_corrupted_wrong_size.crm";
    ASSERT_OK(create_corrupted_file_wrong_size(filename));

    RowsMapperIterator iterator;
    auto st = iterator.open(filename);
    ASSERT_TRUE(st.is_corruption()) << st;
}

TEST_F(RowsMapperTest, test_iterator_wrong_checksum) {
    // Test reading a file with wrong checksum
    const std::string filename = std::string(kTestDirectory) + "test_wrong_checksum.crm";
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(&rssid_rowids, 0, 100, 11);
    ASSERT_OK(create_file_wrong_checksum(filename, rssid_rowids));

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));

    // Read all data
    std::vector<uint64_t> result;
    ASSERT_OK(iterator.next_values(100, &result));

    // status() should report checksum mismatch
    auto st = iterator.status();
    ASSERT_TRUE(st.is_corruption()) << st;
}

TEST_F(RowsMapperTest, test_iterator_status_row_count_mismatch) {
    // Test status() when not all rows are read
    const std::string filename = std::string(kTestDirectory) + "test_status_mismatch.crm";
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(&rssid_rowids, 0, 100, 11);
    ASSERT_OK(create_rows_mapper_file(filename, rssid_rowids));

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));

    // Read only 50 rows instead of all 100
    std::vector<uint64_t> result;
    ASSERT_OK(iterator.next_values(50, &result));
    ASSERT_EQ(50, result.size());

    // status() should report row count mismatch
    auto st = iterator.status();
    ASSERT_TRUE(st.is_corruption()) << st;
}

TEST_F(RowsMapperTest, test_iterator_eof_in_middle) {
    // Test reading beyond available rows
    const std::string filename = std::string(kTestDirectory) + "test_eof_middle.crm";
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(&rssid_rowids, 0, 50, 11);
    ASSERT_OK(create_rows_mapper_file(filename, rssid_rowids));

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));

    // Try to read more rows than available
    std::vector<uint64_t> result;
    auto st = iterator.next_values(100, &result);
    ASSERT_TRUE(st.is_end_of_file()) << st;
}

TEST_F(RowsMapperTest, test_create_too_small_file) {
    // Create a file that's too small and verify it exists
    const std::string filename = std::string(kTestDirectory) + "too_small.crm";
    ASSERT_OK(create_too_small_file(filename));
    ASSERT_TRUE(fs::path_exist(filename));
}

TEST_F(RowsMapperTest, test_delete_files_on_close) {
    // Test file deletion behavior
    const std::string filename = std::string(kTestDirectory) + "delete_on_close.crm";
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(&rssid_rowids, 0, 100, 11);
    ASSERT_OK(create_rows_mapper_file(filename, rssid_rowids));

    ASSERT_TRUE(fs::path_exist(filename));

    // RowsMapperIterator deletes file in destructor
    {
        RowsMapperIterator iterator;
        ASSERT_OK(iterator.open(filename));
        std::vector<uint64_t> result;
        ASSERT_OK(iterator.next_values(100, &result));
        ASSERT_OK(iterator.status());
    }
    // File should be deleted after iterator is destroyed
    ASSERT_FALSE(fs::path_exist(filename));
}

TEST_F(RowsMapperTest, test_multiple_append_batches) {
    // Test multiple append calls with different batch sizes
    const std::string filename = std::string(kTestDirectory) + "multi_batch.crm";
    RowsMapperBuilder builder(filename);

    std::vector<uint64_t> batch1, batch2, batch3;
    generate_rssid_rowids(&batch1, 0, 10, 1);      // Small batch
    generate_rssid_rowids(&batch2, 10, 1010, 2);   // Large batch
    generate_rssid_rowids(&batch3, 1010, 1015, 3); // Small batch

    ASSERT_OK(builder.append(batch1));
    ASSERT_OK(builder.append(batch2));
    ASSERT_OK(builder.append(batch3));
    ASSERT_OK(builder.finalize());

    // Verify
    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));

    // Read batch1
    std::vector<uint64_t> result;
    ASSERT_OK(iterator.next_values(10, &result));
    ASSERT_EQ(10, result.size());
    for (size_t i = 0; i < 10; i++) {
        ASSERT_EQ(1, result[i] >> 32);
        ASSERT_EQ(i, result[i] & 0xFFFFFFFF);
    }

    // Read batch2
    result.clear();
    ASSERT_OK(iterator.next_values(1000, &result));
    ASSERT_EQ(1000, result.size());
    for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(2, result[i] >> 32);
        ASSERT_EQ(i + 10, result[i] & 0xFFFFFFFF);
    }

    // Read batch3
    result.clear();
    ASSERT_OK(iterator.next_values(5, &result));
    ASSERT_EQ(5, result.size());
    for (size_t i = 0; i < 5; i++) {
        ASSERT_EQ(3, result[i] >> 32);
        ASSERT_EQ(i + 1010, result[i] & 0xFFFFFFFF);
    }

    ASSERT_OK(iterator.status());
}

TEST_F(RowsMapperTest, test_large_rowid) {
    // Test with large rowids and rssids
    const std::string filename = std::string(kTestDirectory) + "large_rowid.crm";
    RowsMapperBuilder builder(filename);

    std::vector<uint64_t> rssid_rowids;
    uint64_t large_rssid = 0xFFFFFFFF; // Max 32-bit value
    uint32_t large_rowid = 0xFFFFFFF0; // Near max 32-bit value

    for (uint32_t i = 0; i < 10; i++) {
        rssid_rowids.push_back((large_rssid << 32) | (large_rowid + i));
    }

    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // Verify
    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));

    std::vector<uint64_t> result;
    ASSERT_OK(iterator.next_values(10, &result));
    ASSERT_EQ(10, result.size());

    for (size_t i = 0; i < 10; i++) {
        ASSERT_EQ(large_rssid, result[i] >> 32);
        ASSERT_EQ(large_rowid + i, result[i] & 0xFFFFFFFF);
    }

    ASSERT_OK(iterator.status());
}

TEST_F(RowsMapperTest, test_single_row) {
    // Test with single row
    const std::string filename = std::string(kTestDirectory) + "single_row.crm";
    RowsMapperBuilder builder(filename);

    std::vector<uint64_t> rssid_rowids;
    rssid_rowids.push_back((uint64_t(42) << 32) | 12345);

    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));

    std::vector<uint64_t> result;
    ASSERT_OK(iterator.next_values(1, &result));
    ASSERT_EQ(1, result.size());
    ASSERT_EQ(42, result[0] >> 32);
    ASSERT_EQ(12345, result[0] & 0xFFFFFFFF);

    ASSERT_OK(iterator.status());
}

TEST_F(RowsMapperTest, test_crm_file_gc) {
    DataDir* dir = get_stores();
    {
        // generate several crm files.
        ASSERT_OK(fs::new_writable_file(dir->get_tmp_path() + "/aaa.crm"));
        ASSERT_OK(fs::new_writable_file(dir->get_tmp_path() + "/bbb.crm"));
        ASSERT_OK(fs::new_writable_file(dir->get_tmp_path() + "/ccc.crm"));
        // collect files
        dir->perform_tmp_path_scan();
        dir->perform_tmp_path_scan();
        ASSERT_TRUE(dir->get_all_crm_files_cnt() == 3);
        // try to gc
        dir->perform_crm_gc(config::unused_crm_file_threshold_second);
        ASSERT_TRUE(dir->get_all_crm_files_cnt() == 0);
        // try to gc again
        dir->perform_tmp_path_scan();
        ASSERT_TRUE(dir->get_all_crm_files_cnt() == 3);
        dir->perform_crm_gc(0);
        ASSERT_TRUE(dir->get_all_crm_files_cnt() == 0);
        dir->perform_tmp_path_scan();
        // make sure file have been clean.
        ASSERT_TRUE(dir->get_all_crm_files_cnt() == 0);
    }
    {
        ASSERT_OK(fs::new_writable_file(dir->get_tmp_path() + "/aaa.crm"));
        // collect files
        dir->perform_tmp_path_scan();
        // delete this file
        ASSERT_OK(fs::remove(dir->get_tmp_path() + "/aaa.crm"));
        // try to gc
        dir->perform_crm_gc(config::unused_crm_file_threshold_second);
    }
    {
        ASSERT_OK(fs::remove(dir->get_tmp_path()));
        // collect files
        dir->perform_tmp_path_scan();
    }
}

TEST_F(RowsMapperTest, test_iterator_incremental_read) {
    // Test reading in small increments
    const std::string filename = std::string(kTestDirectory) + "incremental.crm";
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(&rssid_rowids, 0, 1000, 5);
    ASSERT_OK(create_rows_mapper_file(filename, rssid_rowids));

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));

    // Read 10 rows at a time
    size_t total_read = 0;
    for (int i = 0; i < 100; i++) {
        std::vector<uint64_t> result;
        ASSERT_OK(iterator.next_values(10, &result));
        ASSERT_EQ(10, result.size());
        for (size_t j = 0; j < result.size(); j++) {
            ASSERT_EQ(5, result[j] >> 32);
            ASSERT_EQ(total_read + j, result[j] & 0xFFFFFFFF);
        }
        total_read += 10;
    }

    ASSERT_EQ(1000, total_read);
    ASSERT_OK(iterator.status());
}

TEST_F(RowsMapperTest, test_interleaved_rssids) {
    // Test with interleaved rssids (simulating real compaction scenario)
    const std::string filename = std::string(kTestDirectory) + "interleaved.crm";
    RowsMapperBuilder builder(filename);

    std::vector<uint64_t> rssid_rowids;
    // Interleave rssids: 1, 2, 1, 2, 1, 2...
    for (uint32_t i = 0; i < 100; i++) {
        uint64_t rssid = (i % 2) + 1;
        rssid_rowids.push_back((rssid << 32) | i);
    }

    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));

    std::vector<uint64_t> result;
    ASSERT_OK(iterator.next_values(100, &result));
    ASSERT_EQ(100, result.size());

    for (size_t i = 0; i < result.size(); i++) {
        uint64_t expected_rssid = (i % 2) + 1;
        ASSERT_EQ(expected_rssid, result[i] >> 32);
        ASSERT_EQ(i, result[i] & 0xFFFFFFFF);
    }

    ASSERT_OK(iterator.status());
}

// ============================================================================
// lake_rows_mapper_filename with subtask_id Tests
// ============================================================================

TEST_F(RowsMapperTest, test_lake_rows_mapper_filename_with_subtask_id) {
    DataDir* dir = get_stores();
    ASSERT_NE(dir, nullptr);

    int64_t tablet_id = 12345;
    int64_t txn_id = 67890;
    int32_t subtask_id = 2;

    auto filename_or = lake_rows_mapper_filename(tablet_id, txn_id, subtask_id);
    ASSERT_OK(filename_or.status());
    std::string filename = filename_or.value();
    // Verify filename format: {tmp_path}/{tablet_id:016X}_{txn_id:016X}_{subtask_id}.crm
    ASSERT_TRUE(filename.find(".crm") != std::string::npos);
    ASSERT_TRUE(filename.find(dir->get_tmp_path()) != std::string::npos);
}

} // namespace starrocks