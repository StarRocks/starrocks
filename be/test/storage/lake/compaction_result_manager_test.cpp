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

#include "storage/lake/compaction_result_manager.h"

#include <gtest/gtest.h>

#include <regex>
#include <set>

#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/lake/join_path.h"
#include "storage/lake/test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/raw_container.h"

namespace starrocks::lake {

class CompactionResultManagerTest : public TestBase {
public:
    CompactionResultManagerTest() : TestBase(kTestDir) { clear_and_init_test_dir(); }

    ~CompactionResultManagerTest() override = default;

    void SetUp() override {
        // Create compaction_results directory
        _results_dir = CompactionResultManager::get_results_dir(_test_dir);
        CHECK_OK(fs::create_directories(_results_dir));
    }

    void TearDown() override { (void)fs::remove_all(_test_dir); }

protected:
    constexpr static const char* kTestDir = "test_compaction_result_manager";
    std::string _results_dir;
    CompactionResultManager _manager;
};

// ============== Test get_results_dir ==============
TEST_F(CompactionResultManagerTest, test_get_results_dir) {
    std::string storage_root = "/path/to/storage";
    std::string expected = "/path/to/storage/lake/compaction_results";
    EXPECT_EQ(expected, CompactionResultManager::get_results_dir(storage_root));

    // Test with trailing slash
    std::string storage_root2 = "/path/to/storage/";
    std::string expected2 = "/path/to/storage//lake/compaction_results";
    EXPECT_EQ(expected2, CompactionResultManager::get_results_dir(storage_root2));

    // Test with empty string
    std::string expected_empty = "/lake/compaction_results";
    EXPECT_EQ(expected_empty, CompactionResultManager::get_results_dir(""));
}

// ============== Test extract_tablet_id_from_filename ==============
class ExtractTabletIdTest : public testing::Test {};

TEST_F(ExtractTabletIdTest, test_valid_filename) {
    // Valid filename format: tablet_{tablet_id}_result_{timestamp}_{random}.pb
    auto result = CompactionResultManager::extract_tablet_id_from_filename("tablet_12345_result_1234567890_123456.pb");
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(12345, result.value());
}

TEST_F(ExtractTabletIdTest, test_large_tablet_id) {
    auto result =
            CompactionResultManager::extract_tablet_id_from_filename("tablet_9223372036854775807_result_1_1.pb");
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(9223372036854775807LL, result.value());
}

TEST_F(ExtractTabletIdTest, test_invalid_filename_no_tablet_prefix) {
    auto result = CompactionResultManager::extract_tablet_id_from_filename("12345_result_1234567890_123456.pb");
    ASSERT_FALSE(result.ok());
}

TEST_F(ExtractTabletIdTest, test_invalid_filename_wrong_extension) {
    auto result = CompactionResultManager::extract_tablet_id_from_filename("tablet_12345_result_1234567890_123456.txt");
    ASSERT_FALSE(result.ok());
}

TEST_F(ExtractTabletIdTest, test_invalid_filename_missing_parts) {
    auto result = CompactionResultManager::extract_tablet_id_from_filename("tablet_12345.pb");
    ASSERT_FALSE(result.ok());

    auto result2 = CompactionResultManager::extract_tablet_id_from_filename("tablet_12345_result.pb");
    ASSERT_FALSE(result2.ok());
}

TEST_F(ExtractTabletIdTest, test_invalid_filename_non_numeric_tablet_id) {
    auto result = CompactionResultManager::extract_tablet_id_from_filename("tablet_abc_result_1234567890_123456.pb");
    ASSERT_FALSE(result.ok());
}

TEST_F(ExtractTabletIdTest, test_empty_filename) {
    auto result = CompactionResultManager::extract_tablet_id_from_filename("");
    ASSERT_FALSE(result.ok());
}

// ============== Test has_pending_results and get_tablets_with_pending_results ==============
TEST_F(CompactionResultManagerTest, test_pending_results_initially_empty) {
    // Initially, no pending results
    EXPECT_FALSE(_manager.has_pending_results(12345));
    EXPECT_TRUE(_manager.get_tablets_with_pending_results().empty());
}

// ============== Test validate_result ==============
TEST_F(CompactionResultManagerTest, test_validate_result_null_tablet_mgr) {
    CompactionResultPB result;
    result.set_tablet_id(12345);
    result.set_base_version(1);

    std::string reason;
    auto valid_or = _manager.validate_result(nullptr, result, &reason);
    ASSERT_FALSE(valid_or.ok());
}

TEST_F(CompactionResultManagerTest, test_validate_result_missing_tablet_id) {
    CompactionResultPB result;
    // Don't set tablet_id

    std::string reason;
    auto valid_or = _manager.validate_result(_tablet_mgr.get(), result, &reason);
    ASSERT_TRUE(valid_or.ok());
    EXPECT_FALSE(valid_or.value());
    EXPECT_EQ("missing tablet_id", reason);
}

TEST_F(CompactionResultManagerTest, test_validate_result_tablet_not_found) {
    CompactionResultPB result;
    result.set_tablet_id(99999); // Non-existent tablet

    std::string reason;
    auto valid_or = _manager.validate_result(_tablet_mgr.get(), result, &reason);
    ASSERT_TRUE(valid_or.ok());
    EXPECT_FALSE(valid_or.value());
    // The error message may contain "tablet not found" or "tablet metadata not found"
    EXPECT_TRUE(reason.find("not found") != std::string::npos || reason.find("metadata") != std::string::npos);
}

TEST_F(CompactionResultManagerTest, test_validate_result_with_valid_tablet) {
    // Create a tablet with metadata
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    int64_t tablet_id = metadata->id();
    metadata->set_version(10);

    // Add a rowset
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(100);
    rowset->set_data_size(1000);

    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    // Test with valid result
    CompactionResultPB result;
    result.set_tablet_id(tablet_id);
    result.set_base_version(5); // base_version < visible_version
    result.add_input_rowset_ids(1); // Existing rowset

    std::string reason;
    auto valid_or = _manager.validate_result(_tablet_mgr.get(), result, &reason);
    ASSERT_TRUE(valid_or.ok());
    EXPECT_TRUE(valid_or.value());
}

TEST_F(CompactionResultManagerTest, test_validate_result_base_version_too_high) {
    // Create a tablet with metadata
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    int64_t tablet_id = metadata->id();
    metadata->set_version(5);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    // Test with base_version > visible_version
    CompactionResultPB result;
    result.set_tablet_id(tablet_id);
    result.set_base_version(10); // base_version > visible_version (5)

    std::string reason;
    auto valid_or = _manager.validate_result(_tablet_mgr.get(), result, &reason);
    ASSERT_TRUE(valid_or.ok());
    EXPECT_FALSE(valid_or.value());
    EXPECT_TRUE(reason.find("base_version") != std::string::npos);
}

TEST_F(CompactionResultManagerTest, test_validate_result_missing_rowsets) {
    // Create a tablet with metadata
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    int64_t tablet_id = metadata->id();
    metadata->set_version(10);

    // Add a rowset with id 1
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(100);
    rowset->set_data_size(1000);

    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    // Test with non-existent input rowset
    CompactionResultPB result;
    result.set_tablet_id(tablet_id);
    result.set_base_version(5);
    result.add_input_rowset_ids(999); // Non-existent rowset

    std::string reason;
    auto valid_or = _manager.validate_result(_tablet_mgr.get(), result, &reason);
    ASSERT_TRUE(valid_or.ok());
    EXPECT_FALSE(valid_or.value());
    EXPECT_TRUE(reason.find("input rowsets no longer exist") != std::string::npos);
}

// Helper function to write string to file
static Status write_string_to_file(const std::string& path, const std::string& content) {
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(path));
    RETURN_IF_ERROR(wf->append(content));
    RETURN_IF_ERROR(wf->close());
    return Status::OK();
}

// Helper function to read string from file
static StatusOr<std::string> read_string_from_file(const std::string& path) {
    std::string content;
    ASSIGN_OR_RETURN(auto rf, fs::new_sequential_file(path));
    raw::stl_string_resize_uninitialized(&content, 1024 * 1024); // 1MB max
    ASSIGN_OR_RETURN(auto nread, rf->read(content.data(), content.size()));
    content.resize(nread);
    return content;
}

// ============== Test delete_result ==============
TEST_F(CompactionResultManagerTest, test_delete_result_file_exists) {
    // Create a test file
    std::string file_path = _results_dir + "/test_result.pb";
    ASSERT_OK(write_string_to_file(file_path, "test content"));

    // Verify file exists
    ASSERT_TRUE(fs::path_exist(file_path));

    // Delete the file
    ASSERT_OK(_manager.delete_result(file_path));

    // Verify file is deleted
    EXPECT_FALSE(fs::path_exist(file_path));
}

TEST_F(CompactionResultManagerTest, test_delete_result_file_not_exists) {
    std::string file_path = _results_dir + "/non_existent.pb";
    // Should not fail even if file doesn't exist (graceful handling)
    auto status = _manager.delete_result(file_path);
    // The actual behavior depends on fs::delete_file implementation
}

// ============== Test file operations with direct file system access ==============
TEST_F(CompactionResultManagerTest, test_result_file_read_write) {
    // Create a CompactionResultPB
    CompactionResultPB result;
    result.set_tablet_id(12345);
    result.set_base_version(10);
    result.add_input_rowset_ids(1);
    result.add_input_rowset_ids(2);
    result.add_input_rowset_ids(3);

    // Manually write to file (simulating save_result without StorageEngine)
    std::string filename = "tablet_12345_result_1234567890_123456.pb";
    std::string file_path = _results_dir + "/" + filename;

    std::string serialized;
    ASSERT_TRUE(result.SerializeToString(&serialized));
    ASSERT_OK(write_string_to_file(file_path, serialized));

    // Read and parse
    auto content_or = read_string_from_file(file_path);
    ASSERT_TRUE(content_or.ok());

    CompactionResultPB loaded_result;
    ASSERT_TRUE(loaded_result.ParseFromString(content_or.value()));

    // Verify
    EXPECT_EQ(12345, loaded_result.tablet_id());
    EXPECT_EQ(10, loaded_result.base_version());
    EXPECT_EQ(3, loaded_result.input_rowset_ids_size());
    EXPECT_EQ(1, loaded_result.input_rowset_ids(0));
    EXPECT_EQ(2, loaded_result.input_rowset_ids(1));
    EXPECT_EQ(3, loaded_result.input_rowset_ids(2));
}

TEST_F(CompactionResultManagerTest, test_result_file_parse_invalid_content) {
    // Write invalid content to file
    std::string filename = "tablet_12345_result_1234567890_123456.pb";
    std::string file_path = _results_dir + "/" + filename;
    ASSERT_OK(write_string_to_file(file_path, "invalid protobuf content"));

    // Read and try to parse
    auto content_or = read_string_from_file(file_path);
    ASSERT_TRUE(content_or.ok());

    CompactionResultPB result;
    EXPECT_FALSE(result.ParseFromString(content_or.value()));
}

// ============== Test filename pattern matching ==============
TEST_F(CompactionResultManagerTest, test_filename_pattern_matching) {
    // Test the pattern used in scan_all_result_files
    std::vector<std::string> test_files = {"tablet_100_result_1234_5678.pb",
                                           "tablet_200_result_9999_1111.pb",
                                           "other_file.pb",
                                           "tablet_invalid.pb",
                                           "tablet_300_result_abc_def.pb"};

    for (const auto& file : test_files) {
        std::string file_path = _results_dir + "/" + file;
        ASSERT_OK(write_string_to_file(file_path, "test"));
    }

    // List files
    std::set<std::string> dirs, files;
    ASSERT_OK(fs::list_dirs_files(_results_dir, &dirs, &files));

    // Filter matching files
    std::vector<std::string> matching_files;
    for (const auto& file : files) {
        if (file.ends_with(".pb") && file.find("tablet_") == 0) {
            auto tablet_id_or = CompactionResultManager::extract_tablet_id_from_filename(file);
            if (tablet_id_or.ok()) {
                matching_files.push_back(file);
            }
        }
    }

    // Should match tablet_100 and tablet_200 (tablet_300 has invalid format)
    EXPECT_EQ(2, matching_files.size());
}

// ============== Test load_and_validate_results ==============
TEST_F(CompactionResultManagerTest, test_load_and_validate_results_empty) {
    // Create a tablet
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    int64_t tablet_id = metadata->id();
    metadata->set_version(10);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    // No result files exist for this tablet
    auto results = _manager.load_and_validate_results(_tablet_mgr.get(), tablet_id);
    ASSERT_TRUE(results.ok());
    EXPECT_TRUE(results.value().empty());
}

TEST_F(CompactionResultManagerTest, test_load_and_validate_results_with_valid_file) {
    // Create a tablet with metadata
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    int64_t tablet_id = metadata->id();
    metadata->set_version(10);

    // Add a rowset
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(100);
    rowset->set_data_size(1000);

    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    // Create a valid result file
    CompactionResultPB result;
    result.set_tablet_id(tablet_id);
    result.set_base_version(5);
    result.add_input_rowset_ids(1);

    std::string filename = fmt::format("tablet_{}_result_1234567890_123456.pb", tablet_id);
    std::string file_path = _results_dir + "/" + filename;

    std::string serialized;
    ASSERT_TRUE(result.SerializeToString(&serialized));
    ASSERT_OK(write_string_to_file(file_path, serialized));

    // Load and validate - this requires the storage root to match
    // Since we can't easily mock get_storage_roots, we'll test the validation logic directly
    std::string invalid_reason;
    auto valid_or = _manager.validate_result(_tablet_mgr.get(), result, &invalid_reason);
    ASSERT_TRUE(valid_or.ok());
    EXPECT_TRUE(valid_or.value());
}

// ============== Test recover_on_startup with null tablet_mgr ==============
TEST_F(CompactionResultManagerTest, test_recover_on_startup_null_tablet_mgr) {
    auto result = _manager.recover_on_startup(nullptr);
    ASSERT_FALSE(result.ok());
}

// ============== Test RecoveryStats structure ==============
TEST_F(CompactionResultManagerTest, test_recovery_stats_default_values) {
    RecoveryStats stats;
    EXPECT_EQ(0, stats.total_files_scanned);
    EXPECT_EQ(0, stats.valid_results);
    EXPECT_EQ(0, stats.invalid_results_cleaned);
    EXPECT_EQ(0, stats.parse_errors);
    EXPECT_EQ(0, stats.validation_errors);
    EXPECT_TRUE(stats.recovered_tablet_ids.empty());
}

// ============== Test RecoveredResultInfo structure ==============
TEST_F(CompactionResultManagerTest, test_recovered_result_info_default_values) {
    RecoveredResultInfo info;
    EXPECT_TRUE(info.file_path.empty());
    EXPECT_FALSE(info.is_valid);
    EXPECT_TRUE(info.invalid_reason.empty());
}

// ============== Test multiple rowsets validation ==============
TEST_F(CompactionResultManagerTest, test_validate_result_partial_missing_rowsets) {
    // Create a tablet with metadata
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    int64_t tablet_id = metadata->id();
    metadata->set_version(10);

    // Add rowsets 1 and 2
    auto* rowset1 = metadata->add_rowsets();
    rowset1->set_id(1);
    rowset1->set_overlapped(false);
    rowset1->set_num_rows(100);
    rowset1->set_data_size(1000);

    auto* rowset2 = metadata->add_rowsets();
    rowset2->set_id(2);
    rowset2->set_overlapped(false);
    rowset2->set_num_rows(100);
    rowset2->set_data_size(1000);

    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    // Test with some existing and some non-existent rowsets
    CompactionResultPB result;
    result.set_tablet_id(tablet_id);
    result.set_base_version(5);
    result.add_input_rowset_ids(1); // Exists
    result.add_input_rowset_ids(2); // Exists
    result.add_input_rowset_ids(3); // Does not exist

    std::string reason;
    auto valid_or = _manager.validate_result(_tablet_mgr.get(), result, &reason);
    ASSERT_TRUE(valid_or.ok());
    EXPECT_FALSE(valid_or.value());
    EXPECT_TRUE(reason.find("3") != std::string::npos);
}

// ============== Test validate_result with all rowsets present ==============
TEST_F(CompactionResultManagerTest, test_validate_result_all_rowsets_present) {
    // Create a tablet with metadata
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    int64_t tablet_id = metadata->id();
    metadata->set_version(10);

    // Add multiple rowsets
    for (uint32_t i = 1; i <= 5; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(false);
        rowset->set_num_rows(100);
        rowset->set_data_size(1000);
    }

    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    // Test with all existing rowsets
    CompactionResultPB result;
    result.set_tablet_id(tablet_id);
    result.set_base_version(5);
    for (uint32_t i = 1; i <= 5; i++) {
        result.add_input_rowset_ids(i);
    }

    std::string reason;
    auto valid_or = _manager.validate_result(_tablet_mgr.get(), result, &reason);
    ASSERT_TRUE(valid_or.ok());
    EXPECT_TRUE(valid_or.value());
    EXPECT_TRUE(reason.empty());
}

// ============== Test validate_result without invalid_reason parameter ==============
TEST_F(CompactionResultManagerTest, test_validate_result_without_reason_output) {
    CompactionResultPB result;
    // Missing tablet_id

    // Should work without providing invalid_reason
    auto valid_or = _manager.validate_result(_tablet_mgr.get(), result, nullptr);
    ASSERT_TRUE(valid_or.ok());
    EXPECT_FALSE(valid_or.value());
}

// ============== Test edge case: base_version equals visible_version ==============
TEST_F(CompactionResultManagerTest, test_validate_result_base_version_equals_visible) {
    // Create a tablet with metadata
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    int64_t tablet_id = metadata->id();
    metadata->set_version(10);

    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(100);
    rowset->set_data_size(1000);

    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    // base_version == visible_version should be valid
    CompactionResultPB result;
    result.set_tablet_id(tablet_id);
    result.set_base_version(10); // Equal to visible_version
    result.add_input_rowset_ids(1);

    std::string reason;
    auto valid_or = _manager.validate_result(_tablet_mgr.get(), result, &reason);
    ASSERT_TRUE(valid_or.ok());
    EXPECT_TRUE(valid_or.value());
}

// ============== Test thread safety of pending results ==============
TEST_F(CompactionResultManagerTest, test_pending_results_thread_safety) {
    // This test verifies that the has_pending_results and get_tablets_with_pending_results
    // methods are thread-safe (they use mutex internally)

    // Since we can't easily populate _tablets_with_pending_results directly,
    // we just verify the methods don't crash when called concurrently
    std::vector<std::thread> threads;
    std::atomic<int> counter{0};

    for (int i = 0; i < 10; i++) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < 100; j++) {
                _manager.has_pending_results(i * 1000 + j);
                _manager.get_tablets_with_pending_results();
                counter++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(1000, counter.load());
}

} // namespace starrocks::lake

