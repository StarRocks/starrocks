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

#include "storage/load_spill_block_manager.h"

#include <gtest/gtest.h>

#include "fs/fs.h"
#include "testutil/assert.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"

namespace starrocks {

class LoadSpillBlockManagerTest : public ::testing::Test {
public:
    void SetUp() { (void)FileSystem::Default()->create_dir_recursive(kTestDir); }

    void TearDown() { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

protected:
    constexpr static const char* const kTestDir = "./lake_load_spill_block_manager_test";
};

// Test that destroying LoadSpillBlockManager without calling init() does not crash.
// This covers the case where init() fails or is never called, and the destructor
// should safely skip clear_parent_path() when _remote_dir_manager is null.
TEST_F(LoadSpillBlockManagerTest, test_destroy_without_init) {
    auto block_manager = std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, nullptr);
    // Destroy without calling init() — should not crash
    block_manager.reset();
}

TEST_F(LoadSpillBlockManagerTest, test_basic) {
    std::unique_ptr<LoadSpillBlockManager> block_manager =
            std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, nullptr);
    ASSERT_OK(block_manager->init());
    ASSIGN_OR_ABORT(auto block, block_manager->acquire_block(1024));
    ASSERT_OK(block_manager->release_block(block));
}

TEST_F(LoadSpillBlockManagerTest, test_write_read) {
    std::unique_ptr<LoadSpillBlockManager> block_manager =
            std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, nullptr);
    ASSERT_OK(block_manager->init());
    ASSIGN_OR_ABORT(auto block, block_manager->acquire_block(1024));
    ASSERT_OK(block->append({Slice("hello"), Slice("world")}));
    ASSERT_OK(block->flush());
    ASSIGN_OR_ABORT(auto input_stream, block->get_readable());
    ASSERT_TRUE(input_stream != nullptr);
    raw::RawString buffer;
    buffer.resize(10);
    ASSERT_OK(input_stream->read_fully(buffer.data(), 10));
    ASSERT_EQ(buffer, "helloworld");
    ASSERT_OK(block_manager->release_block(block));
}

// Test that clear_parent_path() cleans up the spill parent directory.
TEST_F(LoadSpillBlockManagerTest, test_clear_parent_path) {
    TUniqueId load_id;
    load_id.hi = 12345;
    load_id.lo = 67890;
    auto block_manager = std::make_unique<LoadSpillBlockManager>(load_id, TUniqueId(), kTestDir, nullptr);
    ASSERT_OK(block_manager->init());

    // Acquire a block with force_remote=true to create the remote spill directory
    ASSIGN_OR_ABORT(auto block, block_manager->acquire_block(1024, /*force_remote=*/true));
    ASSERT_OK(block->append({Slice("test_data")}));
    ASSERT_OK(block->flush());

    // Check that the parent path exists
    std::string parent_path = std::string(kTestDir) + "/load_spill/" + print_id(load_id);
    auto status = FileSystem::Default()->iterate_dir(parent_path, [](std::string_view) -> bool { return true; });
    ASSERT_OK(status);

    ASSERT_OK(block_manager->release_block(block));
    block.reset();

    // Explicitly call clear_parent_path() — should clean up the parent path
    ASSERT_OK(block_manager->clear_parent_path());

    // The parent directory should have been deleted
    status = FileSystem::Default()->iterate_dir(parent_path, [](std::string_view) -> bool { return true; });
    ASSERT_TRUE(status.is_not_found()) << "Expected parent path to be deleted, but got: " << status;
}

// Test that destroying LoadSpillBlockManager does NOT clean up the spill parent directory.
// The cleanup should be done explicitly via clear_parent_path() in DeltaWriter::close().
TEST_F(LoadSpillBlockManagerTest, test_destroy_does_not_clear_parent_path) {
    TUniqueId load_id;
    load_id.hi = 12345;
    load_id.lo = 67890;
    auto block_manager = std::make_unique<LoadSpillBlockManager>(load_id, TUniqueId(), kTestDir, nullptr);
    ASSERT_OK(block_manager->init());

    // Acquire a block with force_remote=true to create the remote spill directory
    ASSIGN_OR_ABORT(auto block, block_manager->acquire_block(1024, /*force_remote=*/true));
    ASSERT_OK(block->append({Slice("test_data")}));
    ASSERT_OK(block->flush());

    std::string parent_path = std::string(kTestDir) + "/load_spill/" + print_id(load_id);
    auto status = FileSystem::Default()->iterate_dir(parent_path, [](std::string_view) -> bool { return true; });
    ASSERT_OK(status);

    ASSERT_OK(block_manager->release_block(block));
    block.reset();

    // Destroy without calling clear_parent_path()
    block_manager.reset();

    // The parent directory should still exist — destructor no longer cleans it up
    status = FileSystem::Default()->iterate_dir(parent_path, [](std::string_view) -> bool { return true; });
    ASSERT_TRUE(status.ok()) << "Expected parent path to still exist after destroy, but got: " << status;
}

// Test that clear_parent_path() is safe to call when init() was not called.
TEST_F(LoadSpillBlockManagerTest, test_clear_parent_path_without_init) {
    auto block_manager = std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, nullptr);
    // Call clear_parent_path() without init() — should return OK without crashing
    ASSERT_OK(block_manager->clear_parent_path());
}

class LoadSpillBlockMergeExecutorTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(LoadSpillBlockMergeExecutorTest, test_init) {
    LoadSpillBlockMergeExecutor executor;
    ASSERT_OK(executor.init());
    ASSERT_NE(executor.get_thread_pool(), nullptr);
}

TEST_F(LoadSpillBlockMergeExecutorTest, test_create_token) {
    LoadSpillBlockMergeExecutor executor;
    ASSERT_OK(executor.init());

    // Test creating regular merge token
    auto token = executor.create_token();
    ASSERT_NE(token, nullptr);
}

TEST_F(LoadSpillBlockMergeExecutorTest, test_refresh_max_thread_num) {
    LoadSpillBlockMergeExecutor executor;
    ASSERT_OK(executor.init());

    ASSERT_OK(executor.refresh_max_thread_num());

    auto token = executor.create_token();
    ASSERT_NE(token, nullptr);
}

// ============================================================================
// flat-layout mode tests
// ----------------------------------------------------------------------------
// In flat-layout mode (Lake DeltaWriter path), spill files live under
// <root>/load_spill_txns/ with names "<txn_id_hex>_<load_id>_<frag_id>_<seq>".
// Per-file deletion at release time is suppressed; reclamation is delegated to
// the merge-task hot-delete path and vacuum_load_spill.
// ============================================================================

// flat-layout mode places spill files under <root>/load_spill_txns/
// (and NOT under <root>/load_spill/).
TEST_F(LoadSpillBlockManagerTest, flat_layout_mode_writes_to_load_spill_txns_dir) {
    constexpr int64_t kTxnId = 0x123456;
    auto block_manager = std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, /*fs=*/nullptr,
                                                                 /*enable_flat_layout=*/true, /*txn_id=*/kTxnId);
    ASSERT_OK(block_manager->init());

    ASSIGN_OR_ABORT(auto block, block_manager->acquire_block(1024, /*force_remote=*/true));
    ASSERT_OK(block->append({Slice("vacuum_mode_payload")}));
    ASSERT_OK(block->flush());

    // Flat-mode tree must exist; legacy tree must NOT have been created.
    std::string txns_dir = std::string(kTestDir) + "/load_spill_txns";
    std::string legacy_dir = std::string(kTestDir) + "/load_spill";
    auto txns_st = FileSystem::Default()->path_exists(txns_dir);
    ASSERT_TRUE(txns_st.ok()) << "expected " << txns_dir << " to exist in flat-layout mode, got: " << txns_st;
    auto legacy_st = FileSystem::Default()->path_exists(legacy_dir);
    ASSERT_TRUE(legacy_st.is_not_found())
            << "legacy " << legacy_dir << " should not be created in flat-layout mode, got: " << legacy_st;

    // Flat-layout invariant: <txns_dir>/<hex>/ must NOT exist. If it does, the writer
    // is producing a 2-level path that vacuum_load_spill cannot reclaim.
    std::string nested_hex_dir = txns_dir + "/" + fmt::format("{:016x}", kTxnId);
    auto nested_st = FileSystem::Default()->path_exists(nested_hex_dir);
    ASSERT_TRUE(nested_st.is_not_found())
            << "flat layout broken: nested " << nested_hex_dir << " must not exist, got: " << nested_st;

    ASSERT_OK(block_manager->release_block(block));
}

// In flat-layout mode, the on-disk file name must start with the
// 16-char hex-encoded txn_id followed by '_', placed directly under load_spill_txns/
// (no nested <hex>/ directory). This is what vacuum_load_spill's flat scan relies on.
TEST_F(LoadSpillBlockManagerTest, flat_layout_mode_filename_starts_with_hex_txn_id) {
    constexpr int64_t kTxnId = 0xABCD1234;
    auto block_manager = std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, /*fs=*/nullptr,
                                                                 /*enable_flat_layout=*/true, /*txn_id=*/kTxnId);
    ASSERT_OK(block_manager->init());

    ASSIGN_OR_ABORT(auto block, block_manager->acquire_block(1024, /*force_remote=*/true));
    ASSERT_OK(block->append({Slice("payload")}));
    ASSERT_OK(block->flush());

    std::string txns_dir = std::string(kTestDir) + "/load_spill_txns";
    bool found_hex_prefixed_file = false;
    auto expected_prefix = fmt::format("{:016x}_", kTxnId);
    auto st = FileSystem::Default()->iterate_dir(txns_dir, [&](std::string_view name) -> bool {
        if (name.find(expected_prefix) == 0) {
            found_hex_prefixed_file = true;
            return false; // stop early
        }
        return true;
    });
    ASSERT_OK(st);
    // ASSERT_TRUE (not EXPECT_TRUE): if the writer regresses to a 2-level layout, the
    // first-level entry will be a directory named just "<hex>" (no '_' suffix) and this
    // search will fail. We want that regression to abort the test so the next assertion
    // does not mask the real failure.
    ASSERT_TRUE(found_hex_prefixed_file) << "expected a file beginning with hex prefix '" << expected_prefix
                                         << "' directly under " << txns_dir
                                         << " (flat layout). Did the writer regress to a nested <hex>/ directory?";

    ASSERT_OK(block_manager->release_block(block));
}

// Legacy mode (enable_flat_layout=false) keeps the historical
// <root>/load_spill/ layout for non-Lake callers. Regression guard so a
// future refactor does not silently migrate them onto the flat layout.
TEST_F(LoadSpillBlockManagerTest, legacy_mode_writes_to_load_spill_dir) {
    auto block_manager = std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, /*fs=*/nullptr,
                                                                 /*enable_flat_layout=*/false);
    ASSERT_OK(block_manager->init());

    ASSIGN_OR_ABORT(auto block, block_manager->acquire_block(1024, /*force_remote=*/true));
    ASSERT_OK(block->append({Slice("legacy_payload")}));
    ASSERT_OK(block->flush());

    std::string txns_dir = std::string(kTestDir) + "/load_spill_txns";
    std::string legacy_dir = std::string(kTestDir) + "/load_spill";
    auto legacy_st = FileSystem::Default()->path_exists(legacy_dir);
    ASSERT_TRUE(legacy_st.ok()) << "expected legacy " << legacy_dir
                                << " to exist when enable_flat_layout=false, got: " << legacy_st;
    auto txns_st = FileSystem::Default()->path_exists(txns_dir);
    ASSERT_TRUE(txns_st.is_not_found()) << "flat " << txns_dir
                                        << " must not be created in legacy mode, got: " << txns_st;

    ASSERT_OK(block_manager->release_block(block));
}

// In flat-layout mode, release_block() must NOT physically delete the
// spill file (per-file deletion is delegated to merge-task hot-delete path and
// vacuum_load_spill). Contract guard for skip_file_deletion = true.
//
// Also asserts the snapshot enumerates *files* (not sub-directories): in flat
// layout the writer must produce regular files directly under load_spill_txns/.
TEST_F(LoadSpillBlockManagerTest, flat_layout_mode_release_block_does_not_delete_file) {
    constexpr int64_t kTxnId = 0xCAFE;
    auto block_manager = std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, /*fs=*/nullptr,
                                                                 /*enable_flat_layout=*/true, /*txn_id=*/kTxnId);
    ASSERT_OK(block_manager->init());

    ASSIGN_OR_ABORT(auto block, block_manager->acquire_block(1024, /*force_remote=*/true));
    ASSERT_OK(block->append({Slice("must_survive_release")}));
    ASSERT_OK(block->flush());

    // Snapshot file list before release. In flat layout every entry under txns_dir is
    // a regular file (no sub-directories); enforce that here so a nested-layout
    // regression is caught.
    std::string txns_dir = std::string(kTestDir) + "/load_spill_txns";
    std::vector<std::string> files_before;
    std::string nonfile_entry; // captured for diagnostics if a sub-directory leaks in
    ASSERT_OK(FileSystem::Default()->iterate_dir2(txns_dir, [&](DirEntry entry) -> bool {
        const bool is_regular_file = entry.is_dir.has_value() && !entry.is_dir.value();
        if (!is_regular_file) {
            // First non-file entry wins; keep iterating so files_before still reflects truth.
            if (nonfile_entry.empty()) {
                nonfile_entry.assign(entry.name);
            }
            return true;
        }
        files_before.emplace_back(entry.name);
        return true;
    }));
    ASSERT_TRUE(nonfile_entry.empty())
            << "flat layout broken: entry '" << nonfile_entry << "' under " << txns_dir
            << " is not a regular file. Did the writer regress to a nested <hex>/ directory?";
    ASSERT_FALSE(files_before.empty()) << "no spill file was produced under " << txns_dir;

    ASSERT_OK(block_manager->release_block(block));
    block.reset();

    // After release, the same files must still exist (not deleted on hot path).
    for (const auto& name : files_before) {
        auto st = FileSystem::Default()->path_exists(txns_dir + "/" + name);
        EXPECT_TRUE(st.ok()) << "spill file '" << name
                             << "' was unexpectedly deleted by release_block in flat-layout mode: " << st;
    }
}

} // namespace starrocks
