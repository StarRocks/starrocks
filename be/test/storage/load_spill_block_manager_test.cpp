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

// Test that the spill parent directory is cleaned up after LoadSpillBlockManager is destroyed.
TEST_F(LoadSpillBlockManagerTest, test_clear_parent_path_on_destroy) {
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

    // Destroy the manager — should clean up the parent path
    block_manager.reset();

    // The parent directory should have been deleted
    status = FileSystem::Default()->iterate_dir(parent_path, [](std::string_view) -> bool { return true; });
    ASSERT_TRUE(status.is_not_found()) << "Expected parent path to be deleted, but got: " << status;
}
} // namespace starrocks
