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

#include "base/testutil/assert.h"
#include "fs/fs.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"

namespace starrocks {

class LoadSpillBlockManagerTest : public ::testing::Test {
public:
    void SetUp() { (void)FileSystem::Default()->create_dir_recursive(kTestDir); }

    void TearDown() { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

protected:
    constexpr static const char* const kTestDir = "./lake_load_spill_block_manager_test";
};

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

TEST_F(LoadSpillBlockMergeExecutorTest, test_create_tablet_internal_parallel_merge_token) {
    LoadSpillBlockMergeExecutor executor;
    ASSERT_OK(executor.init());

    // Test creating tablet internal parallel merge token
    auto token = executor.create_tablet_internal_parallel_merge_token();
    ASSERT_NE(token, nullptr);
}

TEST_F(LoadSpillBlockMergeExecutorTest, test_create_multiple_tokens) {
    LoadSpillBlockMergeExecutor executor;
    ASSERT_OK(executor.init());

    // Test creating multiple tokens of different types
    std::vector<std::unique_ptr<ThreadPoolToken>> tokens;
    for (int i = 0; i < 5; i++) {
        tokens.push_back(executor.create_token());
        ASSERT_NE(tokens.back(), nullptr);
    }

    std::vector<std::unique_ptr<ThreadPoolToken>> parallel_tokens;
    for (int i = 0; i < 5; i++) {
        parallel_tokens.push_back(executor.create_tablet_internal_parallel_merge_token());
        ASSERT_NE(parallel_tokens.back(), nullptr);
    }
}

TEST_F(LoadSpillBlockMergeExecutorTest, test_refresh_max_thread_num) {
    LoadSpillBlockMergeExecutor executor;
    ASSERT_OK(executor.init());

    // Test refreshing max thread number
    ASSERT_OK(executor.refresh_max_thread_num());

    // Verify the thread pool is still functional after refresh
    auto token = executor.create_token();
    ASSERT_NE(token, nullptr);

    auto parallel_token = executor.create_tablet_internal_parallel_merge_token();
    ASSERT_NE(parallel_token, nullptr);
}

TEST_F(LoadSpillBlockMergeExecutorTest, test_token_execution_mode) {
    LoadSpillBlockMergeExecutor executor;
    ASSERT_OK(executor.init());

    // Create tokens and verify they can submit tasks
    auto serial_token = executor.create_token();
    auto parallel_token = executor.create_tablet_internal_parallel_merge_token();

    std::atomic<int> serial_counter{0};
    std::atomic<int> parallel_counter{0};

    // Submit tasks to serial token
    for (int i = 0; i < 10; i++) {
        ASSERT_OK(serial_token->submit_func([&serial_counter]() { serial_counter++; }));
    }

    // Submit tasks to parallel token
    for (int i = 0; i < 10; i++) {
        ASSERT_OK(parallel_token->submit_func([&parallel_counter]() { parallel_counter++; }));
    }

    // Wait for completion
    serial_token->wait();
    parallel_token->wait();

    // Verify all tasks executed
    ASSERT_EQ(serial_counter, 10);
    ASSERT_EQ(parallel_counter, 10);
}

class LoadSpillBlockContainerTest : public ::testing::Test {
public:
    void SetUp() override { (void)FileSystem::Default()->create_dir_recursive(kTestDir); }
    void TearDown() override { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

protected:
    constexpr static const char* const kTestDir = "./lake_load_spill_block_container_test";
};

TEST_F(LoadSpillBlockContainerTest, test_block_group_with_slot_idx) {
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), TUniqueId(), "./lake_load_spill_block_container_test", nullptr);
    ASSERT_OK(block_manager->init());

    auto container = block_manager->block_container();
    ASSERT_TRUE(container->empty());

    auto* group0 = container->create_block_group(0);
    auto* group1 = container->create_block_group(1);
    auto* group2 = container->create_block_group(2);

    ASSERT_NE(group0, nullptr);
    ASSERT_NE(group1, nullptr);
    ASSERT_NE(group2, nullptr);

    ASSIGN_OR_ABORT(auto block0, block_manager->acquire_block(1024));
    ASSERT_OK(block0->append({Slice("data0")}));
    ASSERT_OK(block0->flush());
    container->append_block(group0, block0);

    ASSIGN_OR_ABORT(auto block1, block_manager->acquire_block(1024));
    ASSERT_OK(block1->append({Slice("data1")}));
    ASSERT_OK(block1->flush());
    container->append_block(group1, block1);

    ASSIGN_OR_ABORT(auto block2, block_manager->acquire_block(1024));
    ASSERT_OK(block2->append({Slice("data2")}));
    ASSERT_OK(block2->flush());
    container->append_block(group2, block2);

    ASSERT_FALSE(container->empty());

    auto& groups = container->block_groups();
    ASSERT_EQ(3, groups.size());
    ASSERT_EQ(0, groups[0].slot_idx);
    ASSERT_EQ(1, groups[1].slot_idx);
    ASSERT_EQ(2, groups[2].slot_idx);

    ASSERT_EQ(1, groups[0].block_group->blocks().size());
    ASSERT_EQ(1, groups[1].block_group->blocks().size());
    ASSERT_EQ(1, groups[2].block_group->blocks().size());

    ASSERT_OK(block_manager->release_block(block0));
    ASSERT_OK(block_manager->release_block(block1));
    ASSERT_OK(block_manager->release_block(block2));
}

TEST_F(LoadSpillBlockContainerTest, test_block_group_ordering) {
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), TUniqueId(), "./lake_load_spill_block_container_test", nullptr);
    ASSERT_OK(block_manager->init());

    auto container = block_manager->block_container();

    auto* group5 = container->create_block_group(5);
    auto* group2 = container->create_block_group(2);
    auto* group8 = container->create_block_group(8);
    auto* group1 = container->create_block_group(1);

    ASSERT_NE(group5, nullptr);
    ASSERT_NE(group2, nullptr);
    ASSERT_NE(group8, nullptr);
    ASSERT_NE(group1, nullptr);

    ASSIGN_OR_ABORT(auto block0, block_manager->acquire_block(1024));
    ASSERT_OK(block0->flush());
    container->append_block(group5, block0);

    ASSIGN_OR_ABORT(auto block1, block_manager->acquire_block(1024));
    ASSERT_OK(block1->flush());
    container->append_block(group2, block1);

    ASSIGN_OR_ABORT(auto block2, block_manager->acquire_block(1024));
    ASSERT_OK(block2->flush());
    container->append_block(group8, block2);

    ASSIGN_OR_ABORT(auto block3, block_manager->acquire_block(1024));
    ASSERT_OK(block3->flush());
    container->append_block(group1, block3);

    auto& groups = container->block_groups();
    ASSERT_EQ(4, groups.size());
    ASSERT_EQ(5, groups[0].slot_idx);
    ASSERT_EQ(2, groups[1].slot_idx);
    ASSERT_EQ(8, groups[2].slot_idx);
    ASSERT_EQ(1, groups[3].slot_idx);

    std::sort(groups.begin(), groups.end(),
              [](const BlockGroupPtrWithSlot& a, const BlockGroupPtrWithSlot& b) { return a.slot_idx < b.slot_idx; });

    ASSERT_EQ(1, groups[0].slot_idx);
    ASSERT_EQ(2, groups[1].slot_idx);
    ASSERT_EQ(5, groups[2].slot_idx);
    ASSERT_EQ(8, groups[3].slot_idx);

    ASSERT_OK(block_manager->release_block(block0));
    ASSERT_OK(block_manager->release_block(block1));
    ASSERT_OK(block_manager->release_block(block2));
    ASSERT_OK(block_manager->release_block(block3));
}

TEST_F(LoadSpillBlockContainerTest, test_multiple_blocks_per_group) {
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), TUniqueId(), "./lake_load_spill_block_container_test", nullptr);
    ASSERT_OK(block_manager->init());

    auto container = block_manager->block_container();

    auto* group = container->create_block_group(10);
    ASSERT_NE(group, nullptr);

    std::vector<spill::BlockPtr> blocks;
    for (int i = 0; i < 5; i++) {
        ASSIGN_OR_ABORT(auto block, block_manager->acquire_block(1024));
        std::string data = "data" + std::to_string(i);
        ASSERT_OK(block->append({Slice(data)}));
        ASSERT_OK(block->flush());
        container->append_block(group, block);
        blocks.push_back(block);
    }

    auto& groups = container->block_groups();
    ASSERT_EQ(1, groups.size());
    ASSERT_EQ(10, groups[0].slot_idx);
    ASSERT_EQ(5, groups[0].block_group->blocks().size());

    for (auto& block : blocks) {
        ASSERT_OK(block_manager->release_block(block));
    }
}

} // namespace starrocks
