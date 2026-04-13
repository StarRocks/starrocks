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

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "common/config_storage_fwd.h"
#include "exec/spill/dir_manager.h"
#include "exec/spill/file_block_manager.h"
#include "exec/spill/hybird_block_manager.h"
#include "exec/spill/log_block_manager.h"
#include "fmt/format.h"
#include "fs/fs_memory.h"
#include "gen_cpp/Types_types.h"

namespace starrocks::vectorized {

std::string generate_spill_path(const TUniqueId& query_id, const std::string& path) {
    return fmt::format("{}/{}/{}", config::storage_root_path, path, print_id(query_id));
}

spill::DirPtr create_spill_dir(const std::string& path, int64_t capacity_limit, const std::shared_ptr<FileSystem>& fs) {
    return std::make_shared<spill::Dir>(path, fs, capacity_limit);
}

std::unique_ptr<spill::DirManager> create_spill_dir_manager(const std::vector<spill::DirPtr>& dirs) {
    auto dir_mgr = std::make_unique<spill::DirManager>(dirs);
    return dir_mgr;
}

class SpillBlockManagerTest : public ::testing::Test {
public:
    void SetUp() override {
        dummy_query_id = generate_uuid();
        local_path = fmt::format("{}/{}", config::storage_root_path, "local_data");
        remote_path = fmt::format("{}/{}", config::storage_root_path, "remote_data");
        fs_handle = std::shared_ptr<FileSystem>(&fs, [](FileSystem*) {});
        ASSERT_OK(fs.create_dir_recursive(local_path));
        ASSERT_OK(fs.create_dir_recursive(remote_path));
        local_dir = create_spill_dir(local_path, 100, fs_handle);
        remote_dir = create_spill_dir(remote_path, INT64_MAX, fs_handle);
        local_dir_mgr = create_spill_dir_manager({local_dir});
        remote_dir_mgr = create_spill_dir_manager({remote_dir});
    }

    void TearDown() override {}

protected:
    MemoryFileSystem fs;
    std::shared_ptr<FileSystem> fs_handle;
    TUniqueId dummy_query_id;
    std::string local_path;
    std::string remote_path;
    spill::DirPtr local_dir;
    spill::DirPtr remote_dir;
    std::unique_ptr<spill::DirManager> local_dir_mgr;
    std::unique_ptr<spill::DirManager> remote_dir_mgr;
};

TEST_F(SpillBlockManagerTest, dir_choose_strategy) {
    using DirInfo = std::pair<std::string, size_t>;
    std::vector<DirInfo> dir_info_list = {{"dir1", 100}, {"dir2", 120}};
    TUniqueId dummy_query_id = generate_uuid();
    std::vector<spill::DirPtr> dir_list;
    for (auto& dir_info : dir_info_list) {
        auto path = generate_spill_path(dummy_query_id, dir_info.first);
        auto dir = create_spill_dir(path, dir_info.second, fs_handle);
        dir_list.emplace_back(std::move(dir));
    }
    auto dir_mgr = create_spill_dir_manager(dir_list);
    {
        spill::AcquireDirOptions opts{.data_size = 10};
        auto res = dir_mgr->acquire_writable_dir(opts);
        ASSERT_TRUE(res.ok());
        std::string expected_dir = generate_spill_path(dummy_query_id, "dir2");
        ASSERT_EQ(expected_dir, res.value()->dir());
        // after allocation
        // dir1 used 0, dir2 used 10
    }
    {
        spill::AcquireDirOptions opts{.data_size = 20};
        auto res = dir_mgr->acquire_writable_dir(opts);
        ASSERT_TRUE(res.ok());
        std::string expected_dir = generate_spill_path(dummy_query_id, "dir2");
        ASSERT_EQ(expected_dir, res.value()->dir());
        // after allocation
        // dir1 used 0, dir2 used 30
    }
    {
        // only dir1 can meet the capacity requirements
        spill::AcquireDirOptions opts{.data_size = 100};
        auto res = dir_mgr->acquire_writable_dir(opts);
        ASSERT_TRUE(res.ok());
        std::string expected_dir = generate_spill_path(dummy_query_id, "dir1");
        ASSERT_EQ(expected_dir, res.value()->dir());
        // after allocation
        // dir1 used 100, dir2 used 30
    }
    {
        // no dirs can meet the capacity requirements
        spill::AcquireDirOptions opts{.data_size = 100};
        auto res = dir_mgr->acquire_writable_dir(opts);
        ASSERT_FALSE(res.ok());
    }
    {
        // only dir2 can meet the capacity requirements
        spill::AcquireDirOptions opts{.data_size = 90};
        auto res = dir_mgr->acquire_writable_dir(opts);
        ASSERT_TRUE(res.ok());
        std::string expected_dir = generate_spill_path(dummy_query_id, "dir2");
        ASSERT_EQ(expected_dir, res.value()->dir());
    }
}

TEST_F(SpillBlockManagerTest, log_block_allocation_test) {
    auto log_block_mgr = std::make_shared<spill::LogBlockManager>(dummy_query_id, local_dir_mgr.get());
    ASSERT_OK(log_block_mgr->open());

    std::vector<spill::BlockPtr> blocks;
    {
        // 1. allocate the first block but not release it
        spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                        .fragment_instance_id = dummy_query_id,
                                        .plan_node_id = 1,
                                        .name = "node1",
                                        .block_size = 10};
        auto res = log_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        auto block = res.value();
        ASSERT_TRUE(block->try_acquire_sizes(10));
        std::string expected = fmt::format("LogBlock[container={}/{}/{}-{}]", local_path, print_id(dummy_query_id),
                                           print_id(dummy_query_id), "node1-1-0");
        ASSERT_EQ(block->debug_string(), expected);
        blocks.emplace_back(std::move(block));
    }
    {
        // 2. allocate the second block, since the first block didn't release, a new container should be created.
        spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                        .fragment_instance_id = dummy_query_id,
                                        .plan_node_id = 1,
                                        .name = "node1",
                                        .block_size = 10};
        auto res = log_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        auto block = res.value();
        std::string expected = fmt::format("LogBlock[container={}/{}/{}-{}]", local_path, print_id(dummy_query_id),
                                           print_id(dummy_query_id), "node1-1-1");
        ASSERT_EQ(block->debug_string(), expected);
        blocks.emplace_back(std::move(block));
    }
    // 3. release the first block
    ASSERT_OK(log_block_mgr->release_block(blocks.at(0)));
    {
        // 4. allocate the third block, it will use the first container
        spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                        .fragment_instance_id = dummy_query_id,
                                        .plan_node_id = 1,
                                        .name = "node1",
                                        .block_size = 10};
        auto res = log_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        auto block = res.value();
        std::string expected = fmt::format("LogBlock[container={}/{}/{}-{}]", local_path, print_id(dummy_query_id),
                                           print_id(dummy_query_id), "node1-1-0");
        ASSERT_EQ(block->debug_string(), expected);
    }
}

TEST_F(SpillBlockManagerTest, file_block_allocation_test) {
    auto file_block_mgr = std::make_shared<spill::FileBlockManager>(dummy_query_id, local_dir_mgr.get());
    ASSERT_OK(file_block_mgr->open());

    std::vector<spill::BlockPtr> blocks;
    {
        // 1. allocate the first block but not release it
        spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                        .fragment_instance_id = dummy_query_id,
                                        .plan_node_id = 1,
                                        .name = "node1",
                                        .block_size = 10};
        auto res = file_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        auto block = res.value();
        ASSERT_TRUE(block->try_acquire_sizes(10));
        std::string expected = fmt::format("FileBlock[container={}/{}/{}-{}]", local_path, print_id(dummy_query_id),
                                           print_id(dummy_query_id), "node1-1-0");
        ASSERT_EQ(block->debug_string(), expected);
        blocks.emplace_back(std::move(block));
    }
    {
        // 2. allocate the second block, since the first block didn't release, a new container should be created.
        spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                        .fragment_instance_id = dummy_query_id,
                                        .plan_node_id = 1,
                                        .name = "node1",
                                        .block_size = 10};
        auto res = file_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        auto block = res.value();
        std::string expected = fmt::format("FileBlock[container={}/{}/{}-{}]", local_path, print_id(dummy_query_id),
                                           print_id(dummy_query_id), "node1-1-1");
        ASSERT_EQ(block->debug_string(), expected);
        blocks.emplace_back(std::move(block));
    }
    // 3. release the first block
    ASSERT_OK(file_block_mgr->release_block(blocks.at(0)));
    {
        // 4. allocate the third block, it will use a new container, this is differ from LogBlockManager
        spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                        .fragment_instance_id = dummy_query_id,
                                        .plan_node_id = 1,
                                        .name = "node1",
                                        .block_size = 10};
        auto res = file_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        auto block = res.value();
        std::string expected = fmt::format("FileBlock[container={}/{}/{}-{}]", local_path, print_id(dummy_query_id),
                                           print_id(dummy_query_id), "node1-1-2");
        ASSERT_EQ(block->debug_string(), expected);
    }
}

TEST_F(SpillBlockManagerTest, hybird_block_allocation_test) {
    std::shared_ptr<spill::HyBirdBlockManager> hybird_block_mgr;
    {
        auto local_block_mgr = std::make_unique<spill::LogBlockManager>(dummy_query_id, local_dir_mgr.get());
        ASSERT_OK(local_block_mgr->open());

        auto remote_block_mgr = std::make_unique<spill::FileBlockManager>(dummy_query_id, remote_dir_mgr.get());
        ASSERT_OK(remote_block_mgr->open());

        hybird_block_mgr = std::make_shared<spill::HyBirdBlockManager>(dummy_query_id, std::move(local_block_mgr),
                                                                       std::move(remote_block_mgr));
        ASSERT_OK(hybird_block_mgr->open());
    }
    {
        // 1. allocate the first block, local block manager can hold it
        spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                        .fragment_instance_id = dummy_query_id,
                                        .plan_node_id = 1,
                                        .name = "node1",
                                        .block_size = 10};
        auto res = hybird_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        auto block = res.value();
        std::string expected = fmt::format("LogBlock[container={}/{}/{}-{}]", local_path, print_id(dummy_query_id),
                                           print_id(dummy_query_id), "node1-1-0");
        ASSERT_EQ(block->debug_string(), expected);
        ASSERT_OK(hybird_block_mgr->release_block(block));
    }
    {
        // 2. allocate the second block, local block manager's capacity exceeds limit, and it will be put in remote block manger
        spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                        .fragment_instance_id = dummy_query_id,
                                        .plan_node_id = 1,
                                        .name = "node1",
                                        .block_size = 100};
        auto res = hybird_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        auto block = res.value();
        std::string expected = fmt::format("FileBlock[container={}/{}/{}-{}]", remote_path, print_id(dummy_query_id),
                                           print_id(dummy_query_id), "node1-1-0");
        ASSERT_EQ(block->debug_string(), expected);
    }
    {
        // 3. allocate the third block, it's small enough that local block manger can hold it
        spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                        .fragment_instance_id = dummy_query_id,
                                        .plan_node_id = 1,
                                        .name = "node1",
                                        .block_size = 90};
        auto res = hybird_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        auto block = res.value();
        std::string expected = fmt::format("LogBlock[container={}/{}/{}-{}]", local_path, print_id(dummy_query_id),
                                           print_id(dummy_query_id), "node1-1-0");
        ASSERT_EQ(block->debug_string(), expected);
    }
}

TEST_F(SpillBlockManagerTest, dir_allocate_test) {
    auto log_block_mgr = std::make_shared<spill::LogBlockManager>(dummy_query_id, local_dir_mgr.get());
    ASSERT_OK(log_block_mgr->open());
    {
        // 1. allocate the first block but not release it
        spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                        .fragment_instance_id = dummy_query_id,
                                        .plan_node_id = 1,
                                        .name = "node1",
                                        .block_size = 10};
        auto res = log_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
    }
    ASSERT_EQ(local_dir_mgr->_dirs[0]->get_current_size(), 0);
}

TEST_F(SpillBlockManagerTest, block_capacity_test) {
    {
        auto log_block_mgr = std::make_shared<spill::LogBlockManager>(dummy_query_id, local_dir_mgr.get());
        ASSERT_OK(log_block_mgr->open());
        char vals[4096];
        // 1. allocate the first block but not release it
        spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                        .fragment_instance_id = dummy_query_id,
                                        .plan_node_id = 1,
                                        .name = "node1",
                                        .block_size = 10};
        opts.affinity_group = log_block_mgr->acquire_affinity_group();
        auto res = log_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(local_dir->get_current_size(), 10);
        ASSERT_OK(res.value()->append(std::vector<Slice>{Slice(vals, 5)}));
        ASSERT_EQ(local_dir->get_current_size(), 10);
        ASSERT_OK(res.value()->append(std::vector<Slice>{Slice(vals, 5)}));
        ASSERT_EQ(local_dir->get_current_size(), 10);
        ASSERT_OK(res.value()->append(std::vector<Slice>{Slice(vals, 5)}));
        ASSERT_EQ(local_dir->get_current_size(), 15);
        ASSERT_OK(log_block_mgr->release_block(res.value()));

        res = log_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(local_dir->get_current_size(), 25);
        ASSERT_OK(log_block_mgr->release_block(res.value()));

        res = log_block_mgr->acquire_block(opts);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(local_dir->get_current_size(), 25);
    }
    ASSERT_EQ(local_dir->get_current_size(), 0);
}

} // namespace starrocks::vectorized
