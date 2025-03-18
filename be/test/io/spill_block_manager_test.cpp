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

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iterator>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/dir_manager.h"
#include "exec/spill/executor.h"
#include "exec/spill/file_block_manager.h"
#include "exec/spill/hybird_block_manager.h"
#include "exec/spill/log_block_manager.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/spill_components.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "exec/spill/spiller_factory.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"

namespace starrocks::vectorized {

std::string generate_spill_path(const TUniqueId& query_id, const std::string& path) {
    return fmt::format("{}/{}/{}", config::storage_root_path, path, print_id(query_id));
}

spill::DirPtr create_spill_dir(const std::string& path, int64_t capacity_limit) {
    auto fs = FileSystem::CreateSharedFromString(path);
    return std::make_shared<spill::Dir>(path, fs.value(), capacity_limit);
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
        auto fs = FileSystem::Default();
        ASSERT_OK(fs->create_dir_recursive(local_path));
        ASSERT_OK(fs->create_dir_recursive(remote_path));
        local_dir = create_spill_dir(local_path, 100);
        remote_dir = create_spill_dir(remote_path, INT64_MAX);
        local_dir_mgr = create_spill_dir_manager({local_dir});
        remote_dir_mgr = create_spill_dir_manager({remote_dir});
    }

    void TearDown() override {}

protected:
    TUniqueId dummy_query_id;
    std::string local_path;
    std::string remote_path;
    spill::DirPtr local_dir;
    spill::DirPtr remote_dir;
    std::unique_ptr<spill::DirManager> local_dir_mgr;
    std::unique_ptr<spill::DirManager> remote_dir_mgr;
    RuntimeState dummy_state;
    RuntimeProfile dummy_profile{"dummy"};
};

TEST_F(SpillBlockManagerTest, dir_choose_strategy) {
    using DirInfo = std::pair<std::string, size_t>;
    std::vector<DirInfo> dir_info_list = {{"dir1", 100}, {"dir2", 120}};
    TUniqueId dummy_query_id = generate_uuid();
    std::vector<spill::DirPtr> dir_list;
    for (auto& dir_info : dir_info_list) {
        auto path = generate_spill_path(dummy_query_id, dir_info.first);
        auto dir = create_spill_dir(path, dir_info.second);
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
    auto test_func = [&](std::shared_ptr<spill::BlockManager>& block_mgr, spill::DirPtr dir) {
        ASSERT_OK(block_mgr->open());
        {
            spill::AcquireBlockOptions opts{.query_id = dummy_query_id,
                                            .fragment_instance_id = dummy_query_id,
                                            .plan_node_id = 1,
                                            .name = "node1",
                                            .block_size = 10};
            auto res = block_mgr->acquire_block(opts);
            ASSERT_TRUE(res.ok());
            ASSERT_EQ(dir->get_current_size(), 10);
            auto block = res.value();

            ASSERT_TRUE(block->preallocate(5));
            // there are 10 bytes left unused, preallocate will not actually apply for space at this time
            ASSERT_EQ(dir->get_current_size(), 10);
            ASSERT_EQ(block->size(), 0);
            char tmp[5];
            ASSERT_OK(block->append({Slice(tmp, 5)}));
            ASSERT_EQ(block->size(), 5);

            // there are 5 bytes left unused, preallocate will not actually apply for space at this time
            ASSERT_TRUE(block->preallocate(5));
            ASSERT_EQ(dir->get_current_size(), 10);
            ASSERT_OK(block->append({Slice(tmp, 5)}));
            ASSERT_EQ(block->size(), 10);
            ASSERT_EQ(dir->get_current_size(), 10);

            // there is no remaining space, preallocate needs to actually apply for space
            ASSERT_TRUE(block->preallocate(20));
            ASSERT_EQ(block->size(), 10);
            ASSERT_EQ(dir->get_current_size(), 30);
        }

        block_mgr.reset();
        // after block_mgr is destroyed, all space should be released
        ASSERT_EQ(dir->get_current_size(), 0);
    };
    std::shared_ptr<spill::BlockManager> log_block_mgr =
            std::make_shared<spill::LogBlockManager>(dummy_query_id, local_dir_mgr.get());
    test_func(log_block_mgr, local_dir);

    std::shared_ptr<spill::BlockManager> file_block_mgr =
            std::make_shared<spill::FileBlockManager>(dummy_query_id, remote_dir_mgr.get());
    test_func(file_block_mgr, remote_dir);
}

} // namespace starrocks::vectorized
