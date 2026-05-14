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

#include "exec/spill/query_spill_manager.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "common/config_storage_fwd.h"
#include "exec/spill/dir_manager.h"
#include "fmt/format.h"
#include "fs/fs_memory.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks::spill {

class QuerySpillManagerCoreTest : public ::testing::Test {
public:
    void SetUp() override {
        _query_id = generate_uuid();
        _local_path = fmt::format("{}/query_spill_manager", config::storage_root_path);
        _fs_handle = std::shared_ptr<FileSystem>(&_fs, [](FileSystem*) {});
        ASSERT_OK(_fs.create_dir_recursive(_local_path));
        _local_dir = std::make_shared<Dir>(_local_path, _fs_handle, 1024);
        _dir_mgr = std::make_unique<DirManager>(std::vector<DirPtr>{_local_dir});
    }

protected:
    MemoryFileSystem _fs;
    std::shared_ptr<FileSystem> _fs_handle;
    TUniqueId _query_id;
    std::string _local_path;
    DirPtr _local_dir;
    std::unique_ptr<DirManager> _dir_mgr;
    GlobalSpillManager _global_mgr;
};

TEST_F(QuerySpillManagerCoreTest, LocalSpillUsesInjectedDirManager) {
    QuerySpillManager query_spill_manager(_query_id, &_global_mgr, _dir_mgr.get());
    TQueryOptions query_options;
    ASSERT_OK(query_spill_manager.init_block_manager(query_options));
    ASSERT_NE(nullptr, query_spill_manager.block_manager());

    AcquireBlockOptions block_options;
    block_options.query_id = _query_id;
    block_options.fragment_instance_id = _query_id;
    block_options.plan_node_id = 1;
    block_options.name = "local";
    block_options.block_size = 16;
    auto block_or = query_spill_manager.block_manager()->acquire_block(block_options);
    ASSERT_TRUE(block_or.ok()) << block_or.status();
    auto block = block_or.value();
    EXPECT_EQ(fmt::format("LogBlock[container={}/{}/{}-{}]", _local_path, print_id(_query_id), print_id(_query_id),
                          "local-1-0"),
              block->debug_string());
}

TEST_F(QuerySpillManagerCoreTest, CountersForwardToGlobalSpillManager) {
    QuerySpillManager query_spill_manager(_query_id, &_global_mgr, _dir_mgr.get());

    query_spill_manager.increase_spillable_operators();
    query_spill_manager.inc_reserve_bytes(128);
    EXPECT_EQ(1, _global_mgr.spillable_operators());
    EXPECT_EQ(128, _global_mgr.spill_expected_reserved_bytes());

    query_spill_manager.dec_reserve_bytes(64);
    query_spill_manager.decrease_spillable_operators();
    EXPECT_EQ(0, _global_mgr.spillable_operators());
    EXPECT_EQ(64, _global_mgr.spill_expected_reserved_bytes());
}

} // namespace starrocks::spill
