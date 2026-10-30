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

#include "compute_env/spill/dir_manager.h"

#include <gtest/gtest.h>

#include <string>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_storage_fwd.h"
#include "fs/fs.h"

namespace starrocks::spill {

// A spill dir that shares a disk with a storage path may only use spill_max_dir_bytes_ratio of that disk,
// which defaults to half so that spilling cannot crowd out tablet data.
TEST(DirManagerTest, spill_dir_on_storage_disk_is_capped_by_ratio) {
    ASSERT_DOUBLE_EQ(0.5, config::spill_max_dir_bytes_ratio);

    auto fs = FileSystem::Default();
    std::string dir_path = config::storage_root_path + "/spill_dir_manager_test/" + print_id(generate_uuid());
    ASSERT_OK(fs->create_dir_recursive(dir_path));
    DeferOp cleanup([&]() { (void)fs->delete_dir_recursive(dir_path); });

    DirManager dir_mgr;
    ASSERT_OK(dir_mgr.init(dir_path, {config::storage_root_path}));
    auto space_info = fs->space(dir_path);
    ASSERT_OK(space_info.status());

    auto dirs = dir_mgr.dirs();
    ASSERT_EQ(1, dirs.size());
    int64_t expected_max_size = space_info->capacity * 0.5;
    ASSERT_GT(expected_max_size, 0);
    EXPECT_EQ(expected_max_size, dirs[0]->get_max_size());

    EXPECT_TRUE(dirs[0]->inc_size(expected_max_size));
    EXPECT_FALSE(dirs[0]->inc_size(1));
}

} // namespace starrocks::spill
