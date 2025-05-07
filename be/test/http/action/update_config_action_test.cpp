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

#include "http/action/update_config_action.h"

#include <gtest/gtest.h>

#include "cache/block_cache/block_cache.h"
#include "cache/block_cache/test_cache_utils.h"
#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "storage/persistent_index_load_executor.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "testutil/scoped_updater.h"

namespace starrocks {

class UpdateConfigActionTest : public testing::Test {
public:
    UpdateConfigActionTest() = default;
    ~UpdateConfigActionTest() override = default;

    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(UpdateConfigActionTest, update_datacache_disk_size) {
    SCOPED_UPDATE(bool, config::datacache_auto_adjust_enable, false);
    const std::string cache_dir = "./block_cache_for_update_config";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    auto cache = std::make_shared<StarCacheWrapper>();
    CacheOptions options = TestCacheUtils::create_simple_options(256 * KB, 0);
    options.disk_spaces.push_back({.path = cache_dir, .size = 50 * MB});
    ASSERT_OK(cache->init(options));
    CacheEnv::GetInstance()->set_local_cache(cache);

    UpdateConfigAction action(ExecEnv::GetInstance());

    ASSERT_ERROR(action.update_config("datacache_disk_size", "-200"));

    ASSERT_OK(action.update_config("datacache_disk_size", "100000000"));

    std::vector<DirSpace> spaces;
    cache->disk_spaces(&spaces);
    ASSERT_EQ(spaces.size(), 1);
    ASSERT_EQ(spaces[0].size, 100000000);

    fs::remove_all(cache_dir).ok();
}

TEST_F(UpdateConfigActionTest, test_update_pindex_load_thread_pool_num_max) {
    UpdateConfigAction action(ExecEnv::GetInstance());

    ASSERT_OK(action.update_config("pindex_load_thread_pool_num_max", "16"));

    auto* load_pool = StorageEngine::instance()->update_manager()->get_pindex_load_executor()->TEST_get_load_pool();
    ASSERT_EQ(16, load_pool->max_threads());
}

} // namespace starrocks
