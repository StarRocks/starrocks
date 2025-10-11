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

#include "agent/agent_common.h"
#include "agent/agent_server.h"
#include "cache/datacache.h"
#include "cache/disk_cache/starcache_engine.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "common/config.h"
#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "storage/persistent_index_load_executor.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "testutil/scoped_updater.h"
#include "util/bthreads/executor.h"

namespace starrocks {

class UpdateConfigActionTest : public testing::Test {
public:
    UpdateConfigActionTest() = default;
    ~UpdateConfigActionTest() override = default;

    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(UpdateConfigActionTest, update_datacache_config) {
    SCOPED_UPDATE(bool, config::enable_datacache_disk_auto_adjust, false);
    const std::string cache_dir = "./block_cache_for_update_config";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    auto cache = std::make_shared<StarCacheEngine>();
    DiskCacheOptions options = TestCacheUtils::create_simple_options(256 * KB, 0);
    options.dir_spaces.push_back({.path = cache_dir, .size = 50 * MB});
    ASSERT_OK(cache->init(options));
    DataCache::GetInstance()->set_local_disk_cache(cache);

    UpdateConfigAction action(ExecEnv::GetInstance());

    // update disk size
    ASSERT_ERROR(action.update_config("datacache_disk_size", "-200"));
    ASSERT_OK(action.update_config("datacache_disk_size", "100000000"));
    // update inline cache limit
    ASSERT_OK(action.update_config("datacache_inline_item_count_limit", "260344"));

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

TEST_F(UpdateConfigActionTest, test_update_number_tablet_writer_threads) {
    UpdateConfigAction action(ExecEnv::GetInstance());
    auto* executor =
            static_cast<bthreads::ThreadPoolExecutor*>(StorageEngine::instance()->async_delta_writer_executor());
    auto* pool = executor->get_thread_pool();

    {
        auto st = action.update_config("number_tablet_writer_threads", "8");
        CHECK_OK(st);
        ASSERT_EQ(8, pool->max_threads());
    }

    {
        auto st = action.update_config("number_tablet_writer_threads", "0");
        CHECK_OK(st);
        ASSERT_EQ(CpuInfo::num_cores() / 2, pool->max_threads());
    }
}

TEST_F(UpdateConfigActionTest, test_update_transaction_publish_version_worker_count) {
    UpdateConfigAction action(ExecEnv::GetInstance());

    auto st = action.update_config("transaction_publish_version_worker_count", "8");
    CHECK_OK(st);
    ASSERT_EQ(8, ExecEnv::GetInstance()->put_aggregate_metadata_thread_pool()->max_threads());
}

TEST_F(UpdateConfigActionTest, update_tablet_meta_info_worker_count) {
    UpdateConfigAction action(ExecEnv::GetInstance());

    auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::UPDATE_TABLET_META_INFO);
    ASSERT_NE(thread_pool, nullptr);

    int initial_max_threads = thread_pool->max_threads();
    ASSERT_EQ(initial_max_threads, 10);

    Status st = action.update_config("update_tablet_meta_info_worker_count", "5");
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(thread_pool->max_threads(), 5);

    st = action.update_config("update_tablet_meta_info_worker_count", "10");
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(thread_pool->max_threads(), 10);

    st = action.update_config("update_tablet_meta_info_worker_count", "1");
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(thread_pool->max_threads(), 1);

    st = action.update_config("update_tablet_meta_info_worker_count", std::to_string(initial_max_threads));
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(thread_pool->max_threads(), initial_max_threads);
}

} // namespace starrocks
