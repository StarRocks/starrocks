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

#include <memory>

#include "agent/agent_server.h"
#include "base/testutil/assert.h"
#include "base/testutil/scoped_updater.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "cache/datacache.h"
#include "cache/disk_cache/starcache_engine.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "common/config_agent_fwd.h"
#include "common/config_cache_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/config_update_registry.h"
#include "common/config_vector_index_fwd.h"
#include "common/system/cpu_info.h"
#include "common/thread/threadpool.h"
#include "common/util/bthreads/executor.h"
#include "data_workflows/load/tablet_writer/load_channel_mgr.h"
#include "fs/fs_util.h"
#include "gen_cpp/Types_types.h"
#include "platform/platform_env.h"
#include "platform/store_path.h"
#include "runtime/env/global_env.h"
#include "runtime/exec_env.h"
#include "service/service_be/config_update_hooks.h"
#include "storage/index/vector/vector_index_cache.h"
#include "storage/persistent_index_load_executor.h"
#include "storage/storage_cleanup_executor.h"
#include "storage/storage_engine.h"
#include "storage/storage_env.h"
#include "storage/update_manager.h"

namespace starrocks {

class ConfigUpdateHooksTest : public testing::Test {
public:
    ConfigUpdateHooksTest() = default;
    ~ConfigUpdateHooksTest() override = default;

    void SetUp() override {
        ConfigUpdateRegistry::instance()->TEST_reset();
        _global_env = GlobalEnv::GetInstance();
        _load_channel_mgr = std::make_unique<LoadChannelMgr>(nullptr, GlobalEnv::GetInstance()->diagnose_daemon(),
                                                             PlatformEnv::GetInstance()->brpc_stub_cache());
        ASSERT_OK(_load_channel_mgr->init(_global_env->load_mem_tracker()));
        register_config_update_hooks(ExecEnv::GetInstance(), *_global_env, _load_channel_mgr.get());
        ConfigUpdateRegistry::instance()->set_ready();
    }
    void TearDown() override {
        ConfigUpdateRegistry::instance()->TEST_reset();
        if (_load_channel_mgr != nullptr) {
            _load_channel_mgr->close();
        }
    }

protected:
    GlobalEnv* _global_env = nullptr;
    std::unique_ptr<LoadChannelMgr> _load_channel_mgr;
};

TEST_F(ConfigUpdateHooksTest, update_datacache_config) {
    SCOPED_UPDATE(bool, config::enable_datacache_disk_auto_adjust, false);
    const std::string cache_dir = "./block_cache_for_update_config";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    auto cache = std::make_shared<StarCacheEngine>();
    DiskCacheOptions options = TestCacheUtils::create_simple_options(256 * KB, 0);
    options.dir_spaces.push_back({.path = cache_dir, .size = 50 * MB});
    ASSERT_OK(cache->init(options));
    DataCache::GetInstance()->set_local_disk_cache(cache);

    // update disk size
    ASSERT_ERROR(ConfigUpdateRegistry::instance()->update_config("datacache_disk_size", "-200"));
    ASSERT_OK(ConfigUpdateRegistry::instance()->update_config("datacache_disk_size", "100000000"));
    // update inline cache limit
    ASSERT_OK(ConfigUpdateRegistry::instance()->update_config("datacache_inline_item_count_limit", "260344"));

    std::vector<DirSpace> spaces;
    cache->disk_spaces(&spaces);
    ASSERT_EQ(spaces.size(), 1);
    ASSERT_EQ(spaces[0].size, 100000000);

    fs::remove_all(cache_dir).ok();
}

TEST_F(ConfigUpdateHooksTest, test_update_pindex_load_thread_pool_num_max) {
    ASSERT_OK(ConfigUpdateRegistry::instance()->update_config("pindex_load_thread_pool_num_max", "16"));

    auto* load_pool = StorageEngine::instance()->update_manager()->get_pindex_load_executor()->TEST_get_load_pool();
    ASSERT_EQ(16, load_pool->max_threads());
}

TEST_F(ConfigUpdateHooksTest, test_update_number_tablet_writer_threads) {
    auto* executor =
            static_cast<bthreads::ThreadPoolExecutor*>(StorageEngine::instance()->async_delta_writer_executor());
    auto* pool = executor->get_thread_pool();

    {
        auto st = ConfigUpdateRegistry::instance()->update_config("number_tablet_writer_threads", "8");
        CHECK_OK(st);
        ASSERT_EQ(8, pool->max_threads());
    }

    {
        auto st = ConfigUpdateRegistry::instance()->update_config("number_tablet_writer_threads", "0");
        CHECK_OK(st);
        ASSERT_EQ(CpuInfo::num_cores() / 2, pool->max_threads());
    }
}

TEST_F(ConfigUpdateHooksTest, test_update_transaction_publish_version_worker_count) {
    auto st = ConfigUpdateRegistry::instance()->update_config("transaction_publish_version_worker_count", "8");
    CHECK_OK(st);
    ASSERT_EQ(8, _global_env->put_aggregate_metadata_thread_pool()->max_threads());
}

TEST_F(ConfigUpdateHooksTest, test_update_tablet_meta_info_worker_count) {
    auto* thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::UPDATE_TABLET_META_INFO);
    ASSERT_NE(nullptr, thread_pool);

    auto st = ConfigUpdateRegistry::instance()->update_config("update_tablet_meta_info_worker_count", "4");
    CHECK_OK(st);
    ASSERT_EQ(4, thread_pool->max_threads());

    st = ConfigUpdateRegistry::instance()->update_config("update_tablet_meta_info_worker_count", "0");
    CHECK_OK(st);
    ASSERT_EQ(1, thread_pool->max_threads());
}

TEST_F(ConfigUpdateHooksTest, test_update_parallel_clone_task_per_path) {
    auto* thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::CLONE);
    ASSERT_NE(nullptr, thread_pool);

    auto st = ConfigUpdateRegistry::instance()->update_config("parallel_clone_task_per_path", "4");
    CHECK_OK(st);

    const auto* store_path_registry = ExecEnv::GetInstance()->platform_services().store_path_registry;
    ASSERT_NE(nullptr, store_path_registry);
    int expected_max_threads = static_cast<int>(store_path_registry->store_path_count()) * 4;
    expected_max_threads = std::max(expected_max_threads, 2);
    ASSERT_EQ(expected_max_threads, thread_pool->max_threads());
}

TEST_F(ConfigUpdateHooksTest, test_update_parallel_clone_task_per_path_with_missing_clone_pool) {
    SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                          [](void* arg) { *(ThreadPool**)arg = nullptr; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto st = ConfigUpdateRegistry::instance()->update_config("parallel_clone_task_per_path", "4");
    CHECK_OK(st);
}

TEST_F(ConfigUpdateHooksTest, test_update_lake_schema_change_pool_size) {
    auto* thread_pool = StorageEngine::instance()->lake_schema_change_thread_pool();
    ASSERT_NE(nullptr, thread_pool);

    const int original_alter_tablet_worker_count = config::alter_tablet_worker_count;
    const int original_lake_schema_change_parallelism = config::lake_schema_change_per_tablet_parallelism;
    DeferOp defer([&]() {
        CHECK_OK(ConfigUpdateRegistry::instance()->update_config("alter_tablet_worker_count",
                                                                 std::to_string(original_alter_tablet_worker_count)));
        CHECK_OK(ConfigUpdateRegistry::instance()->update_config(
                "lake_schema_change_per_tablet_parallelism", std::to_string(original_lake_schema_change_parallelism)));
    });

    auto st = ConfigUpdateRegistry::instance()->update_config("lake_schema_change_per_tablet_parallelism", "3");
    CHECK_OK(st);
    ASSERT_EQ(std::max(1, config::alter_tablet_worker_count * 3), thread_pool->max_threads());

    st = ConfigUpdateRegistry::instance()->update_config("alter_tablet_worker_count", "2");
    CHECK_OK(st);
    ASSERT_EQ(6, thread_pool->max_threads());
}

TEST_F(ConfigUpdateHooksTest, test_update_storage_cleanup_worker_count) {
    auto* storage_cleanup_executor = StorageEngine::instance()->storage_cleanup_executor();
    ASSERT_NE(nullptr, storage_cleanup_executor);
    auto* storage_cleanup_pool = storage_cleanup_executor->thread_pool();
    ASSERT_NE(nullptr, storage_cleanup_pool);

    auto* drop_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::DROP);
    ASSERT_NE(nullptr, drop_pool);
    const auto original_drop_pool_max_threads = drop_pool->max_threads();
    const auto original_drop_tablet_worker_count = config::drop_tablet_worker_count;
    const auto original_storage_cleanup_worker_count = config::storage_cleanup_worker_count;
    DeferOp defer([&]() {
        CHECK_OK(ConfigUpdateRegistry::instance()->update_config("drop_tablet_worker_count",
                                                                 std::to_string(original_drop_tablet_worker_count)));
        CHECK_OK(ConfigUpdateRegistry::instance()->update_config(
                "storage_cleanup_worker_count", std::to_string(original_storage_cleanup_worker_count)));
    });

    auto st = ConfigUpdateRegistry::instance()->update_config("storage_cleanup_worker_count", "4");
    CHECK_OK(st);
    ASSERT_EQ(4, storage_cleanup_pool->max_threads());
    ASSERT_EQ(original_drop_pool_max_threads, drop_pool->max_threads());

    st = ConfigUpdateRegistry::instance()->update_config("drop_tablet_worker_count", "2");
    CHECK_OK(st);
    ASSERT_EQ(2, drop_pool->max_threads());
    ASSERT_EQ(4, storage_cleanup_pool->max_threads());
}

TEST_F(ConfigUpdateHooksTest, test_update_lake_metadata_fetch_thread_count) {
    auto* thread_pool = _global_env->lake_metadata_fetch_thread_pool();
    ASSERT_NE(nullptr, thread_pool);
    ASSERT_EQ(std::max(1, config::lake_metadata_fetch_thread_count), thread_pool->max_threads());

    auto st = ConfigUpdateRegistry::instance()->update_config("lake_metadata_fetch_thread_count", "8");
    CHECK_OK(st);
    ASSERT_EQ(8, thread_pool->max_threads());

    // Verify clamped to at least 1
    st = ConfigUpdateRegistry::instance()->update_config("lake_metadata_fetch_thread_count", "0");
    CHECK_OK(st);
    ASSERT_EQ(1, thread_pool->max_threads());
}

#ifdef WITH_TENANN
TEST_F(ConfigUpdateHooksTest, vector_query_cache_capacity_uninitialized_cache_returns_internal_error) {
    auto* storage_env = StorageEnv::GetInstance();
    storage_env->destroy_vector_index_cache();
    auto st = ConfigUpdateRegistry::instance()->update_config("vector_query_cache_capacity", "1G");
    EXPECT_FALSE(st.ok()) << st.to_string();
    EXPECT_TRUE(st.is_internal_error()) << st.to_string();

    ASSERT_OK(storage_env->init_vector_index_cache(GlobalEnv::GetInstance()->process_mem_limit(),
                                                   GlobalEnv::GetInstance()->vector_index_mem_tracker()));
}

TEST_F(ConfigUpdateHooksTest, vector_query_cache_capacity_happy_path_resizes_cache) {
    auto* cache = StorageEnv::GetInstance()->vector_index_cache();
    ASSERT_NE(cache, nullptr) << "test_main must initialize StorageEnv with vector_index_cache";
    const std::string saved = config::vector_query_cache_capacity;

    // Absolute bytes.
    ASSERT_OK(ConfigUpdateRegistry::instance()->update_config("vector_query_cache_capacity", "4294967296"));
    EXPECT_EQ(cache->capacity(), 4294967296u);

    // Unit-suffixed.
    ASSERT_OK(ConfigUpdateRegistry::instance()->update_config("vector_query_cache_capacity", "512M"));
    EXPECT_EQ(cache->capacity(), 512u * 1024 * 1024);

    // Percentage of process_mem_limit — exact value depends on test env, just
    // sanity-check it parses and resizes to something positive.
    ASSERT_OK(ConfigUpdateRegistry::instance()->update_config("vector_query_cache_capacity", "10%"));
    EXPECT_GT(cache->capacity(), 0u);

    // Restore for downstream tests/files.
    ASSERT_OK(ConfigUpdateRegistry::instance()->update_config("vector_query_cache_capacity", saved));
}
#endif

} // namespace starrocks
