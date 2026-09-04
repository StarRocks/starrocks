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

<<<<<<< HEAD
#include "http/action/update_config_action.h"

#include <gtest/gtest.h>

=======
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <iterator>
#include <memory>
#include <string>

>>>>>>> 45b9f5a ([Enhancement] Make starlet object-store upload thresholds configurable at runtime (#78448))
#include "agent/agent_server.h"
#include "cache/datacache.h"
#include "cache/disk_cache/starcache_engine.h"
#include "cache/disk_cache/test_cache_utils.h"
<<<<<<< HEAD
=======
#include "common/config_agent_fwd.h"
#include "common/config_cache_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_staros_worker_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/config_update_registry.h"
#include "common/config_vector_index_fwd.h"
#include "common/configbase.h"
#include "common/system/cpu_info.h"
#include "common/thread/threadpool.h"
#include "common/util/bthreads/executor.h"
#include "data_workflows/load/tablet_writer/load_channel_mgr.h"
#include "exec/exec_env.h"
>>>>>>> 45b9f5a ([Enhancement] Make starlet object-store upload thresholds configurable at runtime (#78448))
#include "fs/fs_util.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"
#include "storage/persistent_index_load_executor.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "testutil/scoped_updater.h"
#include "testutil/sync_point.h"
#include "util/bthreads/executor.h"

#ifdef USE_STAROS
DECLARE_int64(fslib_s3_max_single_part_size);
DECLARE_int64(fslib_s3_min_upload_part_size);
DECLARE_int64(fslib_gs_max_single_part_size);
DECLARE_int64(fslib_azure_storage_max_single_part_size);
DECLARE_int64(fslib_azure_storage_min_upload_part_size);
#endif

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

TEST_F(UpdateConfigActionTest, test_update_tablet_meta_info_worker_count) {
    UpdateConfigAction action(ExecEnv::GetInstance());

    auto* thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::UPDATE_TABLET_META_INFO);
    ASSERT_NE(nullptr, thread_pool);

    auto st = action.update_config("update_tablet_meta_info_worker_count", "4");
    CHECK_OK(st);
    ASSERT_EQ(4, thread_pool->max_threads());

    st = action.update_config("update_tablet_meta_info_worker_count", "0");
    CHECK_OK(st);
    ASSERT_EQ(1, thread_pool->max_threads());
}

TEST_F(UpdateConfigActionTest, test_update_parallel_clone_task_per_path) {
    UpdateConfigAction action(ExecEnv::GetInstance());

    auto* thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::CLONE);
    ASSERT_NE(nullptr, thread_pool);

    auto st = action.update_config("parallel_clone_task_per_path", "4");
    CHECK_OK(st);

    int expected_max_threads = static_cast<int>(ExecEnv::GetInstance()->store_paths().size()) * 4;
    expected_max_threads = std::max(expected_max_threads, 2);
    ASSERT_EQ(expected_max_threads, thread_pool->max_threads());
}

TEST_F(UpdateConfigActionTest, test_update_parallel_clone_task_per_path_with_missing_clone_pool) {
    UpdateConfigAction action(ExecEnv::GetInstance());

    SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                          [](void* arg) { *(ThreadPool**)arg = nullptr; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto st = action.update_config("parallel_clone_task_per_path", "4");
    CHECK_OK(st);
}

TEST_F(UpdateConfigActionTest, test_update_lake_metadata_fetch_thread_count) {
    UpdateConfigAction action(ExecEnv::GetInstance());

    auto* thread_pool = ExecEnv::GetInstance()->lake_metadata_fetch_thread_pool();
    ASSERT_NE(nullptr, thread_pool);
    ASSERT_EQ(std::max(1, config::lake_metadata_fetch_thread_count), thread_pool->max_threads());

    auto st = action.update_config("lake_metadata_fetch_thread_count", "8");
    CHECK_OK(st);
    ASSERT_EQ(8, thread_pool->max_threads());

    // Verify clamped to at least 1
    st = action.update_config("lake_metadata_fetch_thread_count", "0");
    CHECK_OK(st);
    ASSERT_EQ(1, thread_pool->max_threads());
}

<<<<<<< HEAD
=======
#ifdef WITH_TENANN
TEST_F(ConfigUpdateHooksTest, vector_query_cache_capacity_uninitialized_cache_returns_internal_error) {
    auto* storage_env = StorageEnv::GetInstance();
    storage_env->destroy_vector_index_cache();
    auto st = ConfigUpdateRegistry::instance()->update_config("vector_query_cache_capacity", "1G");
    EXPECT_FALSE(st.ok()) << st.to_string();
    EXPECT_TRUE(st.is_internal_error()) << st.to_string();

    ASSERT_OK(storage_env->init_vector_index_cache(RuntimeEnv::GetInstance()->process_mem_limit(),
                                                   RuntimeEnv::GetInstance()->vector_index_mem_tracker()));
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

#ifdef USE_STAROS

namespace {

struct StarletUploadThresholdMapping {
    const char* be_config;
    int64_t* flag;
    int64_t* config_value;
    int64_t valid_value;
};

// Distinct valid_value per row, so deleting one UPDATE_STARLET_CONFIG line or swapping two fails.
const StarletUploadThresholdMapping kStarletUploadThresholdMappings[] = {
        {"starlet_fslib_s3_max_single_part_size", &FLAGS_fslib_s3_max_single_part_size,
         &config::starlet_fslib_s3_max_single_part_size, 21000000},
        {"starlet_fslib_s3_min_upload_part_size", &FLAGS_fslib_s3_min_upload_part_size,
         &config::starlet_fslib_s3_min_upload_part_size, 22000000},
        {"starlet_fslib_gs_max_single_part_size", &FLAGS_fslib_gs_max_single_part_size,
         &config::starlet_fslib_gs_max_single_part_size, 23000000},
        {"starlet_fslib_azure_storage_max_single_part_size", &FLAGS_fslib_azure_storage_max_single_part_size,
         &config::starlet_fslib_azure_storage_max_single_part_size, 24000000},
        {"starlet_fslib_azure_storage_min_upload_part_size", &FLAGS_fslib_azure_storage_min_upload_part_size,
         &config::starlet_fslib_azure_storage_min_upload_part_size, 25000000},
};

constexpr size_t kStarletUploadThresholdMappingCount = std::size(kStarletUploadThresholdMappings);

// Saves the BE configs and every starlet gflag on construction and puts them back on destruction.
// Restores through config::set_config, never by assigning the config global directly:
// Field::set_value maintains the _current_set_val/_last_set_val pair that Field::rollback() reads,
// so a raw assignment would leave that metadata pointing at this test's value and a later rejected
// update in another test could roll back to it.
class ScopedStarletUploadThresholdConfigs {
public:
    ScopedStarletUploadThresholdConfigs() {
        for (size_t i = 0; i < kStarletUploadThresholdMappingCount; ++i) {
            _saved[i] = *kStarletUploadThresholdMappings[i].config_value;
        }
    }

    ~ScopedStarletUploadThresholdConfigs() {
        for (size_t i = 0; i < kStarletUploadThresholdMappingCount; ++i) {
            auto st = config::set_config(kStarletUploadThresholdMappings[i].be_config, std::to_string(_saved[i]));
            EXPECT_TRUE(st.ok()) << kStarletUploadThresholdMappings[i].be_config << ": " << st;
        }
    }

private:
    int64_t _saved[kStarletUploadThresholdMappingCount];
    // Restores every gflag it saw at construction. The destructor body above always completes before
    // any member is destroyed, so the config restore runs first and this undoes the gflag side after.
    gflags::FlagSaver _flag_saver;
};

} // namespace

TEST_F(ConfigUpdateHooksTest, update_starlet_upload_threshold_configs) {
    ScopedStarletUploadThresholdConfigs scoped_configs;

    auto* registry = ConfigUpdateRegistry::instance();
    for (const auto& mapping : kStarletUploadThresholdMappings) {
        // ASSERT_OK is a do/while macro and cannot take a trailing `<< message`, so capture the
        // status and use a streamable native assertion instead.
        auto st = registry->update_config(mapping.be_config, std::to_string(mapping.valid_value));
        ASSERT_TRUE(st.ok()) << mapping.be_config << ": " << st;
        EXPECT_EQ(mapping.valid_value, *mapping.flag) << mapping.be_config;
    }
}

// starlet's validator rejects non-positive values; the registry must roll the BE config back.
// Covers all five mappings, with both 0 and a negative value.
TEST_F(ConfigUpdateHooksTest, update_starlet_upload_threshold_configs_reject_non_positive) {
    ScopedStarletUploadThresholdConfigs scoped_configs;

    auto* registry = ConfigUpdateRegistry::instance();
    for (const auto& mapping : kStarletUploadThresholdMappings) {
        auto st = registry->update_config(mapping.be_config, std::to_string(mapping.valid_value));
        ASSERT_TRUE(st.ok()) << mapping.be_config << ": " << st;
        ASSERT_EQ(mapping.valid_value, *mapping.flag) << mapping.be_config;

        for (const char* bad_value : {"0", "-1"}) {
            EXPECT_FALSE(registry->update_config(mapping.be_config, bad_value).ok())
                    << mapping.be_config << " should reject " << bad_value;
            EXPECT_EQ(mapping.valid_value, *mapping.flag)
                    << mapping.be_config << " flag changed on rejected " << bad_value;
            EXPECT_EQ(mapping.valid_value, *mapping.config_value)
                    << mapping.be_config << " config not rolled back on rejected " << bad_value;
        }
    }
}

#endif // USE_STAROS

>>>>>>> 45b9f5a ([Enhancement] Make starlet object-store upload thresholds configurable at runtime (#78448))
} // namespace starrocks
