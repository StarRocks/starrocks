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

#include "storage/storage_env.h"

#include <gtest/gtest.h>

#include <string>

#ifdef WITH_TENANN
#include "tenann/index/index_cache.h"
#endif

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "common/config_vector_index_fwd.h"
#include "platform/store_path.h"
#include "runtime/mem_tracker.h"
#include "storage/index/vector/vector_index_cache.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks {

TEST(StorageEnvTest, disabled_init_leaves_lake_resources_null) {
    StorageEnv env;
    StorageEnvOptions options;

    ASSERT_OK(env.init(options));

    EXPECT_EQ(env.lake_location_provider().get(), nullptr);
    EXPECT_EQ(env.lake_update_manager(), nullptr);
    EXPECT_EQ(env.lake_tablet_manager(), nullptr);
    EXPECT_EQ(env.lake_replication_txn_manager(), nullptr);
    EXPECT_EQ(env.parallel_compact_mgr(), nullptr);
    EXPECT_EQ(env.vector_index_cache(), nullptr);

    env.stop();
    env.stop_lake_tablet_manager();
    env.destroy();
}

TEST(StorageEnvTest, fixed_provider_init_owns_lake_resources) {
    StorageEnv env;
    MemTracker update_mem_tracker(-1, "storage_env_test");
    StorageEnvOptions options;
    options.lake_location_provider_mode = LakeLocationProviderMode::kFixed;
    StorePathRegistry store_path_registry;
    ASSERT_OK(store_path_registry.init({StorePath("storage_env_test_root")}));
    options.store_path_registry = &store_path_registry;
    options.update_mem_tracker = &update_mem_tracker;
    options.lake_metadata_cache_limit = 1024 * 1024;

    ASSERT_OK(env.init(options));

    EXPECT_NE(env.lake_location_provider().get(), nullptr);
    EXPECT_NE(env.lake_update_manager(), nullptr);
    EXPECT_NE(env.lake_tablet_manager(), nullptr);
    EXPECT_EQ(env.lake_tablet_manager()->store_path_registry(), &store_path_registry);
    EXPECT_NE(env.lake_replication_txn_manager(), nullptr);
    EXPECT_NE(env.parallel_compact_mgr(), nullptr);

    ASSERT_OK(env.init(options));
    env.stop();
    env.stop();
    env.stop_lake_tablet_manager();
    env.stop_lake_tablet_manager();
    env.destroy();
    env.destroy();

    EXPECT_EQ(env.lake_location_provider().get(), nullptr);
    EXPECT_EQ(env.lake_update_manager(), nullptr);
    EXPECT_EQ(env.lake_tablet_manager(), nullptr);
    EXPECT_EQ(env.lake_replication_txn_manager(), nullptr);
    EXPECT_EQ(env.parallel_compact_mgr(), nullptr);
}

TEST(StorageEnvTest, fixed_provider_requires_store_path) {
    StorageEnv env;
    MemTracker update_mem_tracker(-1, "storage_env_test");
    StorageEnvOptions options;
    options.lake_location_provider_mode = LakeLocationProviderMode::kFixed;
    StorePathRegistry store_path_registry;
    options.store_path_registry = &store_path_registry;
    options.update_mem_tracker = &update_mem_tracker;
    options.lake_metadata_cache_limit = 1024 * 1024;

    auto status = env.init(options);
    EXPECT_TRUE(status.is_invalid_argument()) << status;
}

#ifdef WITH_TENANN
TEST(StorageEnvTest, vector_index_cache_init_installs_and_destroy_clears_global) {
    auto* saved_cache = tenann::GetGlobalIndexCache();
    DeferOp restore_global([&] { tenann::SetGlobalIndexCache(saved_cache); });
    tenann::SetGlobalIndexCache(nullptr);

    const std::string saved_capacity = config::vector_query_cache_capacity.value();
    DeferOp restore_capacity([&] { config::vector_query_cache_capacity = saved_capacity; });
    config::vector_query_cache_capacity = std::string("1024");

    MemTracker vector_index_mem_tracker(-1, "storage_env_vector_index_test");
    StorageEnv env;

    ASSERT_OK(env.init_vector_index_cache(1024 * 1024, &vector_index_mem_tracker));
    ASSERT_NE(env.vector_index_cache(), nullptr);
    EXPECT_EQ(env.vector_index_cache()->capacity(), 1024u);
    EXPECT_EQ(tenann::GetGlobalIndexCache(), env.vector_index_cache());

    ASSERT_OK(env.init_vector_index_cache(1024 * 1024, &vector_index_mem_tracker));
    EXPECT_EQ(env.vector_index_cache()->capacity(), 1024u);

    env.destroy_vector_index_cache();
    EXPECT_EQ(env.vector_index_cache(), nullptr);
    EXPECT_EQ(tenann::GetGlobalIndexCache(), nullptr);
}

TEST(StorageEnvTest, vector_index_cache_destroy_ignores_cache_owned_by_other_instance) {
    auto* saved_cache = tenann::GetGlobalIndexCache();
    DeferOp restore_global([&] { tenann::SetGlobalIndexCache(saved_cache); });
    tenann::SetGlobalIndexCache(nullptr);

    const std::string saved_capacity = config::vector_query_cache_capacity.value();
    DeferOp restore_capacity([&] { config::vector_query_cache_capacity = saved_capacity; });
    config::vector_query_cache_capacity = std::string("1024");

    MemTracker vector_index_mem_tracker(-1, "storage_env_vector_index_test");
    StorageEnv owner;
    ASSERT_OK(owner.init_vector_index_cache(1024 * 1024, &vector_index_mem_tracker));
    auto* installed_cache = tenann::GetGlobalIndexCache();
    ASSERT_EQ(installed_cache, owner.vector_index_cache());

    { StorageEnv unrelated_env; }
    EXPECT_EQ(tenann::GetGlobalIndexCache(), installed_cache);

    owner.destroy_vector_index_cache();
    EXPECT_EQ(tenann::GetGlobalIndexCache(), nullptr);
}

TEST(StorageEnvTest, vector_index_cache_init_requires_tracker) {
    StorageEnv env;
    auto status = env.init_vector_index_cache(1024 * 1024, nullptr);
    EXPECT_TRUE(status.is_invalid_argument()) << status;
}
#endif

} // namespace starrocks
