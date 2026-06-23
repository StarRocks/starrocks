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

#include "base/testutil/assert.h"
#include "platform/store_path.h"
#include "runtime/mem_tracker.h"
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

} // namespace starrocks
