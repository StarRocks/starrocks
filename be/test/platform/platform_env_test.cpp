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

#include "platform/platform_env.h"

#include <gtest/gtest.h>

#include <utility>
#include <vector>

#include "base/metrics.h"
#include "base/testutil/assert.h"
#include "common/brpc/brpc_stub_cache.h"
#include "platform/store_path.h"
#include "platform/thrift_rpc_helper.h"

namespace starrocks {

TEST(PlatformEnvTest, OwnsRpcTransportCacheAccessors) {
    auto* env = PlatformEnv::GetInstance();
    env->destroy();

    MetricRegistry metrics("platform_env_test");
    ASSERT_OK(env->init(PlatformEnvOptions{.metrics = &metrics}));

    ASSERT_NE(env->backend_client_cache(), nullptr);
    ASSERT_NE(env->frontend_client_cache(), nullptr);
    ASSERT_NE(env->broker_client_cache(), nullptr);
    ASSERT_NE(env->brpc_stub_cache(), nullptr);
    EXPECT_EQ(ThriftRpcHelper::_s_backend_client_cache, env->backend_client_cache());
    EXPECT_EQ(ThriftRpcHelper::_s_frontend_client_cache, env->frontend_client_cache());
    EXPECT_EQ(ThriftRpcHelper::_s_broker_client_cache, env->broker_client_cache());
    ASSERT_NE(env->http_brpc_stub_cache(), nullptr);
#ifndef __APPLE__
    ASSERT_NE(env->lake_service_brpc_stub_cache(), nullptr);
#endif

    env->destroy();
    EXPECT_EQ(env->backend_client_cache(), nullptr);
    EXPECT_EQ(env->frontend_client_cache(), nullptr);
    EXPECT_EQ(env->broker_client_cache(), nullptr);
    EXPECT_EQ(env->brpc_stub_cache(), nullptr);
    EXPECT_EQ(ThriftRpcHelper::_s_backend_client_cache, nullptr);
    EXPECT_EQ(ThriftRpcHelper::_s_frontend_client_cache, nullptr);
    EXPECT_EQ(ThriftRpcHelper::_s_broker_client_cache, nullptr);

    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto http_stub = env->http_brpc_stub_cache()->get_http_stub(address);
    ASSERT_FALSE(http_stub.ok());
    EXPECT_TRUE(http_stub.status().is_service_unavailable());
#ifndef __APPLE__
    auto lake_stub = env->lake_service_brpc_stub_cache()->get_stub(address.hostname, address.port);
    ASSERT_FALSE(lake_stub.ok());
    EXPECT_TRUE(lake_stub.status().is_service_unavailable());
#endif
}

TEST(PlatformEnvTest, OwnsStorePathsFromInitOptions) {
    auto* env = PlatformEnv::GetInstance();
    env->destroy();
    env->reset_store_paths_for_test();

    std::vector<StorePath> paths;
    paths.emplace_back("/path1");
    paths.emplace_back("/path2");
    paths[1].storage_medium = TStorageMedium::SSD;

    MetricRegistry metrics("platform_env_store_path_test");
    PlatformEnvOptions options;
    options.metrics = &metrics;
    options.store_paths = paths;
    ASSERT_OK(env->init(std::move(options)));

    const auto* store_path_registry = env->store_path_registry();
    ASSERT_NE(nullptr, store_path_registry);
    ASSERT_EQ(2, store_path_registry->store_path_count());
    EXPECT_TRUE(store_path_registry->has_store_paths());
    ASSERT_EQ(2, store_path_registry->store_paths().size());
    EXPECT_EQ("/path1", store_path_registry->store_paths()[0].path);
    EXPECT_EQ(TStorageMedium::HDD, store_path_registry->store_paths()[0].storage_medium);
    EXPECT_EQ("/path2", store_path_registry->store_paths()[1].path);
    EXPECT_EQ(TStorageMedium::SSD, store_path_registry->store_paths()[1].storage_medium);
    ASSERT_EQ(2, store_path_registry->store_path_roots().size());
    EXPECT_EQ("/path1", store_path_registry->store_path_roots()[0]);
    EXPECT_EQ("/path2", store_path_registry->store_path_roots()[1]);

    env->destroy();
    EXPECT_EQ(2, store_path_registry->store_path_count());
    env->reset_store_paths_for_test();
}

TEST(PlatformEnvTest, RejectsConflictingStorePathReinitUntilTestReset) {
    auto* env = PlatformEnv::GetInstance();
    env->destroy();
    env->reset_store_paths_for_test();

    MetricRegistry metrics("platform_env_store_path_reinit_test");
    PlatformEnvOptions options;
    options.metrics = &metrics;
    options.store_paths = {StorePath("/path1")};
    ASSERT_OK(env->init(std::move(options)));

    PlatformEnvOptions same_options;
    same_options.metrics = &metrics;
    same_options.store_paths = {StorePath("/path1")};
    ASSERT_OK(env->init(std::move(same_options)));

    PlatformEnvOptions different_options;
    different_options.metrics = &metrics;
    different_options.store_paths = {StorePath("/path2")};
    EXPECT_FALSE(env->init(std::move(different_options)).ok());

    env->reset_store_paths_for_test();
    PlatformEnvOptions reset_options;
    reset_options.metrics = &metrics;
    reset_options.store_paths = {StorePath("/path2")};
    ASSERT_OK(env->init(std::move(reset_options)));
    const auto* store_path_registry = env->store_path_registry();
    ASSERT_NE(nullptr, store_path_registry);
    ASSERT_EQ(1, store_path_registry->store_path_count());
    EXPECT_EQ("/path2", store_path_registry->store_paths()[0].path);

    env->destroy();
    env->reset_store_paths_for_test();
}

} // namespace starrocks
