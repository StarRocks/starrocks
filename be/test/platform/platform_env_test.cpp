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
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "base/metrics.h"
#include "base/testutil/assert.h"
#include "common/brpc/brpc_stub_cache.h"
#include "common/config_path_fwd.h"
#include "platform/small_file_mgr.h"
#include "platform/store_path.h"
#include "platform/thrift_rpc_helper.h"

namespace starrocks {

namespace {

class ScopedSmallFileDir {
public:
    ScopedSmallFileDir() {
        _old_small_file_dir = config::small_file_dir;
        _path = std::filesystem::temp_directory_path() / ("starrocks_platform_env_test_" + std::to_string(getpid()) +
                                                          "_" + std::to_string(_next_id.fetch_add(1)));
        std::filesystem::remove_all(_path);
        std::filesystem::create_directories(_path);
        config::small_file_dir = _path.string();
    }

    ~ScopedSmallFileDir() {
        config::small_file_dir = _old_small_file_dir;
        std::error_code ec;
        std::filesystem::remove_all(_path, ec);
    }

    const std::filesystem::path& path() const { return _path; }

private:
    inline static std::atomic<int> _next_id{0};
    std::string _old_small_file_dir;
    std::filesystem::path _path;
};

class PlatformEnvResetGuard {
public:
    explicit PlatformEnvResetGuard(PlatformEnv* env) : _env(env) {}
    ~PlatformEnvResetGuard() {
        _env->destroy();
        _env->reset_store_paths_for_test();
    }

private:
    PlatformEnv* _env;
};

} // namespace

TEST(PlatformEnvTest, OwnsRpcTransportCacheAccessors) {
    auto* env = PlatformEnv::GetInstance();
    env->destroy();
    ScopedSmallFileDir small_file_dir;

    MetricRegistry metrics("platform_env_test");
    PlatformEnvResetGuard guard(env);
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
    EXPECT_EQ(env->small_file_mgr(), nullptr);
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

TEST(PlatformEnvTest, OwnsSmallFileMgrLifecycle) {
    auto* env = PlatformEnv::GetInstance();
    env->destroy();
    ScopedSmallFileDir small_file_dir;

    static constexpr int64_t kFileId = 12345;
    static constexpr const char* kEmptyFileMd5 = "d41d8cd98f00b204e9800998ecf8427e";
    const auto cached_file = small_file_dir.path() / (std::to_string(kFileId) + "." + kEmptyFileMd5);
    std::ofstream(cached_file.string()).close();

    MetricRegistry metrics("platform_env_small_file_test");
    PlatformEnvResetGuard guard(env);
    ASSERT_OK(env->init(PlatformEnvOptions{.metrics = &metrics}));
    ASSERT_NE(env->small_file_mgr(), nullptr);

    std::string file_path;
    ASSERT_OK(env->small_file_mgr()->get_file(kFileId, kEmptyFileMd5, &file_path));
    EXPECT_EQ(cached_file.string(), file_path);

    metrics.trigger_hook();
    auto* small_file_metric = metrics.get_metric("small_file_cache_count");
    ASSERT_NE(nullptr, small_file_metric);
    EXPECT_EQ("1", small_file_metric->to_string());

    env->destroy();
    EXPECT_EQ(env->small_file_mgr(), nullptr);
}

TEST(PlatformEnvTest, OwnsStorePathsFromInitOptions) {
    auto* env = PlatformEnv::GetInstance();
    env->destroy();
    env->reset_store_paths_for_test();
    ScopedSmallFileDir small_file_dir;

    std::vector<StorePath> paths;
    paths.emplace_back("/path1");
    paths.emplace_back("/path2");
    paths[1].storage_medium = TStorageMedium::SSD;

    MetricRegistry metrics("platform_env_store_path_test");
    PlatformEnvResetGuard guard(env);
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
    ScopedSmallFileDir small_file_dir;

    MetricRegistry metrics("platform_env_store_path_reinit_test");
    PlatformEnvResetGuard guard(env);
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
