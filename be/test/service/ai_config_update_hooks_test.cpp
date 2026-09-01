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

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

#include "base/testutil/assert.h"
#include "common/config_llm_fwd.h"
#include "common/config_update_registry.h"
#include "common/configbase.h"
#include "compute_env/ai/ai_executor.h"
#include "compute_env/compute_env.h"
#include "exec/exec_env.h"
#include "exec/pipeline/driver_executor_factory.h"
#include "exec/pipeline/driver_queue_factory.h"
#include "runtime/runtime_env.h"
#include "service/service_be/config_update_hooks.h"

namespace starrocks {
namespace {

using namespace std::chrono_literals;

AIRuntimeConfig config_from_globals() {
    AIRuntimeConfig config;
    config.request_timeout_ms = config::ai_function_request_timeout_ms;
    config.connect_timeout_ms = config::ai_function_connect_timeout_ms;
    config.max_response_bytes = config::ai_function_max_response_bytes;
    config.worker_thread_num = config::ai_function_worker_thread_num;
    config.sub_chunk_size = config::ai_function_sub_chunk_size;
    config.max_retries = config::ai_function_max_retries;
    config.max_retries_on_throttle = config::ai_function_max_retries_on_throttle;
    config.on_error = config::ai_function_on_error.value();
    config.rate_limit_qps_chat = config::ai_function_rate_limit_qps_chat;
    config.max_inflight = config::ai_function_max_inflight;
    return config;
}

void set_config_globals(const AIRuntimeConfig& config) {
    EXPECT_OK(config::set_config("ai_function_request_timeout_ms", std::to_string(config.request_timeout_ms)));
    EXPECT_OK(config::set_config("ai_function_connect_timeout_ms", std::to_string(config.connect_timeout_ms)));
    EXPECT_OK(config::set_config("ai_function_max_response_bytes", std::to_string(config.max_response_bytes)));
    EXPECT_OK(config::set_config("ai_function_worker_thread_num", std::to_string(config.worker_thread_num)));
    EXPECT_OK(config::set_config("ai_function_sub_chunk_size", std::to_string(config.sub_chunk_size)));
    EXPECT_OK(config::set_config("ai_function_max_retries", std::to_string(config.max_retries)));
    EXPECT_OK(
            config::set_config("ai_function_max_retries_on_throttle", std::to_string(config.max_retries_on_throttle)));
    EXPECT_OK(config::set_config("ai_function_on_error", config.on_error));
    EXPECT_OK(config::set_config("ai_function_rate_limit_qps_chat", std::to_string(config.rate_limit_qps_chat)));
    EXPECT_OK(config::set_config("ai_function_max_inflight", std::to_string(config.max_inflight)));
}

class StartGate {
public:
    void wait() {
        std::unique_lock lock(_mutex);
        ++_waiters;
        _all_waiting.notify_all();
        _start.wait(lock, [this] { return _started; });
    }

    bool wait_for_waiters(size_t count, std::chrono::milliseconds timeout = 5s) {
        std::unique_lock lock(_mutex);
        return _all_waiting.wait_for(lock, timeout, [this, count] { return _waiters >= count; });
    }

    void release() {
        {
            std::lock_guard lock(_mutex);
            _started = true;
        }
        _start.notify_all();
    }

private:
    std::mutex _mutex;
    std::condition_variable _all_waiting;
    std::condition_variable _start;
    size_t _waiters = 0;
    bool _started = false;
};

ComputeEnvOptions make_compute_env_options() {
    ComputeEnvOptions options;
    options.runtime_env = RuntimeEnv::GetInstance();
    options.as_cn = true;
    options.query_cache_capacity = 4 * 1024 * 1024;
    options.driver_queue_factory = pipeline::create_query_shared_driver_queue;
    options.driver_executor_factory = pipeline::create_workgroup_driver_executor;
    return options;
}

class AIConfigUpdateHooksTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_globals = config_from_globals();
        AIRuntimeConfig small = _saved_globals;
        small.worker_thread_num = 1;
        small.max_inflight = 2;
        set_config_globals(small);

        ASSERT_OK(_compute_env.init(make_compute_env_options()));
        _exec_env.set_compute_env(&_compute_env);
        ConfigUpdateRegistry::instance()->TEST_reset();
        register_ai_config_update_hooks(&_exec_env);
        ConfigUpdateRegistry::instance()->set_ready();
    }

    void TearDown() override {
        ConfigUpdateRegistry::instance()->TEST_reset();
        _exec_env.set_compute_env(nullptr);
        _compute_env.destroy();
        set_config_globals(_saved_globals);
    }

    AIExecutor* executor() const { return _compute_env.ai_executor(); }

    AIRuntimeConfig _saved_globals;
    ComputeEnv _compute_env;
    ExecEnv _exec_env;
};

TEST_F(AIConfigUpdateHooksTest, UpdatesAllTenRuntimeConfigs) {
    auto* registry = ConfigUpdateRegistry::instance();

    ASSERT_OK(registry->update_config("ai_function_request_timeout_ms", "1234"));
    ASSERT_OK(registry->update_config("ai_function_connect_timeout_ms", "2345"));
    ASSERT_OK(registry->update_config("ai_function_max_response_bytes", "3456"));
    ASSERT_OK(registry->update_config("ai_function_worker_thread_num", "2"));
    ASSERT_OK(registry->update_config("ai_function_sub_chunk_size", "8"));
    ASSERT_OK(registry->update_config("ai_function_max_retries", "4"));
    ASSERT_OK(registry->update_config("ai_function_max_retries_on_throttle", "6"));
    ASSERT_OK(registry->update_config("ai_function_on_error", "fail"));
    ASSERT_OK(registry->update_config("ai_function_rate_limit_qps_chat", "17"));
    ASSERT_OK(registry->update_config("ai_function_max_inflight", "19"));

    const AIRuntimeConfig snapshot = executor()->config_snapshot();
    EXPECT_EQ(1234, snapshot.request_timeout_ms);
    EXPECT_EQ(2345, snapshot.connect_timeout_ms);
    EXPECT_EQ(3456, snapshot.max_response_bytes);
    EXPECT_EQ(2, snapshot.worker_thread_num);
    EXPECT_EQ(8, snapshot.sub_chunk_size);
    EXPECT_EQ(4, snapshot.max_retries);
    EXPECT_EQ(6, snapshot.max_retries_on_throttle);
    EXPECT_EQ("fail", snapshot.on_error);
    EXPECT_EQ(17, snapshot.rate_limit_qps_chat);
    EXPECT_EQ(19, snapshot.max_inflight);

    EXPECT_EQ(1234, config::ai_function_request_timeout_ms);
    EXPECT_EQ(2345, config::ai_function_connect_timeout_ms);
    EXPECT_EQ(3456, config::ai_function_max_response_bytes);
    EXPECT_EQ(2, config::ai_function_worker_thread_num);
    EXPECT_EQ(8, config::ai_function_sub_chunk_size);
    EXPECT_EQ(4, config::ai_function_max_retries);
    EXPECT_EQ(6, config::ai_function_max_retries_on_throttle);
    EXPECT_EQ("fail", config::ai_function_on_error.value());
    EXPECT_EQ(17, config::ai_function_rate_limit_qps_chat);
    EXPECT_EQ(19, config::ai_function_max_inflight);
}

TEST_F(AIConfigUpdateHooksTest, InvalidUpdatesRollbackGlobalsAndRuntimeSnapshot) {
    const AIRuntimeConfig before = executor()->config_snapshot();
    auto* registry = ConfigUpdateRegistry::instance();

    Status worker_status = registry->update_config("ai_function_worker_thread_num", "0");
    Status inflight_status = registry->update_config("ai_function_max_inflight", "0");
    Status on_error_status = registry->update_config("ai_function_on_error", "continue");

    EXPECT_TRUE(worker_status.is_invalid_argument()) << worker_status;
    EXPECT_TRUE(inflight_status.is_invalid_argument()) << inflight_status;
    EXPECT_TRUE(on_error_status.is_invalid_argument()) << on_error_status;
    EXPECT_EQ(before.worker_thread_num, config::ai_function_worker_thread_num);
    EXPECT_EQ(before.max_inflight, config::ai_function_max_inflight);
    EXPECT_EQ(before.on_error, config::ai_function_on_error.value());

    const AIRuntimeConfig after = executor()->config_snapshot();
    EXPECT_EQ(before.worker_thread_num, after.worker_thread_num);
    EXPECT_EQ(before.max_inflight, after.max_inflight);
    EXPECT_EQ(before.on_error, after.on_error);
}

TEST_F(AIConfigUpdateHooksTest, ShutdownRejectsUpdateAndRollsBackGlobal) {
    const int32_t before_global = config::ai_function_sub_chunk_size;
    const AIRuntimeConfig before = executor()->config_snapshot();
    _compute_env.stop();

    Status status = ConfigUpdateRegistry::instance()->update_config("ai_function_sub_chunk_size", "32");

    EXPECT_TRUE(status.is_shutdown()) << status;
    EXPECT_EQ(before_global, config::ai_function_sub_chunk_size);
    EXPECT_EQ(before.sub_chunk_size, executor()->config_snapshot().sub_chunk_size);
}

TEST_F(AIConfigUpdateHooksTest, ConcurrentDifferentFieldsPreserveBothUpdates) {
    StartGate gate;
    Status request_status;
    Status retry_status;
    auto* registry = ConfigUpdateRegistry::instance();

    std::thread request_update([&] {
        gate.wait();
        request_status = registry->update_config("ai_function_request_timeout_ms", "4321");
    });
    std::thread retry_update([&] {
        gate.wait();
        retry_status = registry->update_config("ai_function_max_retries", "9");
    });
    const bool both_ready = gate.wait_for_waiters(2);
    gate.release();
    request_update.join();
    retry_update.join();

    ASSERT_TRUE(both_ready);
    ASSERT_OK(request_status);
    ASSERT_OK(retry_status);
    const AIRuntimeConfig snapshot = executor()->config_snapshot();
    EXPECT_EQ(4321, snapshot.request_timeout_ms);
    EXPECT_EQ(9, snapshot.max_retries);
    EXPECT_EQ(4321, config::ai_function_request_timeout_ms);
    EXPECT_EQ(9, config::ai_function_max_retries);
}

TEST_F(AIConfigUpdateHooksTest, MissingComputeEnvOrAIExecutorReturnsInternalError) {
    auto* registry = ConfigUpdateRegistry::instance();
    registry->TEST_reset();
    ExecEnv missing_env;
    register_ai_config_update_hooks(&missing_env);
    registry->set_ready();
    const int64_t request_timeout_before = config::ai_function_request_timeout_ms;

    Status missing_compute_env = registry->update_config("ai_function_request_timeout_ms", "7654");

    EXPECT_TRUE(missing_compute_env.is_internal_error()) << missing_compute_env;
    EXPECT_EQ(request_timeout_before, config::ai_function_request_timeout_ms);

    ComputeEnv uninitialized_compute_env;
    missing_env.set_compute_env(&uninitialized_compute_env);
    const int32_t retry_before = config::ai_function_max_retries;
    Status missing_executor = registry->update_config("ai_function_max_retries", "7");

    EXPECT_TRUE(missing_executor.is_internal_error()) << missing_executor;
    EXPECT_EQ(retry_before, config::ai_function_max_retries);
    missing_env.set_compute_env(nullptr);
    registry->TEST_reset();
}

} // namespace
} // namespace starrocks
