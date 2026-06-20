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

#include "service/core_dump_resource_releaser.h"

#include <atomic>
#include <cctype>

#include "agent/agent_common.h"
#include "agent/agent_server.h"
#include "cache/datacache.h"
#include "common/config_diagnostic_fwd.h"
#include "common/thread/priority_thread_pool.hpp"
#include "common/thread/threadpool.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "compute_env/workgroup/scan_executor.h"
#include "compute_env/workgroup/work_group_manager.h"
#include "exec/pipeline/primitives/driver_executor.h"
#include "runtime/env/global_env.h"
#include "runtime/exec_env.h"

namespace starrocks {

namespace {

constexpr uint32_t kDataCache = 1u << 0;
constexpr uint32_t kConnectorScanExecutor = 1u << 1;
constexpr uint32_t kOlapScanExecutor = 1u << 2;
constexpr uint32_t kNonPipelineScanThreadPool = 1u << 3;
constexpr uint32_t kPipelinePrepareThreadPool = 1u << 4;
constexpr uint32_t kPipelineSinkIoThreadPool = 1u << 5;
constexpr uint32_t kQueryRpcThreadPool = 1u << 6;
constexpr uint32_t kDatacacheRpcThreadPool = 1u << 7;
constexpr uint32_t kPublishVersionWorkerPool = 1u << 8;
constexpr uint32_t kWorkgroupDriverExecutor = 1u << 9;

constexpr uint32_t kAllResourceMask = kDataCache | kConnectorScanExecutor | kOlapScanExecutor |
                                      kNonPipelineScanThreadPool | kPipelinePrepareThreadPool |
                                      kPipelineSinkIoThreadPool | kQueryRpcThreadPool | kDatacacheRpcThreadPool |
                                      kPublishVersionWorkerPool | kWorkgroupDriverExecutor;

struct ResourceName {
    std::string_view name;
    uint32_t bit;
};

constexpr ResourceName kResourceNames[] = {
        {"data_cache", kDataCache},
        {"connector_scan_executor", kConnectorScanExecutor},
        {"olap_scan_executor", kOlapScanExecutor},
        {"non_pipeline_scan_thread_pool", kNonPipelineScanThreadPool},
        {"pipeline_prepare_thread_pool", kPipelinePrepareThreadPool},
        {"pipeline_sink_io_thread_pool", kPipelineSinkIoThreadPool},
        {"query_rpc_thread_pool", kQueryRpcThreadPool},
        {"datacache_rpc_thread_pool", kDatacacheRpcThreadPool},
        {"publish_version_worker_pool", kPublishVersionWorkerPool},
        {"wg_driver_executor", kWorkgroupDriverExecutor},
};

// The failure handler can run after OOM or heap corruption. Parse the mutable
// config on normal paths and publish a bitmask so the crash path avoids building
// std::set/std::string containers while trying to reduce the core file.
std::atomic<uint32_t> g_core_dump_resource_mask{0};

std::string_view trim_resource_name(std::string_view resource_name) {
    while (!resource_name.empty() && std::isspace(static_cast<unsigned char>(resource_name.front()))) {
        resource_name.remove_prefix(1);
    }
    while (!resource_name.empty() && std::isspace(static_cast<unsigned char>(resource_name.back()))) {
        resource_name.remove_suffix(1);
    }
    return resource_name;
}

bool equals_ignore_case(std::string_view lhs, std::string_view rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(lhs[i])) != rhs[i]) {
            return false;
        }
    }
    return true;
}

uint32_t resource_bit_for_name(std::string_view resource_name) {
    for (const auto& resource : kResourceNames) {
        if (equals_ignore_case(resource_name, resource.name)) {
            return resource.bit;
        }
    }
    return 0;
}

uint32_t parse_resource_mask(std::string_view config_value) {
    if (config_value == "*") {
        return kAllResourceMask;
    }

    uint32_t mask = 0;
    size_t token_start = 0;
    while (token_start <= config_value.size()) {
        const size_t token_end = config_value.find(',', token_start);
        const auto token = trim_resource_name(config_value.substr(
                token_start, token_end == std::string_view::npos ? std::string_view::npos : token_end - token_start));
        if (!token.empty()) {
            mask |= resource_bit_for_name(token);
        }
        if (token_end == std::string_view::npos) {
            break;
        }
        token_start = token_end + 1;
    }
    return mask;
}

bool mask_should_release(uint32_t mask, uint32_t resource_bit) {
    return (mask & resource_bit) != 0;
}

void try_release_exec_env_resources_before_core_dump(ExecEnv* exec_env, uint32_t mask) {
    if (exec_env == nullptr) {
        return;
    }

    auto* workgroup_manager = exec_env->workgroup_manager();
    if (workgroup_manager != nullptr && mask_should_release(mask, kConnectorScanExecutor)) {
        workgroup_manager->for_each_executors([](auto& executors) { executors.connector_scan_executor()->close(); });
    }
    if (workgroup_manager != nullptr && mask_should_release(mask, kOlapScanExecutor)) {
        workgroup_manager->for_each_executors([](auto& executors) { executors.scan_executor()->close(); });
    }
    if (exec_env->agent_server() != nullptr && mask_should_release(mask, kPublishVersionWorkerPool)) {
        exec_env->agent_server()->stop_task_worker_pool(TaskWorkerType::PUBLISH_VERSION);
    }
    if (workgroup_manager != nullptr && mask_should_release(mask, kWorkgroupDriverExecutor)) {
        workgroup_manager->for_each_executors([](auto& executors) { executors.driver_executor()->close(); });
    }
}

void try_release_global_env_resources_before_core_dump(GlobalEnv* global_env, uint32_t mask) {
    if (global_env == nullptr) {
        return;
    }

    if (global_env->thread_pool() != nullptr && mask_should_release(mask, kNonPipelineScanThreadPool)) {
        global_env->thread_pool()->shutdown();
    }
    if (global_env->pipeline_prepare_pool() != nullptr && mask_should_release(mask, kPipelinePrepareThreadPool)) {
        global_env->pipeline_prepare_pool()->shutdown();
    }
    if (global_env->pipeline_sink_io_pool() != nullptr && mask_should_release(mask, kPipelineSinkIoThreadPool)) {
        global_env->pipeline_sink_io_pool()->shutdown();
    }
    if (global_env->query_rpc_pool() != nullptr && mask_should_release(mask, kQueryRpcThreadPool)) {
        global_env->query_rpc_pool()->shutdown();
    }
    if (global_env->datacache_rpc_pool() != nullptr && mask_should_release(mask, kDatacacheRpcThreadPool)) {
        global_env->datacache_rpc_pool()->shutdown();
    }
}

} // namespace

CoreDumpResourceSelector::CoreDumpResourceSelector(std::string_view config_value)
        : _mask(parse_resource_mask(config_value)), _release_all(config_value == "*") {}

bool CoreDumpResourceSelector::should_release(std::string_view resource_name) const {
    return mask_should_release(_mask, resource_bit_for_name(resource_name));
}

void refresh_core_dump_resource_releaser_config() {
    const auto config_value = config::try_release_resource_before_core_dump.value();
    const CoreDumpResourceSelector selector(config_value);
    g_core_dump_resource_mask.store(selector.mask(), std::memory_order_relaxed);
}

void try_release_resources_before_core_dump(ExecEnv* exec_env, GlobalEnv* global_env, DataCache* data_cache) {
    const uint32_t mask = g_core_dump_resource_mask.load(std::memory_order_relaxed);
    if (mask == 0) {
        return;
    }

    try_release_exec_env_resources_before_core_dump(exec_env, mask);
    try_release_global_env_resources_before_core_dump(global_env, mask);

#ifndef __APPLE__
    if (data_cache != nullptr && mask_should_release(mask, kDataCache)) {
        data_cache->release_memory_before_core_dump();
    }
#endif
}

} // namespace starrocks
