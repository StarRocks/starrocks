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

#include <algorithm>
#include <cctype>

#include "agent/agent_common.h"
#include "agent/agent_server.h"
#include "cache/datacache.h"
#include "common/config_diagnostic_fwd.h"
#include "common/logging.h"
#include "common/thread/priority_thread_pool.hpp"
#include "common/thread/threadpool.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "compute_env/workgroup/scan_executor.h"
#include "compute_env/workgroup/work_group_manager.h"
#include "exec/pipeline/primitives/driver_executor.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "runtime/env/global_env.h"
#include "runtime/exec_env.h"

namespace starrocks {

namespace {

bool parse_resource_str(const std::string& str, std::string* value) {
    if (str.empty()) {
        return false;
    }
    std::string tmp_str = str;
    StripLeadingWhiteSpace(&tmp_str);
    StripTrailingWhitespace(&tmp_str);
    if (tmp_str.empty()) {
        return false;
    }
    *value = tmp_str;
    std::transform(value->begin(), value->end(), value->begin(), [](char c) { return std::tolower(c); });
    return true;
}

void try_release_exec_env_resources_before_core_dump(ExecEnv* exec_env, const CoreDumpResourceSelector& selector) {
    if (exec_env == nullptr) {
        return;
    }

    auto* global_env = exec_env->global_env();
    if (global_env == nullptr) {
        return;
    }

    auto* workgroup_manager = exec_env->workgroup_manager();

    if (workgroup_manager != nullptr && selector.should_release("connector_scan_executor")) {
        workgroup_manager->for_each_executors([](auto& executors) { executors.connector_scan_executor()->close(); });
    }
    if (workgroup_manager != nullptr && selector.should_release("olap_scan_executor")) {
        workgroup_manager->for_each_executors([](auto& executors) { executors.scan_executor()->close(); });
    }
    if (global_env->thread_pool() != nullptr && selector.should_release("non_pipeline_scan_thread_pool")) {
        global_env->thread_pool()->shutdown();
    }
    if (global_env->pipeline_prepare_pool() != nullptr && selector.should_release("pipeline_prepare_thread_pool")) {
        global_env->pipeline_prepare_pool()->shutdown();
    }
    if (global_env->pipeline_sink_io_pool() != nullptr && selector.should_release("pipeline_sink_io_thread_pool")) {
        global_env->pipeline_sink_io_pool()->shutdown();
    }
    if (global_env->query_rpc_pool() != nullptr && selector.should_release("query_rpc_thread_pool")) {
        global_env->query_rpc_pool()->shutdown();
    }
    if (global_env->datacache_rpc_pool() != nullptr && selector.should_release("datacache_rpc_thread_pool")) {
        global_env->datacache_rpc_pool()->shutdown();
        LOG(INFO) << "shutdown datacache rpc thread pool";
    }
    if (exec_env->agent_server() != nullptr && selector.should_release("publish_version_worker_pool")) {
        exec_env->agent_server()->stop_task_worker_pool(TaskWorkerType::PUBLISH_VERSION);
    }
    if (workgroup_manager != nullptr && selector.should_release("wg_driver_executor")) {
        workgroup_manager->for_each_executors([](auto& executors) { executors.driver_executor()->close(); });
    }
}

} // namespace

CoreDumpResourceSelector::CoreDumpResourceSelector(const std::string& config_value) {
    if (config_value == "*") {
        _release_all = true;
    } else {
        SplitStringAndParseToContainer(StringPiece(config_value), ",", &parse_resource_str, &_modules);
    }
}

bool CoreDumpResourceSelector::should_release(const std::string& resource_name) const {
    return _release_all || _modules.contains(resource_name);
}

void try_release_resources_before_core_dump(ExecEnv* exec_env, DataCache* data_cache) {
    CoreDumpResourceSelector selector(config::try_release_resource_before_core_dump.value());

    try_release_exec_env_resources_before_core_dump(exec_env, selector);

#ifndef __APPLE__
    if (data_cache != nullptr && selector.should_release("data_cache")) {
        data_cache->release_memory_before_core_dump();
    }
#endif
}

} // namespace starrocks
