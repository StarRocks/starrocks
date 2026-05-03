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

#include "service/backend_metrics_initializer.h"

#include <unistd.h>

#include <cerrno>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <set>
#include <sstream>
#include <utility>

#include "agent/agent_metrics.h"
#include "base/compression/compression_context_pool_metrics.h"
#include "base/network/network_util.h"
#include "cache/datacache_metrics.h"
#include "common/config_metrics_fwd.h"
#include "common/metrics/process_metrics_registry.h"
#include "common/status.h"
#include "common/system/backend_options.h"
#include "common/system/disk_info.h"
#include "exec/pipeline/pipeline_metrics.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "http/http_metrics.h"
#include "runtime/runtime_metrics.h"
#include "runtime/stream_load/stream_load_metrics.h"
#include "service/service_metrics.h"
#include "storage/storage_metrics.h"
#ifndef __APPLE__
#include "util/jvm_metrics.h"
#endif
#include "util/logging.h"
#include "util/metrics/catalog_scan_metrics.h"
#include "util/metrics/file_scan_metrics.h"
#include "util/metrics/flat_json_metrics.h"
#include "util/metrics/query_scan_metrics.h"
#include "util/metrics/spill_metrics.h"
#include "util/system_metrics.h"

namespace starrocks {

namespace {

const char* const kRuntimeMetricsHookName = "runtime_metrics";

bool is_known_disk_device_name(const std::string& dev) {
    for (int i = 0; i < DiskInfo::num_disks(); ++i) {
        if (DiskInfo::device_name(i) == dev) {
            return true;
        }
    }
    return false;
}

Status get_disk_devices(const std::vector<std::string>& paths, std::set<std::string>* devices) {
    std::vector<std::string> real_paths;
    real_paths.reserve(paths.size());
    for (const auto& path : paths) {
        std::string real_path;
        Status st = fs::canonicalize(path, &real_path);
        if (!st.ok()) {
            WARN_IF_ERROR(st, "canonicalize path " + path + " failed, skip disk monitoring of this path");
            continue;
        }
        real_paths.emplace_back(std::move(real_path));
    }

    FILE* fp = fopen("/proc/mounts", "r");
    if (fp == nullptr) {
        std::stringstream ss;
        char buf[64];
        ss << "open /proc/mounts failed, errno:" << errno << ", message:" << strerror_r(errno, buf, 64);
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    Status status;
    char* line_ptr = nullptr;
    size_t line_buf_size = 0;
    for (const auto& path : real_paths) {
        size_t max_mount_size = 0;
        std::string match_dev;
        rewind(fp);
        while (getline(&line_ptr, &line_buf_size, fp) > 0) {
            char dev_path[4096];
            char mount_path[4096];
            int num = sscanf(line_ptr, "%4095s %4095s", dev_path, mount_path);
            if (num < 2) {
                continue;
            }
            size_t mount_size = strlen(mount_path);
            if (mount_size < max_mount_size || path.size() < mount_size ||
                strncmp(path.c_str(), mount_path, mount_size) != 0) {
                continue;
            }
            std::string dev = std::filesystem::path(dev_path).filename().string();
            if (is_known_disk_device_name(dev)) {
                max_mount_size = mount_size;
                match_dev = std::move(dev);
            }
        }
        if (ferror(fp) != 0) {
            std::stringstream ss;
            char buf[64];
            ss << "open /proc/mounts failed, errno:" << errno << ", message:" << strerror_r(errno, buf, 64);
            LOG(WARNING) << ss.str();
            status = Status::InternalError(ss.str());
            break;
        }
        if (max_mount_size > 0) {
            devices->emplace(match_dev);
        }
    }
    if (line_ptr != nullptr) {
        free(line_ptr);
    }
    fclose(fp);
    return status;
}

bool prepare_system_metrics_inputs(const BackendMetricsInitOptions& options, std::set<std::string>* disk_devices,
                                   std::vector<std::string>* network_interfaces) {
    auto st = get_disk_devices(options.storage_paths, disk_devices);
    if (!st.ok()) {
        LOG(WARNING) << "get disk devices failed, status=" << st.message();
        return false;
    }
    st = get_inet_interfaces(network_interfaces, options.bind_ipv6);
    if (!st.ok()) {
        LOG(WARNING) << "get inet interfaces failed, status=" << st.message();
        return false;
    }
    return true;
}

void update_process_thread_num(RuntimeMetrics* runtime_metrics) {
    int64_t pid = getpid();
    std::stringstream ss;
    ss << "/proc/" << pid << "/task/";

    int64_t count = 0;
    auto st = FileSystem::Default()->iterate_dir(ss.str(), [&](std::string_view /*name*/) {
        count++;
        return true;
    });
    if (!st.ok()) {
        LOG(WARNING) << "failed to count thread num from: " << ss.str();
        runtime_metrics->process_thread_num.set_value(0);
        return;
    }

    runtime_metrics->process_thread_num.set_value(count);
}

void update_process_fd_num(RuntimeMetrics* runtime_metrics) {
    int64_t pid = getpid();

    std::stringstream ss;
    ss << "/proc/" << pid << "/fd/";
    int64_t count = 0;
    auto st = FileSystem::Default()->iterate_dir(ss.str(), [&](std::string_view) {
        count++;
        return true;
    });
    if (!st.ok()) {
        LOG(WARNING) << "failed to count fd from: " << ss.str();
        runtime_metrics->process_fd_num_used.set_value(0);
        return;
    }
    runtime_metrics->process_fd_num_used.set_value(count);

    std::stringstream ss2;
    ss2 << "/proc/" << pid << "/limits";
    FILE* fp = fopen(ss2.str().c_str(), "r");
    if (fp == nullptr) {
        PLOG(WARNING) << "failed to open " << ss2.str();
        return;
    }

    int64_t values[2];
    size_t line_buf_size = 0;
    char* line_ptr = nullptr;
    while (getline(&line_ptr, &line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(line_ptr, "Max open files %" PRId64 " %" PRId64, &values[0], &values[1]);
        if (num == 2) {
            runtime_metrics->process_fd_num_limit_soft.set_value(values[0]);
            runtime_metrics->process_fd_num_limit_hard.set_value(values[1]);
            break;
        }
    }

    if (line_ptr != nullptr) {
        free(line_ptr);
    }

    if (ferror(fp) != 0) {
        PLOG(WARNING) << "getline failed";
    }
    fclose(fp);
}

void update_runtime_metrics(RuntimeMetrics* runtime_metrics) {
    update_process_thread_num(runtime_metrics);
    update_process_fd_num(runtime_metrics);
}

} // namespace

BackendMetricsInitOptions BackendMetricsInitializer::from_config(std::vector<std::string> storage_paths) {
    BackendMetricsInitOptions options;
    options.storage_paths = std::move(storage_paths);
    options.collect_hook_enabled = !config::enable_metric_calculator;
    options.init_system_metrics = config::enable_system_metrics;
    options.init_jvm_metrics = config::enable_jvm_metrics;
    options.bind_ipv6 = BackendOptions::is_bind_ipv6();
    return options;
}

void BackendMetricsInitializer::initialize(ProcessMetricsRegistry* process_metrics_registry,
                                           const BackendMetricsInitOptions& options) {
    DCHECK(process_metrics_registry != nullptr);

    auto* registry = process_metrics_registry->root_registry();
    registry->set_collect_hook_enabled(options.collect_hook_enabled);
    compression::install_compression_context_pool_metrics(registry);
    HttpMetrics::instance()->install(registry);
    ServiceMetrics::instance()->install(registry);
    auto* agent_metrics = AgentMetrics::instance();
    agent_metrics->install(registry);
    auto* runtime_metrics = RuntimeMetrics::instance();
    runtime_metrics->install(registry);
    StreamLoadMetrics::instance()->install(registry);
    registry->register_hook(kRuntimeMetricsHookName, [runtime_metrics] { update_runtime_metrics(runtime_metrics); });
    QueryScanMetrics::instance()->install(registry);
    FlatJsonMetrics::instance()->install(registry);

    std::set<std::string> disk_devices;
    std::vector<std::string> network_interfaces;
    if (options.init_system_metrics && !prepare_system_metrics_inputs(options, &disk_devices, &network_interfaces)) {
        return;
    }

    pipeline::PipelineExecutorMetrics::instance()->register_all_metrics(registry);
    agent_metrics->install_disk_path_metrics(registry, options.storage_paths);

    if (options.init_system_metrics) {
        SystemMetrics::instance()->install(registry, disk_devices, network_interfaces);
    }

    FileScanMetrics::instance()->install(registry);
    CatalogScanMetrics::instance()->install(registry);
    SpillMetrics::instance()->install(registry);
    DataCacheMetrics::instance()->install(registry);
    StorageMetrics::instance()->install(registry);

#ifndef __APPLE__
    if (options.init_jvm_metrics) {
        auto* jvm_metrics = JVMMetrics::instance();
        auto status = jvm_metrics->init();
        if (!status.ok()) {
            LOG(WARNING) << "init jvm metrics failed: " << status.to_string();
            return;
        }
        jvm_metrics->install(registry);
    }
#endif
}

} // namespace starrocks
