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
#include "runtime/runtime_metrics.h"
#include "runtime/starrocks_metrics.h"
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

void install_starrocks_metrics(MetricRegistry* registry, StarRocksMetrics* fast_metrics) {
    compression::install_compression_context_pool_metrics(registry);

#define REGISTER_STARROCKS_METRIC(name) registry->register_metric(#name, &(fast_metrics->name))

    REGISTER_STARROCKS_METRIC(http_requests_total);
    REGISTER_STARROCKS_METRIC(http_request_send_bytes);

    REGISTER_STARROCKS_METRIC(lake_txn_log_collect_legacy_total);
    REGISTER_STARROCKS_METRIC(lake_txn_log_collect_per_partition_total);
    REGISTER_STARROCKS_METRIC(lake_txn_log_collect_orphan_partition_total);

    REGISTER_STARROCKS_METRIC(segment_file_not_found_total);

    REGISTER_STARROCKS_METRIC(update_primary_index_num);
    REGISTER_STARROCKS_METRIC(update_primary_index_bytes_total);
    REGISTER_STARROCKS_METRIC(update_del_vector_num);
    REGISTER_STARROCKS_METRIC(update_del_vector_dels_num);
    REGISTER_STARROCKS_METRIC(update_del_vector_bytes_total);
    REGISTER_STARROCKS_METRIC(update_del_vector_deletes_total);
    REGISTER_STARROCKS_METRIC(update_del_vector_deletes_new);
    REGISTER_STARROCKS_METRIC(column_partial_update_apply_total);
    REGISTER_STARROCKS_METRIC(column_partial_update_apply_duration_us);
    REGISTER_STARROCKS_METRIC(delta_column_group_get_total);
    REGISTER_STARROCKS_METRIC(delta_column_group_get_hit_cache);
    REGISTER_STARROCKS_METRIC(delta_column_group_get_non_pk_total);
    REGISTER_STARROCKS_METRIC(delta_column_group_get_non_pk_hit_cache);
    REGISTER_STARROCKS_METRIC(primary_key_table_error_state_total);
    REGISTER_STARROCKS_METRIC(primary_key_wait_apply_done_duration_ms);
    REGISTER_STARROCKS_METRIC(primary_key_wait_apply_done_total);
    REGISTER_STARROCKS_METRIC(pk_index_sst_read_error_total);
    REGISTER_STARROCKS_METRIC(pk_index_sst_write_error_total);

    REGISTER_STARROCKS_METRIC(staros_shard_info_fallback_total);
    REGISTER_STARROCKS_METRIC(staros_shard_info_fallback_failed_total);

    registry->register_metric("meta_request_total", MetricLabels().add("type", "write"),
                              &(fast_metrics->meta_write_request_total));
    registry->register_metric("meta_request_total", MetricLabels().add("type", "read"),
                              &(fast_metrics->meta_read_request_total));
    registry->register_metric("meta_request_duration", MetricLabels().add("type", "write"),
                              &(fast_metrics->meta_write_request_duration_us));
    registry->register_metric("meta_request_duration", MetricLabels().add("type", "read"),
                              &(fast_metrics->meta_read_request_duration_us));

    registry->register_metric("segment_read", MetricLabels().add("type", "segment_total_read_times"),
                              &(fast_metrics->segment_read_total));
    registry->register_metric("segment_read", MetricLabels().add("type", "segment_total_row_num"),
                              &(fast_metrics->segment_row_total));
    registry->register_metric("segment_read", MetricLabels().add("type", "segment_rows_by_short_key"),
                              &(fast_metrics->segment_rows_by_short_key));
    registry->register_metric("segment_read", MetricLabels().add("type", "segment_rows_read_by_zone_map"),
                              &(fast_metrics->segment_rows_read_by_zone_map));

    registry->register_metric("txn_request", MetricLabels().add("type", "begin"),
                              &(fast_metrics->txn_begin_request_total));
    registry->register_metric("txn_request", MetricLabels().add("type", "commit"),
                              &(fast_metrics->txn_commit_request_total));
    registry->register_metric("txn_request", MetricLabels().add("type", "rollback"),
                              &(fast_metrics->txn_rollback_request_total));
    registry->register_metric("txn_request", MetricLabels().add("type", "exec"), &(fast_metrics->txn_exec_plan_total));

    registry->register_metric("stream_load", MetricLabels().add("type", "receive_bytes"),
                              &(fast_metrics->stream_receive_bytes_total));
    registry->register_metric("stream_load", MetricLabels().add("type", "load_rows"),
                              &(fast_metrics->stream_load_rows_total));
    registry->register_metric("load_rows", &(fast_metrics->load_rows_total));
    registry->register_metric("load_bytes", &(fast_metrics->load_bytes_total));

    REGISTER_STARROCKS_METRIC(readable_blocks_total);
    REGISTER_STARROCKS_METRIC(writable_blocks_total);
    REGISTER_STARROCKS_METRIC(blocks_created_total);
    REGISTER_STARROCKS_METRIC(blocks_deleted_total);
    REGISTER_STARROCKS_METRIC(bytes_read_total);
    REGISTER_STARROCKS_METRIC(bytes_written_total);
    REGISTER_STARROCKS_METRIC(disk_sync_total);
    REGISTER_STARROCKS_METRIC(blocks_open_reading);
    REGISTER_STARROCKS_METRIC(blocks_open_writing);

    REGISTER_STARROCKS_METRIC(short_circuit_request_total);
    REGISTER_STARROCKS_METRIC(short_circuit_request_duration_us);

#undef REGISTER_STARROCKS_METRIC
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
                                           StarRocksMetrics* fast_metrics, const BackendMetricsInitOptions& options) {
    DCHECK(process_metrics_registry != nullptr);
    DCHECK(fast_metrics != nullptr);

    auto* registry = process_metrics_registry->root_registry();
    registry->set_collect_hook_enabled(options.collect_hook_enabled);
    install_starrocks_metrics(registry, fast_metrics);
    auto* agent_metrics = AgentMetrics::instance();
    agent_metrics->install(registry);
    auto* runtime_metrics = RuntimeMetrics::instance();
    runtime_metrics->install(registry);
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
