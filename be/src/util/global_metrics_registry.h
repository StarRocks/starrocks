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

#pragma once

#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include "common/metrics/process_metrics_registry.h"

namespace starrocks {

class StarRocksMetrics;
class SystemMetrics;
class FileScanMetrics;
class CatalogScanMetrics;
class SpillMetrics;

namespace pipeline {
struct PipelineExecutorMetrics;
} // namespace pipeline

class GlobalMetricsRegistry {
public:
    static GlobalMetricsRegistry* instance();

    // not thread-safe, call before calling metrics
    void initialize(const std::vector<std::string>& paths = std::vector<std::string>(),
                    bool init_system_metrics = false, bool init_jvm_metrics = false,
                    const std::set<std::string>& disk_devices = std::set<std::string>(),
                    const std::vector<std::string>& network_interfaces = std::vector<std::string>());

    ProcessMetricsRegistry* process_metrics_registry() { return &_process_metrics_registry; }
    MetricRegistry* metrics() { return _process_metrics_registry.root_registry(); }
    SystemMetrics* system_metrics();
    TableMetricsManager* table_metrics_mgr() { return _process_metrics_registry.table_metrics_mgr(); }
    TableMetricsPtr table_metrics(uint64_t table_id) { return _process_metrics_registry.table_metrics(table_id); }
    FileScanMetrics* file_scan_metrics();
    CatalogScanMetrics* catalog_scan_metrics();
    SpillMetrics* spill_metrics();
    pipeline::PipelineExecutorMetrics* pipeline_executor_metrics();

private:
    explicit GlobalMetricsRegistry(StarRocksMetrics* fast_metrics);

    void _update();
    void _update_process_thread_num();
    void _update_process_fd_num();

private:
    static const std::string _s_registry_name;
    static const std::string _s_hook_name;

    StarRocksMetrics* _fast_metrics;
    ProcessMetricsRegistry _process_metrics_registry;
};

} // namespace starrocks
