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
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "base/metrics.h"
#include "exec/pipeline/pipeline_metrics.h"
#ifndef __APPLE__
#include "util/jvm_metrics.h"
#endif
#include "util/metrics/file_scan_metrics.h"
#include "util/system_metrics.h"
#include "util/table_metrics.h"

namespace starrocks {

class StarRocksMetrics;

class GlobalMetricsRegistry {
public:
    static GlobalMetricsRegistry* instance();

    // not thread-safe, call before calling metrics
    void initialize(const std::vector<std::string>& paths = std::vector<std::string>(),
                    bool init_system_metrics = false, bool init_jvm_metrics = false,
                    const std::set<std::string>& disk_devices = std::set<std::string>(),
                    const std::vector<std::string>& network_interfaces = std::vector<std::string>());

    MetricRegistry* metrics() { return &_metrics; }
    SystemMetrics* system_metrics() { return &_system_metrics; }
    TableMetricsManager* table_metrics_mgr() { return &_table_metrics_mgr; }
    TableMetricsPtr table_metrics(uint64_t table_id) { return _table_metrics_mgr.get_table_metrics(table_id); }
    FileScanMetrics* file_scan_metrics() { return _file_scan_metrics.get(); }
    pipeline::PipelineExecutorMetrics* pipeline_executor_metrics() { return &_pipeline_executor_metrics; }

private:
    explicit GlobalMetricsRegistry(StarRocksMetrics* fast_metrics);

    void _update();
    void _update_process_thread_num();
    void _update_process_fd_num();

private:
    static const std::string _s_registry_name;
    static const std::string _s_hook_name;

    StarRocksMetrics* _fast_metrics;
    MetricRegistry _metrics;
    SystemMetrics _system_metrics;
#ifndef __APPLE__
    JVMMetrics _jvm_metrics;
#endif
    TableMetricsManager _table_metrics_mgr;
    std::unique_ptr<FileScanMetrics> _file_scan_metrics;
    pipeline::PipelineExecutorMetrics _pipeline_executor_metrics;
};

} // namespace starrocks
