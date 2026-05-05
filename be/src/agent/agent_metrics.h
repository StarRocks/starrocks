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
#include <string>
#include <unordered_map>
#include <vector>

#include "base/metrics.h"

namespace starrocks {

class ThreadPool;

class AgentIntGaugeMetricsMap {
public:
    void set_metric(const std::string& key, int64_t val);
    IntGauge* add_metric(const std::string& key, MetricUnit unit);

private:
    std::unordered_map<std::string, std::unique_ptr<IntGauge>> _metrics;
};

class AgentMetrics {
public:
    AgentMetrics() = default;
    explicit AgentMetrics(MetricRegistry* registry) { install(registry); }
    ~AgentMetrics() = default;

    static AgentMetrics* instance();

    void install(MetricRegistry* registry);
    void install_disk_path_metrics(MetricRegistry* registry, const std::vector<std::string>& paths);
    void set_disk_metrics(const std::string& path, int64_t total_capacity, int64_t available_capacity,
                          int64_t data_used_capacity, int64_t state);
    void register_thread_pool_metrics(const std::string& name, ThreadPool* threadpool);

    METRIC_DEFINE_INT_COUNTER(create_tablet_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(create_tablet_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(drop_tablet_requests_total, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(report_all_tablets_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_all_tablets_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_tablet_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_tablet_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_disk_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_disk_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_task_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_task_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_workgroup_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_workgroup_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_resource_usage_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_resource_usage_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_datacache_metrics_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_datacache_metrics_requests_failed, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(schema_change_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(schema_change_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(create_rollup_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(create_rollup_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(clone_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(clone_requests_failed, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(finish_task_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(finish_task_requests_failed, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(clone_task_inter_node_copy_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(clone_task_intra_node_copy_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(clone_task_inter_node_copy_duration_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_COUNTER(clone_task_intra_node_copy_duration_ms, MetricUnit::MILLISECONDS);

    METRIC_DEFINE_INT_COUNTER(publish_task_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(publish_task_failed_total, MetricUnit::REQUESTS);

    METRICS_DEFINE_THREAD_POOL(publish_version);
    METRICS_DEFINE_THREAD_POOL(drop);
    METRICS_DEFINE_THREAD_POOL(create_tablet);
    METRICS_DEFINE_THREAD_POOL(alter_tablet);
    METRICS_DEFINE_THREAD_POOL(clear_transaction);
    METRICS_DEFINE_THREAD_POOL(storage_medium_migrate);
    METRICS_DEFINE_THREAD_POOL(check_consistency);
    METRICS_DEFINE_THREAD_POOL(manual_compaction);
    METRICS_DEFINE_THREAD_POOL(compaction_control);
    METRICS_DEFINE_THREAD_POOL(update_schema);
    METRICS_DEFINE_THREAD_POOL(upload);
    METRICS_DEFINE_THREAD_POOL(download);
    METRICS_DEFINE_THREAD_POOL(make_snapshot);
    METRICS_DEFINE_THREAD_POOL(release_snapshot);
    METRICS_DEFINE_THREAD_POOL(move_dir);
    METRICS_DEFINE_THREAD_POOL(update_tablet_meta_info);
    METRICS_DEFINE_THREAD_POOL(drop_auto_increment_map_dir);
    METRICS_DEFINE_THREAD_POOL(clone);
    METRICS_DEFINE_THREAD_POOL(remote_snapshot);
    METRICS_DEFINE_THREAD_POOL(replicate_snapshot);

private:
    struct PendingThreadPoolMetrics {
        std::string name;
        ThreadPool* threadpool;
    };

    void _register_thread_pool_metrics(const std::string& name, ThreadPool* threadpool);

    MetricRegistry* _registry = nullptr;
    std::vector<PendingThreadPoolMetrics> _pending_thread_pool_metrics;

    AgentIntGaugeMetricsMap _disks_total_capacity;
    AgentIntGaugeMetricsMap _disks_avail_capacity;
    AgentIntGaugeMetricsMap _disks_data_used_capacity;
    AgentIntGaugeMetricsMap _disks_state;
};

} // namespace starrocks
