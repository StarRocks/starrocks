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

#include "agent/agent_metrics.h"

#include "common/thread/threadpool.h"
#include "gutil/macros.h"

namespace starrocks {

void AgentIntGaugeMetricsMap::set_metric(const std::string& key, int64_t val) {
    auto metric = _metrics.find(key);
    if (metric != _metrics.end()) {
        metric->second->set_value(val);
    }
}

IntGauge* AgentIntGaugeMetricsMap::add_metric(const std::string& key, MetricUnit unit) {
    auto [it, inserted] = _metrics.emplace(key, nullptr);
    if (inserted) {
        it->second = std::make_unique<IntGauge>(unit);
    }
    return it->second.get();
}

AgentMetrics* AgentMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new AgentMetrics();
    return instance;
}

void AgentMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;

#define REGISTER_ENGINE_REQUEST_METRIC(type, status, metric)                                                     \
    registry->register_metric("engine_requests_total", MetricLabels().add("type", #type).add("status", #status), \
                              &metric)

    REGISTER_ENGINE_REQUEST_METRIC(report_all_tablets, failed, report_all_tablets_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(report_tablet, failed, report_tablet_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(report_disk, total, report_disk_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(report_disk, failed, report_disk_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(report_task, total, report_task_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(report_task, failed, report_task_requests_failed);

    REGISTER_ENGINE_REQUEST_METRIC(schema_change, total, schema_change_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(schema_change, failed, schema_change_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(clone, total, clone_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(clone, failed, clone_requests_failed);

    REGISTER_ENGINE_REQUEST_METRIC(finish_task, total, finish_task_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(finish_task, failed, finish_task_requests_failed);

    REGISTER_ENGINE_REQUEST_METRIC(publish, total, publish_task_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(publish, failed, publish_task_failed_total);

#undef REGISTER_ENGINE_REQUEST_METRIC

    registry->register_metric("clone_task_copy_bytes", MetricLabels().add("type", "INTER_NODE"),
                              &clone_task_inter_node_copy_bytes);
    registry->register_metric("clone_task_copy_bytes", MetricLabels().add("type", "INTRA_NODE"),
                              &clone_task_intra_node_copy_bytes);
    registry->register_metric("clone_task_copy_duration_ms", MetricLabels().add("type", "INTER_NODE"),
                              &clone_task_inter_node_copy_duration_ms);
    registry->register_metric("clone_task_copy_duration_ms", MetricLabels().add("type", "INTRA_NODE"),
                              &clone_task_intra_node_copy_duration_ms);

    for (const auto& pending : _pending_thread_pool_metrics) {
        _register_thread_pool_metrics(pending.name, pending.metric_group, pending.threadpool);
    }
    _pending_thread_pool_metrics.clear();
}

void AgentMetrics::install_disk_path_metrics(MetricRegistry* registry, const std::vector<std::string>& paths) {
    DCHECK_EQ(_registry, registry);
    for (auto& path : paths) {
        IntGauge* gauge = _disks_total_capacity.add_metric(path, MetricUnit::BYTES);
        registry->register_metric("disks_total_capacity", MetricLabels().add("path", path), gauge);
        gauge = _disks_avail_capacity.add_metric(path, MetricUnit::BYTES);
        registry->register_metric("disks_avail_capacity", MetricLabels().add("path", path), gauge);
        gauge = _disks_data_used_capacity.add_metric(path, MetricUnit::BYTES);
        registry->register_metric("disks_data_used_capacity", MetricLabels().add("path", path), gauge);
        gauge = _disks_state.add_metric(path, MetricUnit::NOUNIT);
        registry->register_metric("disks_state", MetricLabels().add("path", path), gauge);
    }
}

void AgentMetrics::set_disk_metrics(const std::string& path, int64_t total_capacity, int64_t available_capacity,
                                    int64_t data_used_capacity, int64_t state) {
    _disks_total_capacity.set_metric(path, total_capacity);
    _disks_avail_capacity.set_metric(path, available_capacity);
    _disks_data_used_capacity.set_metric(path, data_used_capacity);
    _disks_state.set_metric(path, state);
}

void AgentMetrics::register_thread_pool_metrics(const std::string& name, ThreadPoolMetricGroup* metric_group,
                                                ThreadPool* threadpool) {
    DCHECK(metric_group != nullptr);
    DCHECK(threadpool != nullptr);
    if (_registry == nullptr) {
        _pending_thread_pool_metrics.emplace_back(PendingThreadPoolMetrics{name, metric_group, threadpool});
        return;
    }
    _register_thread_pool_metrics(name, metric_group, threadpool);
}

void AgentMetrics::_register_thread_pool_metrics(const std::string& name, ThreadPoolMetricGroup* metric_group,
                                                 ThreadPool* threadpool) {
    DCHECK(_registry != nullptr);
    DCHECK(metric_group != nullptr);
    DCHECK(threadpool != nullptr);

    metric_group->register_metrics(_registry, name, threadpool);
}

} // namespace starrocks
