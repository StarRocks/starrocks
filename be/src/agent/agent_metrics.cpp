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

namespace {

void register_thread_pool_metric_group(MetricRegistry* registry, const std::string& name, ThreadPool* threadpool,
                                       UIntGauge* threadpool_size, UIntGauge* executed_tasks_total,
                                       UIntGauge* pending_time_ns_total, UIntGauge* execute_time_ns_total,
                                       UIntGauge* queue_count, UIntGauge* running_threads, UIntGauge* active_threads) {
    DCHECK(registry != nullptr);
    DCHECK(threadpool != nullptr);

    const auto threadpool_size_name = name + "_threadpool_size";
    registry->register_metric(threadpool_size_name, threadpool_size);
    registry->register_hook(threadpool_size_name,
                            [threadpool_size, threadpool] { threadpool_size->set_value(threadpool->max_threads()); });

    const auto executed_tasks_total_name = name + "_executed_tasks_total";
    registry->register_metric(executed_tasks_total_name, executed_tasks_total);
    registry->register_hook(executed_tasks_total_name, [executed_tasks_total, threadpool] {
        executed_tasks_total->set_value(threadpool->total_executed_tasks());
    });

    const auto pending_time_ns_total_name = name + "_pending_time_ns_total";
    registry->register_metric(pending_time_ns_total_name, pending_time_ns_total);
    registry->register_hook(pending_time_ns_total_name, [pending_time_ns_total, threadpool] {
        pending_time_ns_total->set_value(threadpool->total_pending_time_ns());
    });

    const auto execute_time_ns_total_name = name + "_execute_time_ns_total";
    registry->register_metric(execute_time_ns_total_name, execute_time_ns_total);
    registry->register_hook(execute_time_ns_total_name, [execute_time_ns_total, threadpool] {
        execute_time_ns_total->set_value(threadpool->total_execute_time_ns());
    });

    const auto queue_count_name = name + "_queue_count";
    registry->register_metric(queue_count_name, queue_count);
    registry->register_hook(queue_count_name,
                            [queue_count, threadpool] { queue_count->set_value(threadpool->num_queued_tasks()); });

    const auto running_threads_name = name + "_running_threads";
    registry->register_metric(running_threads_name, running_threads);
    registry->register_hook(running_threads_name,
                            [running_threads, threadpool] { running_threads->set_value(threadpool->num_threads()); });

    const auto active_threads_name = name + "_active_threads";
    registry->register_metric(active_threads_name, active_threads);
    registry->register_hook(active_threads_name,
                            [active_threads, threadpool] { active_threads->set_value(threadpool->active_threads()); });
}

} // namespace

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
        _register_thread_pool_metrics(pending.name, pending.threadpool);
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

void AgentMetrics::register_thread_pool_metrics(const std::string& name, ThreadPool* threadpool) {
    DCHECK(threadpool != nullptr);
    if (_registry == nullptr) {
        _pending_thread_pool_metrics.emplace_back(PendingThreadPoolMetrics{name, threadpool});
        return;
    }
    _register_thread_pool_metrics(name, threadpool);
}

void AgentMetrics::_register_thread_pool_metrics(const std::string& name, ThreadPool* threadpool) {
    DCHECK(_registry != nullptr);
    DCHECK(threadpool != nullptr);

#define REGISTER_AGENT_THREAD_POOL_METRICS(threadpool_name)                                                         \
    if (name == #threadpool_name) {                                                                                 \
        register_thread_pool_metric_group(_registry, name, threadpool, &threadpool_name##_threadpool_size,          \
                                          &threadpool_name##_executed_tasks_total,                                  \
                                          &threadpool_name##_pending_time_ns_total,                                 \
                                          &threadpool_name##_execute_time_ns_total, &threadpool_name##_queue_count, \
                                          &threadpool_name##_running_threads, &threadpool_name##_active_threads);   \
        return;                                                                                                     \
    }

    REGISTER_AGENT_THREAD_POOL_METRICS(publish_version);
    REGISTER_AGENT_THREAD_POOL_METRICS(drop);
    REGISTER_AGENT_THREAD_POOL_METRICS(create_tablet);
    REGISTER_AGENT_THREAD_POOL_METRICS(alter_tablet);
    REGISTER_AGENT_THREAD_POOL_METRICS(clear_transaction);
    REGISTER_AGENT_THREAD_POOL_METRICS(storage_medium_migrate);
    REGISTER_AGENT_THREAD_POOL_METRICS(check_consistency);
    REGISTER_AGENT_THREAD_POOL_METRICS(manual_compaction);
    REGISTER_AGENT_THREAD_POOL_METRICS(compaction_control);
    REGISTER_AGENT_THREAD_POOL_METRICS(update_schema);
    REGISTER_AGENT_THREAD_POOL_METRICS(upload);
    REGISTER_AGENT_THREAD_POOL_METRICS(download);
    REGISTER_AGENT_THREAD_POOL_METRICS(make_snapshot);
    REGISTER_AGENT_THREAD_POOL_METRICS(release_snapshot);
    REGISTER_AGENT_THREAD_POOL_METRICS(move_dir);
    REGISTER_AGENT_THREAD_POOL_METRICS(update_tablet_meta_info);
    REGISTER_AGENT_THREAD_POOL_METRICS(drop_auto_increment_map_dir);
    REGISTER_AGENT_THREAD_POOL_METRICS(clone);
    REGISTER_AGENT_THREAD_POOL_METRICS(remote_snapshot);
    REGISTER_AGENT_THREAD_POOL_METRICS(replicate_snapshot);

#undef REGISTER_AGENT_THREAD_POOL_METRICS

    DCHECK(false) << "unknown agent thread pool metric group: " << name;
}

} // namespace starrocks
