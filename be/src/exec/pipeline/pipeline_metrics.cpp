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

#include "exec/pipeline/pipeline_metrics.h"

#include <numeric>

#include "util/metrics.h"
#include "util/starrocks_metrics.h"
#include "util/threadpool.h"

namespace starrocks::pipeline {

// ------------------------------------------------------------------------------------
// QueryTypeTimeMetric.
// ------------------------------------------------------------------------------------

namespace {
constexpr const char* kWorkloadLabelName = "workload_type";

const char* workload_label_value(TQueryType::type query_type) {
    switch (query_type) {
    case TQueryType::SELECT:
        return "query";
    case TQueryType::LOAD:
        return "load";
    case TQueryType::EXTERNAL:
    default:
        return "unknown";
    }
}
} // namespace

void QueryTypeTimeMetric::register_metrics(MetricRegistry* registry, const std::string& metric_name) {
    MetricLabels query_labels;
    query_labels.add(kWorkloadLabelName, workload_label_value(TQueryType::SELECT));
    registry->register_metric(metric_name, query_labels, &_query_counter);

    MetricLabels load_labels;
    load_labels.add(kWorkloadLabelName, workload_label_value(TQueryType::LOAD));
    registry->register_metric(metric_name, load_labels, &_load_counter);

    MetricLabels unknown_labels;
    unknown_labels.add(kWorkloadLabelName, workload_label_value(TQueryType::EXTERNAL));
    registry->register_metric(metric_name, unknown_labels, &_unknown_counter);
}

void QueryTypeTimeMetric::increment(TQueryType::type query_type, int64_t delta) {
    counter(query_type)->increment(delta);
}

IntCounter* QueryTypeTimeMetric::counter(TQueryType::type query_type) {
    switch (query_type) {
    case TQueryType::SELECT:
        return &_query_counter;
    case TQueryType::LOAD:
        return &_load_counter;
    case TQueryType::EXTERNAL:
    default:
        return &_unknown_counter;
    }
}

// ------------------------------------------------------------------------------------
// Metrics.
// ------------------------------------------------------------------------------------

void ScanExecutorMetrics::register_all_metrics(MetricRegistry* registry, const std::string& prefix) {
    const std::string base_name = "pipe_" + prefix + "_";
    execution_time.register_metrics(registry, base_name + "execution_time");
    registry->register_metric(base_name + "finished_tasks", &finished_tasks);
    registry->register_metric(base_name + "running_tasks", &running_tasks);
    registry->register_metric(base_name + "pending_tasks", &pending_tasks);
}

#define ACCUMULATED(array, field)                                                                                     \
    [this]() {                                                                                                        \
        std::lock_guard guard(_mutex);                                                                                \
        return std::accumulate(array.begin(), array.end(), 0, [](int64_t a, const auto& b) { return a + b->field; }); \
    }

#define REGISTER_POOLS_METRICS(name, threadpool)                                                                     \
    REGISTER_GAUGE_STARROCKS_METRIC(name##_threadpool_size, ACCUMULATED(threadpool, max_threads()))                  \
    REGISTER_GAUGE_STARROCKS_METRIC(name##_executed_tasks_total, ACCUMULATED(threadpool, total_executed_tasks()));   \
    REGISTER_GAUGE_STARROCKS_METRIC(name##_pending_time_ns_total, ACCUMULATED(threadpool, total_pending_time_ns())); \
    REGISTER_GAUGE_STARROCKS_METRIC(name##_execute_time_ns_total, ACCUMULATED(threadpool, total_execute_time_ns())); \
    REGISTER_GAUGE_STARROCKS_METRIC(name##_queue_count, ACCUMULATED(threadpool, num_queued_tasks()));                \
    REGISTER_GAUGE_STARROCKS_METRIC(name##_running_threads, ACCUMULATED(threadpool, num_threads()));                 \
    REGISTER_GAUGE_STARROCKS_METRIC(name##_active_threads, ACCUMULATED(threadpool, active_threads()));

void ExecStateReporterMetrics::register_all_metrics() {
    REGISTER_POOLS_METRICS(exec_state_report, _reporter_thr_pools);
    REGISTER_POOLS_METRICS(priority_exec_state_report, _priority_reporter_thr_pools);
}

void DriverQueueMetrics::register_all_metrics(MetricRegistry* registry) {
    registry->register_metric("pipe_driver_queue_len", &driver_queue_len);
}

void PollerMetrics::register_all_metrics(MetricRegistry* registry) {
    registry->register_metric("pipe_poller_block_queue_len", &poller_block_queue_len);
}

void DriverExecutorMetrics::register_all_metrics(MetricRegistry* registry) {
    registry->register_metric("pipe_driver_schedule_count", &driver_schedule_count);
    driver_execution_time.register_metrics(registry, "pipe_driver_execution_time");
    registry->register_metric("pipe_exec_running_tasks", &exec_running_tasks);
    registry->register_metric("pipe_exec_finished_tasks", &exec_finished_tasks);
}

void PipelineExecutorMetrics::register_all_metrics(MetricRegistry* registry) {
    poller_metrics.register_all_metrics(registry);
    driver_executor_metrics.register_all_metrics(registry);
    scan_executor_metrics.register_all_metrics(registry, "scan");
    connector_scan_executor_metrics.register_all_metrics(registry, "connector_scan");
    exec_state_reporter_metrics.register_all_metrics();
}

} // namespace starrocks::pipeline