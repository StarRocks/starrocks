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

void ScanExecutorMetrics::register_all_metrics(MetricRegistry* registry, const std::string& prefix) {
#define REGISTER_SCAN_EXECUTOR_METRIC(name) registry->register_metric("pipe_" + prefix + "_" + #name, &name)

    REGISTER_SCAN_EXECUTOR_METRIC(execution_time);
    REGISTER_SCAN_EXECUTOR_METRIC(finished_tasks);
    REGISTER_SCAN_EXECUTOR_METRIC(running_tasks);
    REGISTER_SCAN_EXECUTOR_METRIC(pending_tasks);
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
#define REGISTER_DRIVER_QUEUE_METRIC(name) registry->register_metric("pipe_" #name, &name)

    REGISTER_DRIVER_QUEUE_METRIC(driver_queue_len);
}

void PollerMetrics::register_all_metrics(MetricRegistry* registry) {
#define REGISTER_POLLER_METRIC(name) registry->register_metric("pipe_" #name, &name)
    REGISTER_POLLER_METRIC(poller_block_queue_len);
}

void DriverExecutorMetrics::register_all_metrics(MetricRegistry* registry) {
#define REGISTER_DRIVER_EXECUTOR_METRIC(name) registry->register_metric("pipe_" #name, &name)

    REGISTER_DRIVER_EXECUTOR_METRIC(driver_schedule_count);
    REGISTER_DRIVER_EXECUTOR_METRIC(driver_execution_time);
    REGISTER_DRIVER_EXECUTOR_METRIC(exec_running_tasks);
    REGISTER_DRIVER_EXECUTOR_METRIC(exec_finished_tasks);
}

void PipelineExecutorMetrics::register_all_metrics(MetricRegistry* registry) {
    poller_metrics.register_all_metrics(registry);
    driver_executor_metrics.register_all_metrics(registry);
    scan_executor_metrics.register_all_metrics(registry, "scan");
    connector_scan_executor_metrics.register_all_metrics(registry, "connector_scan");
    exec_state_reporter_metrics.register_all_metrics();
}
} // namespace starrocks::pipeline