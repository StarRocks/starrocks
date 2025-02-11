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

#include "util/metrics.h"

namespace starrocks::pipeline {

void ScanExecutorMetrics::register_all_metrics(MetricRegistry* registry, const std::string& prefix) {
#define REGISTER_SCAN_EXECUTOR_METRIC(name) registry->register_metric("pipe_" + prefix + "_" + #name, &name)

    REGISTER_SCAN_EXECUTOR_METRIC(execution_time);
    REGISTER_SCAN_EXECUTOR_METRIC(finished_tasks);
    REGISTER_SCAN_EXECUTOR_METRIC(running_tasks);
    REGISTER_SCAN_EXECUTOR_METRIC(pending_tasks);
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
}
} // namespace starrocks::pipeline