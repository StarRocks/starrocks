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

#include "util/metrics.h"

namespace starrocks {
class MetricRegistry;
namespace pipeline {

struct ScanExecutorMetrics {
    METRIC_DEFINE_INT_COUNTER(execution_time, MetricUnit::NANOSECONDS);
    METRIC_DEFINE_INT_COUNTER(finished_tasks, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_CORE_LOCAL_GAUGE(running_tasks, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_CORE_LOCAL_GAUGE(pending_tasks, MetricUnit::NOUNIT);

    void register_all_metrics(MetricRegistry* registry, const std::string& prefix);
};

struct DriverQueueMetrics {
    METRIC_DEFINE_INT_CORE_LOCAL_GAUGE(driver_queue_len, MetricUnit::NOUNIT);
    void register_all_metrics(MetricRegistry* registry);
};

struct PollerMetrics {
    METRIC_DEFINE_INT_CORE_LOCAL_GAUGE(poller_block_queue_len, MetricUnit::NOUNIT);
    void register_all_metrics(MetricRegistry* registry);
};

struct DriverExecutorMetrics {
    METRIC_DEFINE_INT_COUNTER(driver_schedule_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_COUNTER(driver_execution_time, MetricUnit::NANOSECONDS);
    METRIC_DEFINE_INT_CORE_LOCAL_GAUGE(exec_running_tasks, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_CORE_LOCAL_GAUGE(exec_finished_tasks, MetricUnit::NOUNIT);
    void register_all_metrics(MetricRegistry* registry);
};

struct PipelineExecutorMetrics {
public:
    PollerMetrics poller_metrics;
    DriverQueueMetrics driver_queue_metrics;
    DriverExecutorMetrics driver_executor_metrics;
    ScanExecutorMetrics scan_executor_metrics;
    ScanExecutorMetrics connector_scan_executor_metrics;

    PollerMetrics* get_poller_metrics() { return &poller_metrics; }
    DriverQueueMetrics* get_driver_queue_metrics() { return &driver_queue_metrics; }
    DriverExecutorMetrics* get_driver_executor_metrics() { return &driver_executor_metrics; }

    ScanExecutorMetrics* get_scan_executor_metrics() { return &scan_executor_metrics; }

    ScanExecutorMetrics* get_connector_scan_executor_metrics() { return &connector_scan_executor_metrics; }

    void register_all_metrics(MetricRegistry* registry);
};
} // namespace pipeline
} // namespace starrocks