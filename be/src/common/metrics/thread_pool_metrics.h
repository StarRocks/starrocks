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

#include <string>
#include <vector>

#include "base/metrics.h"

namespace starrocks {

class ThreadPool;

struct ThreadPoolMetricGroup {
    UIntGauge* threadpool_size = nullptr;
    UIntGauge* executed_tasks_total = nullptr;
    UIntGauge* pending_time_ns_total = nullptr;
    UIntGauge* execute_time_ns_total = nullptr;
    UIntGauge* queue_count = nullptr;
    UIntGauge* running_threads = nullptr;
    UIntGauge* active_threads = nullptr;
};

void register_thread_pool_metric_group(MetricRegistry* registry, const std::string& name, ThreadPool* threadpool,
                                       const ThreadPoolMetricGroup& metrics);

class ThreadPoolMetricsRegistrar {
public:
    void register_metrics(MetricRegistry* registry, const std::string& name, ThreadPool* threadpool,
                          const ThreadPoolMetricGroup& metrics);
    void install(MetricRegistry* registry);

private:
    struct PendingThreadPoolMetrics {
        std::string name;
        ThreadPool* threadpool;
        ThreadPoolMetricGroup metrics;
    };

    std::vector<PendingThreadPoolMetrics> _pending_metrics;
};

} // namespace starrocks

#define STARROCKS_THREAD_POOL_METRIC_GROUP(metrics_owner, threadpool_name)                                             \
    ::starrocks::ThreadPoolMetricGroup {                                                                               \
        &(metrics_owner)->threadpool_name##_threadpool_size, &(metrics_owner)->threadpool_name##_executed_tasks_total, \
                &(metrics_owner)->threadpool_name##_pending_time_ns_total,                                             \
                &(metrics_owner)->threadpool_name##_execute_time_ns_total,                                             \
                &(metrics_owner)->threadpool_name##_queue_count, &(metrics_owner)->threadpool_name##_running_threads,  \
                &(metrics_owner)->threadpool_name##_active_threads                                                     \
    }
