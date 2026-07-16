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

class ThreadPoolMetricGroup {
public:
    ~ThreadPoolMetricGroup();

    void register_metrics(MetricRegistry* registry, const std::string& name, ThreadPool* threadpool);
    void unregister_metrics();

    METRIC_DEFINE_UINT_GAUGE(threadpool_size, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(executed_tasks_total, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(pending_time_ns_total, MetricUnit::NANOSECONDS);
    METRIC_DEFINE_UINT_GAUGE(execute_time_ns_total, MetricUnit::NANOSECONDS);
    METRIC_DEFINE_UINT_GAUGE(queue_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(running_threads, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(active_threads, MetricUnit::NOUNIT);

private:
    MetricRegistry* _registry = nullptr;
    std::vector<std::string> _hook_names;
};

} // namespace starrocks

#define METRICS_DEFINE_THREAD_POOL(threadpool_name) starrocks::ThreadPoolMetricGroup threadpool_name
