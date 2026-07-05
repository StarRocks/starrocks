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

#include "common/metrics/thread_pool_metrics.h"

#include "common/thread/threadpool.h"
#include "gutil/macros.h"

namespace starrocks {

void register_thread_pool_metric_group(MetricRegistry* registry, const std::string& name, ThreadPool* threadpool,
                                       const ThreadPoolMetricGroup& metrics) {
    DCHECK(registry != nullptr);
    DCHECK(threadpool != nullptr);
    DCHECK(metrics.threadpool_size != nullptr);
    DCHECK(metrics.executed_tasks_total != nullptr);
    DCHECK(metrics.pending_time_ns_total != nullptr);
    DCHECK(metrics.execute_time_ns_total != nullptr);
    DCHECK(metrics.queue_count != nullptr);
    DCHECK(metrics.running_threads != nullptr);
    DCHECK(metrics.active_threads != nullptr);

    const auto threadpool_size_name = name + "_threadpool_size";
    registry->register_metric(threadpool_size_name, metrics.threadpool_size);
    registry->register_hook(threadpool_size_name, [threadpool_size = metrics.threadpool_size, threadpool] {
        threadpool_size->set_value(threadpool->max_threads());
    });

    const auto executed_tasks_total_name = name + "_executed_tasks_total";
    registry->register_metric(executed_tasks_total_name, metrics.executed_tasks_total);
    registry->register_hook(executed_tasks_total_name,
                            [executed_tasks_total = metrics.executed_tasks_total, threadpool] {
                                executed_tasks_total->set_value(threadpool->total_executed_tasks());
                            });

    const auto pending_time_ns_total_name = name + "_pending_time_ns_total";
    registry->register_metric(pending_time_ns_total_name, metrics.pending_time_ns_total);
    registry->register_hook(pending_time_ns_total_name,
                            [pending_time_ns_total = metrics.pending_time_ns_total, threadpool] {
                                pending_time_ns_total->set_value(threadpool->total_pending_time_ns());
                            });

    const auto execute_time_ns_total_name = name + "_execute_time_ns_total";
    registry->register_metric(execute_time_ns_total_name, metrics.execute_time_ns_total);
    registry->register_hook(execute_time_ns_total_name,
                            [execute_time_ns_total = metrics.execute_time_ns_total, threadpool] {
                                execute_time_ns_total->set_value(threadpool->total_execute_time_ns());
                            });

    const auto queue_count_name = name + "_queue_count";
    registry->register_metric(queue_count_name, metrics.queue_count);
    registry->register_hook(queue_count_name, [queue_count = metrics.queue_count, threadpool] {
        queue_count->set_value(threadpool->num_queued_tasks());
    });

    const auto running_threads_name = name + "_running_threads";
    registry->register_metric(running_threads_name, metrics.running_threads);
    registry->register_hook(running_threads_name, [running_threads = metrics.running_threads, threadpool] {
        running_threads->set_value(threadpool->num_threads());
    });

    const auto active_threads_name = name + "_active_threads";
    registry->register_metric(active_threads_name, metrics.active_threads);
    registry->register_hook(active_threads_name, [active_threads = metrics.active_threads, threadpool] {
        active_threads->set_value(threadpool->active_threads());
    });
}

void ThreadPoolMetricsRegistrar::register_metrics(MetricRegistry* registry, const std::string& name,
                                                  ThreadPool* threadpool, const ThreadPoolMetricGroup& metrics) {
    DCHECK(threadpool != nullptr);
    if (registry == nullptr) {
        _pending_metrics.emplace_back(PendingThreadPoolMetrics{name, threadpool, metrics});
        return;
    }
    register_thread_pool_metric_group(registry, name, threadpool, metrics);
}

void ThreadPoolMetricsRegistrar::install(MetricRegistry* registry) {
    DCHECK(registry != nullptr);
    for (const auto& pending : _pending_metrics) {
        register_thread_pool_metric_group(registry, pending.name, pending.threadpool, pending.metrics);
    }
    _pending_metrics.clear();
}

} // namespace starrocks
