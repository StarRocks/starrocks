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

namespace starrocks {

ThreadPoolMetricGroup::~ThreadPoolMetricGroup() {
    if (_registry != nullptr && _hook_registered) {
        _registry->deregister_hook(_hook_name);
    }
}

void ThreadPoolMetricGroup::install(MetricRegistry* registry, const std::string& prefix, ThreadPool* thread_pool) {
    DCHECK(registry != nullptr);
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        DCHECK_EQ(_prefix, prefix);
        DCHECK_EQ(_thread_pool, thread_pool);
        return;
    }

    _registry = registry;
    _thread_pool = thread_pool;
    _prefix = prefix;
    _hook_name = prefix + "_threadpool_metrics";

    registry->register_metric(prefix + "_threadpool_size", &threadpool_size);
    registry->register_metric(prefix + "_executed_tasks_total", &executed_tasks_total);
    registry->register_metric(prefix + "_pending_time_ns_total", &pending_time_ns_total);
    registry->register_metric(prefix + "_execute_time_ns_total", &execute_time_ns_total);
    registry->register_metric(prefix + "_queue_count", &queue_count);
    registry->register_metric(prefix + "_running_threads", &running_threads);
    registry->register_metric(prefix + "_active_threads", &active_threads);
    _hook_registered = registry->register_hook(_hook_name, [this] { update(); });
}

void ThreadPoolMetricGroup::update() {
    if (_thread_pool == nullptr) {
        return;
    }
    threadpool_size.set_value(_thread_pool->max_threads());
    executed_tasks_total.set_value(_thread_pool->total_executed_tasks());
    pending_time_ns_total.set_value(_thread_pool->total_pending_time_ns());
    execute_time_ns_total.set_value(_thread_pool->total_execute_time_ns());
    queue_count.set_value(_thread_pool->num_queued_tasks());
    running_threads.set_value(_thread_pool->num_threads());
    active_threads.set_value(_thread_pool->active_threads());
}

} // namespace starrocks
