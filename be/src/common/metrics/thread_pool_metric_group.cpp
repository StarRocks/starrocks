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

#include "common/metrics/thread_pool_metric_group.h"

#include <functional>

#include "common/thread/threadpool.h"

namespace starrocks {

namespace {

std::string thread_pool_metric_name(const std::string& name, const std::string& suffix) {
    return name + "_" + suffix;
}

void register_metric_and_hook(MetricRegistry* registry, const std::string& name, Metric* metric,
                              const std::function<void()>& hook, std::vector<std::string>* hook_names) {
    const auto metric_registered = registry->register_metric(name, metric);
    DCHECK(metric_registered);
    const auto hook_registered = registry->register_hook(name, hook);
    DCHECK(hook_registered);
    if (hook_registered) {
        hook_names->emplace_back(name);
    }
}

} // namespace

ThreadPoolMetricGroup::~ThreadPoolMetricGroup() {
    unregister_metrics();
}

void ThreadPoolMetricGroup::register_metrics(MetricRegistry* registry, const std::string& name,
                                             ThreadPool* threadpool) {
    DCHECK(registry != nullptr);
    DCHECK(threadpool != nullptr);

    unregister_metrics();
    _registry = registry;

    const auto threadpool_size_name = thread_pool_metric_name(name, "threadpool_size");
    register_metric_and_hook(
            registry, threadpool_size_name, &threadpool_size,
            [this, threadpool] { threadpool_size.set_value(threadpool->max_threads()); }, &_hook_names);

    const auto executed_tasks_total_name = thread_pool_metric_name(name, "executed_tasks_total");
    register_metric_and_hook(
            registry, executed_tasks_total_name, &executed_tasks_total,
            [this, threadpool] { executed_tasks_total.set_value(threadpool->total_executed_tasks()); }, &_hook_names);

    const auto pending_time_ns_total_name = thread_pool_metric_name(name, "pending_time_ns_total");
    register_metric_and_hook(
            registry, pending_time_ns_total_name, &pending_time_ns_total,
            [this, threadpool] { pending_time_ns_total.set_value(threadpool->total_pending_time_ns()); }, &_hook_names);

    const auto execute_time_ns_total_name = thread_pool_metric_name(name, "execute_time_ns_total");
    register_metric_and_hook(
            registry, execute_time_ns_total_name, &execute_time_ns_total,
            [this, threadpool] { execute_time_ns_total.set_value(threadpool->total_execute_time_ns()); }, &_hook_names);

    const auto queue_count_name = thread_pool_metric_name(name, "queue_count");
    register_metric_and_hook(
            registry, queue_count_name, &queue_count,
            [this, threadpool] { queue_count.set_value(threadpool->num_queued_tasks()); }, &_hook_names);

    const auto running_threads_name = thread_pool_metric_name(name, "running_threads");
    register_metric_and_hook(
            registry, running_threads_name, &running_threads,
            [this, threadpool] { running_threads.set_value(threadpool->num_threads()); }, &_hook_names);

    const auto active_threads_name = thread_pool_metric_name(name, "active_threads");
    register_metric_and_hook(
            registry, active_threads_name, &active_threads,
            [this, threadpool] { active_threads.set_value(threadpool->active_threads()); }, &_hook_names);
}

void ThreadPoolMetricGroup::unregister_metrics() {
    if (_registry == nullptr) {
        return;
    }

    const auto registry_alive = threadpool_size.registry() == _registry ||
                                executed_tasks_total.registry() == _registry ||
                                pending_time_ns_total.registry() == _registry ||
                                execute_time_ns_total.registry() == _registry || queue_count.registry() == _registry ||
                                running_threads.registry() == _registry || active_threads.registry() == _registry;
    if (!registry_alive) {
        _registry = nullptr;
        _hook_names.clear();
        return;
    }

    for (const auto& hook_name : _hook_names) {
        _registry->deregister_hook(hook_name);
    }

    threadpool_size.hide();
    executed_tasks_total.hide();
    pending_time_ns_total.hide();
    execute_time_ns_total.hide();
    queue_count.hide();
    running_threads.hide();
    active_threads.hide();

    _registry = nullptr;
    _hook_names.clear();
}

} // namespace starrocks
