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

#include "orchestration/orchestration_metrics.h"

#include <atomic>
#include <utility>

#include "common/logging.h"

namespace starrocks::orchestration {

namespace {

const char* const kRuntimeFilterMetricsHookName = "orchestration_runtime_filter_metrics";

} // namespace

OrchestrationMetrics::~OrchestrationMetrics() {
    if (_registry != nullptr) {
        _registry->deregister_hook(kRuntimeFilterMetricsHookName);
        _registry = nullptr;
    }
}

void OrchestrationMetrics::install(MetricRegistry* registry,
                                   RuntimeFilterMetricsProvider runtime_filter_metrics_provider,
                                   RuntimeFilterQueueSizeProvider runtime_filter_queue_size_provider) {
    if (registry == nullptr) {
        return;
    }
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _runtime_filter_metrics_provider = std::move(runtime_filter_metrics_provider);
    _runtime_filter_queue_size_provider = std::move(runtime_filter_queue_size_provider);
    registry->register_metric("runtime_filter_event_queue_len", &runtime_filter_event_queue_len);
    for (int i = 0; i < EventType::MAX_COUNT; i++) {
        auto event_metrics = std::make_unique<RuntimeFilterEventMetrics>();
        const auto& type = EventTypeToString(static_cast<EventType>(i));
        auto labels = MetricLabels().add("type", type);
        registry->register_metric("runtime_filter_events_in_queue", labels,
                                  &event_metrics->runtime_filter_events_in_queue);
        registry->register_metric("runtime_filter_bytes_in_queue", labels,
                                  &event_metrics->runtime_filter_bytes_in_queue);
        _runtime_filter_event_metrics[i] = std::move(event_metrics);
    }
    if (!registry->register_hook(kRuntimeFilterMetricsHookName, [this] { update_runtime_filter_metrics(); })) {
        LOG(WARNING) << "failed to register orchestration runtime filter metrics hook";
        return;
    }
    _registry = registry;
}

void OrchestrationMetrics::update_runtime_filter_metrics() {
    if (_runtime_filter_queue_size_provider) {
        runtime_filter_event_queue_len.set_value(_runtime_filter_queue_size_provider());
    }
    if (!_runtime_filter_metrics_provider) {
        return;
    }
    const auto* runtime_filter_metrics = _runtime_filter_metrics_provider();
    if (runtime_filter_metrics == nullptr) {
        return;
    }
    for (int i = 0; i < EventType::MAX_COUNT; i++) {
        auto* event_metrics = _runtime_filter_event_metrics[i].get();
        if (event_metrics == nullptr) {
            continue;
        }
        event_metrics->runtime_filter_events_in_queue.set_value(
                runtime_filter_metrics->event_nums[i].load(std::memory_order_relaxed));
        event_metrics->runtime_filter_bytes_in_queue.set_value(
                runtime_filter_metrics->runtime_filter_bytes[i].load(std::memory_order_relaxed));
    }
}

} // namespace starrocks::orchestration
