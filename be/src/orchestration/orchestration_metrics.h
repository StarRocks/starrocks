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

#include <array>
#include <functional>
#include <memory>

#include "base/metrics.h"
#include "runtime/runtime_filter_worker_event.h"

namespace starrocks::orchestration {

class OrchestrationMetrics {
public:
    using RuntimeFilterMetricsProvider = std::function<const RuntimeFilterWorkerMetrics*()>;

    OrchestrationMetrics() = default;
    ~OrchestrationMetrics();

    void install(MetricRegistry* registry, RuntimeFilterMetricsProvider runtime_filter_metrics_provider);
    void update_runtime_filter_metrics();

private:
    struct RuntimeFilterEventMetrics {
        METRIC_DEFINE_INT_GAUGE(runtime_filter_events_in_queue, MetricUnit::NOUNIT);
        METRIC_DEFINE_INT_GAUGE(runtime_filter_bytes_in_queue, MetricUnit::BYTES);
    };

    MetricRegistry* _registry = nullptr;
    RuntimeFilterMetricsProvider _runtime_filter_metrics_provider;
    std::array<std::unique_ptr<RuntimeFilterEventMetrics>, EventType::MAX_COUNT> _runtime_filter_event_metrics;
};

} // namespace starrocks::orchestration
