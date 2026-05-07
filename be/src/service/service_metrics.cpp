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

#include "service/service_metrics.h"

#include "gutil/macros.h"

namespace starrocks {

ServiceMetrics* ServiceMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new ServiceMetrics();
    return instance;
}

void ServiceMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;

    registry->register_metric("staros_shard_info_fallback_total", &staros_shard_info_fallback_total);
    registry->register_metric("staros_shard_info_fallback_failed_total", &staros_shard_info_fallback_failed_total);
    registry->register_metric("short_circuit_request_total", &short_circuit_request_total);
    registry->register_metric("short_circuit_request_duration_us", &short_circuit_request_duration_us);
}

} // namespace starrocks
