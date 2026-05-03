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

#include "base/metrics.h"

namespace starrocks {

class ServiceMetrics {
public:
    ServiceMetrics() = default;
    explicit ServiceMetrics(MetricRegistry* registry) { install(registry); }
    ~ServiceMetrics() = default;

    static ServiceMetrics* instance();

    void install(MetricRegistry* registry);

    METRIC_DEFINE_INT_COUNTER(staros_shard_info_fallback_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(staros_shard_info_fallback_failed_total, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(short_circuit_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(short_circuit_request_duration_us, MetricUnit::MICROSECONDS);

private:
    MetricRegistry* _registry = nullptr;
};

} // namespace starrocks
