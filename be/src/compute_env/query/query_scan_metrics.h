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

class QueryScanMetrics {
public:
    QueryScanMetrics() = default;
    explicit QueryScanMetrics(MetricRegistry* registry) { install(registry); }
    ~QueryScanMetrics() = default;

    static QueryScanMetrics* instance();

    void install(MetricRegistry* registry);

    METRIC_DEFINE_INT_GAUGE(query_scan_bytes_per_second, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(query_scan_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(query_scan_rows, MetricUnit::ROWS);

private:
    MetricRegistry* _registry = nullptr;
};

} // namespace starrocks
