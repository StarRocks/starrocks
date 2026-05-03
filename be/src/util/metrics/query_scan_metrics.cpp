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

#include "util/metrics/query_scan_metrics.h"

#include "gutil/macros.h"

namespace starrocks {

QueryScanMetrics* QueryScanMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new QueryScanMetrics();
    return instance;
}

void QueryScanMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;

    registry->register_metric("query_scan_bytes", &query_scan_bytes);
    registry->register_metric("query_scan_rows", &query_scan_rows);
    registry->register_metric("query_scan_bytes_per_second", &query_scan_bytes_per_second);
}

} // namespace starrocks
