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

#include "storage/flat_json_metrics.h"

#include "gutil/macros.h"

namespace starrocks {

FlatJsonMetrics* FlatJsonMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new FlatJsonMetrics();
    return instance;
}

void FlatJsonMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;

#define REGISTER_FLAT_JSON_METRIC(name) registry->register_metric(#name, &name)

    REGISTER_FLAT_JSON_METRIC(flat_json_segment_write_total);
    REGISTER_FLAT_JSON_METRIC(flat_json_write_rows_total);
    REGISTER_FLAT_JSON_METRIC(flat_json_paths_discovered_total);
    REGISTER_FLAT_JSON_METRIC(flat_json_paths_extracted_total);
    REGISTER_FLAT_JSON_METRIC(flat_json_access_hit_total);
    REGISTER_FLAT_JSON_METRIC(flat_json_access_miss_total);
    REGISTER_FLAT_JSON_METRIC(flat_json_cast_duration_ns_total);
    REGISTER_FLAT_JSON_METRIC(flat_json_merge_duration_ns_total);
    REGISTER_FLAT_JSON_METRIC(flat_json_flatten_duration_ns_total);
    REGISTER_FLAT_JSON_METRIC(flat_json_compaction_total);
    REGISTER_FLAT_JSON_METRIC(flat_json_compaction_schema_change_total);

#undef REGISTER_FLAT_JSON_METRIC
}

} // namespace starrocks
