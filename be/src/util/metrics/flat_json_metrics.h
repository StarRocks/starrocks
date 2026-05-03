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

class FlatJsonMetrics {
public:
    FlatJsonMetrics() = default;
    explicit FlatJsonMetrics(MetricRegistry* registry) { install(registry); }
    ~FlatJsonMetrics() = default;

    static FlatJsonMetrics* instance();

    void install(MetricRegistry* registry);

    METRIC_DEFINE_INT_COUNTER(flat_json_segment_write_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(flat_json_write_rows_total, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(flat_json_paths_discovered_total, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_COUNTER(flat_json_paths_extracted_total, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_COUNTER(flat_json_access_hit_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(flat_json_access_miss_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(flat_json_cast_duration_ns_total, MetricUnit::NANOSECONDS);
    METRIC_DEFINE_INT_COUNTER(flat_json_merge_duration_ns_total, MetricUnit::NANOSECONDS);
    METRIC_DEFINE_INT_COUNTER(flat_json_flatten_duration_ns_total, MetricUnit::NANOSECONDS);
    METRIC_DEFINE_INT_COUNTER(flat_json_compaction_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(flat_json_compaction_schema_change_total, MetricUnit::OPERATIONS);

private:
    MetricRegistry* _registry = nullptr;
};

} // namespace starrocks
