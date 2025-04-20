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

#include "util/metrics.h"

namespace starrocks {

class BlockCacheMetrics {
public:
    BlockCacheMetrics() = default;
    ~BlockCacheMetrics() = default;

    void register_metrics(MetricRegistry* registry);

    // Cache status metrics
    METRIC_DEFINE_INT_GAUGE(cache_status, MetricUnit::NOUNIT);
    
    // Memory metrics
    METRIC_DEFINE_INT_GAUGE(mem_quota_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(mem_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_DOUBLE_GAUGE(mem_used_rate, MetricUnit::PERCENT);
    
    // Disk metrics
    METRIC_DEFINE_INT_GAUGE(disk_quota_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(disk_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_DOUBLE_GAUGE(disk_used_rate, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(meta_used_bytes, MetricUnit::BYTES);
    
    // Hit/miss metrics
    METRIC_DEFINE_INT_COUNTER(hit_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_COUNTER(miss_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_DOUBLE_GAUGE(hit_rate, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_COUNTER(hit_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(miss_bytes, MetricUnit::BYTES);
    
    // Last minute metrics
    METRIC_DEFINE_INT_COUNTER(hit_count_last_minute, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_COUNTER(miss_count_last_minute, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_COUNTER(hit_bytes_last_minute, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(miss_bytes_last_minute, MetricUnit::BYTES);
    
    // Read metrics
    METRIC_DEFINE_INT_COUNTER(read_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(read_disk_bytes, MetricUnit::BYTES);
    
    // Write metrics
    METRIC_DEFINE_INT_COUNTER(write_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(write_success_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_COUNTER(write_fail_count, MetricUnit::NOUNIT);
    
    // Remove metrics
    METRIC_DEFINE_INT_COUNTER(remove_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(remove_success_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_COUNTER(remove_fail_count, MetricUnit::NOUNIT);
    
    // Current operation counts
    METRIC_DEFINE_INT_GAUGE(current_reading_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(current_writing_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(current_removing_count, MetricUnit::NOUNIT);
};

} // namespace starrocks 