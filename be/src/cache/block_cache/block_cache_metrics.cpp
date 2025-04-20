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

#include "cache/block_cache/block_cache_metrics.h"

namespace starrocks {

void BlockCacheMetrics::register_metrics(MetricRegistry* registry) {
    // Cache status metrics
    registry->register_metric("starrocks_cache_status", &cache_status);
    
    // Memory metrics
    registry->register_metric("starrocks_cache_memory_bytes", MetricLabels().add("type", "quota"), &mem_quota_bytes);
    registry->register_metric("starrocks_cache_memory_bytes", MetricLabels().add("type", "used"), &mem_used_bytes);
    registry->register_metric("starrocks_cache_memory_usage_rate", &mem_used_rate);
    
    // Disk metrics
    registry->register_metric("starrocks_cache_disk_bytes", MetricLabels().add("type", "quota"), &disk_quota_bytes);
    registry->register_metric("starrocks_cache_disk_bytes", MetricLabels().add("type", "used"), &disk_used_bytes);
    registry->register_metric("starrocks_cache_disk_usage_rate", &disk_used_rate);
    registry->register_metric("starrocks_cache_meta_bytes", &meta_used_bytes);
    
    // Hit/miss metrics
    registry->register_metric("starrocks_cache_operations_total", MetricLabels().add("type", "hit"), &hit_count);
    registry->register_metric("starrocks_cache_operations_total", MetricLabels().add("type", "miss"), &miss_count);
    registry->register_metric("starrocks_cache_hit_rate", &hit_rate);
    registry->register_metric("starrocks_cache_bytes_total", MetricLabels().add("type", "hit"), &hit_bytes);
    registry->register_metric("starrocks_cache_bytes_total", MetricLabels().add("type", "miss"), &miss_bytes);
    
    // Last minute metrics
    registry->register_metric("starrocks_cache_operations_last_minute", MetricLabels().add("type", "hit"), &hit_count_last_minute);
    registry->register_metric("starrocks_cache_operations_last_minute", MetricLabels().add("type", "miss"), &miss_count_last_minute);
    registry->register_metric("starrocks_cache_bytes_last_minute", MetricLabels().add("type", "hit"), &hit_bytes_last_minute);
    registry->register_metric("starrocks_cache_bytes_last_minute", MetricLabels().add("type", "miss"), &miss_bytes_last_minute);
    
    // Read metrics
    registry->register_metric("starrocks_cache_read_bytes_total", MetricLabels().add("type", "memory"), &read_mem_bytes);
    registry->register_metric("starrocks_cache_read_bytes_total", MetricLabels().add("type", "disk"), &read_disk_bytes);
    
    // Write metrics
    registry->register_metric("starrocks_cache_write_bytes_total", &write_bytes);
    registry->register_metric("starrocks_cache_write_operations_total", MetricLabels().add("type", "success"), &write_success_count);
    registry->register_metric("starrocks_cache_write_operations_total", MetricLabels().add("type", "fail"), &write_fail_count);
    
    // Remove metrics
    registry->register_metric("starrocks_cache_remove_bytes_total", &remove_bytes);
    registry->register_metric("starrocks_cache_remove_operations_total", MetricLabels().add("type", "success"), &remove_success_count);
    registry->register_metric("starrocks_cache_remove_operations_total", MetricLabels().add("type", "fail"), &remove_fail_count);
    
    // Current operation counts
    registry->register_metric("starrocks_cache_current_operations", MetricLabels().add("type", "reading"), &current_reading_count);
    registry->register_metric("starrocks_cache_current_operations", MetricLabels().add("type", "writing"), &current_writing_count);
    registry->register_metric("starrocks_cache_current_operations", MetricLabels().add("type", "removing"), &current_removing_count);
}

} // namespace starrocks 