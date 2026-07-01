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

#include "runtime/memory/process_memory_metrics.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {
namespace {

void assert_metric_registered(MetricRegistry* registry, const std::string& name) {
    ASSERT_NE(nullptr, registry->get_metric(name)) << name;
}

} // namespace

TEST(ProcessMemoryMetricsTest, InstallRegistersProcessMemoryMetrics) {
    MetricRegistry registry("test_registry");
    ProcessMemoryMetrics metrics;
    metrics.install(&registry);
    metrics.install(&registry);

    assert_metric_registered(&registry, "jemalloc_allocated_bytes");
    assert_metric_registered(&registry, "jemalloc_active_bytes");
    assert_metric_registered(&registry, "jemalloc_metadata_bytes");
    assert_metric_registered(&registry, "jemalloc_metadata_thp");
    assert_metric_registered(&registry, "jemalloc_resident_bytes");
    assert_metric_registered(&registry, "jemalloc_mapped_bytes");
    assert_metric_registered(&registry, "jemalloc_retained_bytes");

    assert_metric_registered(&registry, "process_mem_bytes");
    assert_metric_registered(&registry, "query_mem_bytes");
    assert_metric_registered(&registry, "connector_scan_pool_mem_bytes");
    assert_metric_registered(&registry, "load_mem_bytes");
    assert_metric_registered(&registry, "metadata_mem_bytes");
    assert_metric_registered(&registry, "tablet_metadata_mem_bytes");
    assert_metric_registered(&registry, "rowset_metadata_mem_bytes");
    assert_metric_registered(&registry, "segment_metadata_mem_bytes");
    assert_metric_registered(&registry, "column_metadata_mem_bytes");
    assert_metric_registered(&registry, "tablet_schema_mem_bytes");
    assert_metric_registered(&registry, "column_zonemap_index_mem_bytes");
    assert_metric_registered(&registry, "ordinal_index_mem_bytes");
    assert_metric_registered(&registry, "bitmap_index_mem_bytes");
    assert_metric_registered(&registry, "bloom_filter_index_mem_bytes");
    assert_metric_registered(&registry, "builtin_inverted_index_mem_bytes");
    assert_metric_registered(&registry, "segment_zonemap_mem_bytes");
    assert_metric_registered(&registry, "short_key_index_mem_bytes");
    assert_metric_registered(&registry, "compaction_mem_bytes");
    assert_metric_registered(&registry, "schema_change_mem_bytes");
    assert_metric_registered(&registry, "storage_page_cache_mem_bytes");
    assert_metric_registered(&registry, "jit_cache_mem_bytes");
    assert_metric_registered(&registry, "update_mem_bytes");
    assert_metric_registered(&registry, "clone_mem_bytes");
    assert_metric_registered(&registry, "consistency_mem_bytes");
    assert_metric_registered(&registry, "datacache_mem_bytes");
    assert_metric_registered(&registry, "vector_index_mem_bytes");
}

TEST(ProcessMemoryMetricsTest, UpdateIsSafeBeforeRuntimeEnvInitialization) {
    MetricRegistry registry("test_registry");
    ProcessMemoryMetrics metrics;
    metrics.install(&registry);

    metrics.update();
    registry.trigger_hook();
}

} // namespace starrocks
