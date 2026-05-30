// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <map>
#include <memory>
#include <mutex>

#include "base/metrics.h"

namespace starrocks {

class QueryCacheMetrics;
class VectorIndexCacheMetrics;
class RuntimeFilterMetrics;

class MemoryMetrics {
public:
    METRIC_DEFINE_INT_GAUGE(jemalloc_allocated_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jemalloc_active_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jemalloc_metadata_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jemalloc_metadata_thp, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(jemalloc_resident_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jemalloc_mapped_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jemalloc_retained_bytes, MetricUnit::BYTES);

    // MemPool metrics
    METRIC_DEFINE_INT_GAUGE(process_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(query_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(connector_scan_pool_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(load_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(metadata_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(tablet_metadata_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(rowset_metadata_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(segment_metadata_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(column_metadata_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(tablet_schema_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(column_zonemap_index_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(ordinal_index_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(bitmap_index_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(bloom_filter_index_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(builtin_inverted_index_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(segment_zonemap_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(short_key_index_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(compaction_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(schema_change_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(storage_page_cache_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jit_cache_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(update_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(passthrough_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(brpc_iobuf_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(clone_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(consistency_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(datacache_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(replication_mem_bytes, MetricUnit::BYTES);
};

class SystemMetrics {
public:
    SystemMetrics();
    ~SystemMetrics();

    static SystemMetrics* instance();

    // install higher-level system metrics to registry
    void install(MetricRegistry* registry);

    // update metrics
    void update();

    const MemoryMetrics* memory_metrics() const { return _memory_metrics.get(); }

    void update_memory_metrics();

private:
    void _install_memory_metrics(MetricRegistry* registry);

    void _install_query_cache_metrics(MetricRegistry* registry);

    void _update_query_cache_metrics();

    void _install_runtime_filter_metrics(MetricRegistry* registry);

    void _update_runtime_filter_metrics();

    void _install_vector_index_cache_metrics(MetricRegistry* registry);

    void _update_vector_index_cache_metrics();

    void _update_datacache_mem_tracker();
    void _update_pagecache_mem_tracker();

private:
    static const char* const _s_hook_name;

    std::unique_ptr<MemoryMetrics> _memory_metrics;
    std::unique_ptr<QueryCacheMetrics> _query_cache_metrics;
    std::unique_ptr<VectorIndexCacheMetrics> _vector_index_cache_metrics;
    std::map<std::string, RuntimeFilterMetrics*> _runtime_filter_metrics;

    std::mutex _update_mutex;
    MetricRegistry* _registry = nullptr;
};

} // namespace starrocks
