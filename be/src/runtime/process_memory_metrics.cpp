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

#include "runtime/process_memory_metrics.h"

#include "jemalloc/jemalloc.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_env.h"

namespace starrocks {

const char* const ProcessMemoryMetrics::_s_hook_name = "process_memory_metrics";

ProcessMemoryMetrics::ProcessMemoryMetrics() = default;

ProcessMemoryMetrics* ProcessMemoryMetrics::instance() {
    // Process-lifetime singleton: instrumentation may touch ProcessMemoryMetrics before
    // the process metrics registry is constructed, then install it into the
    // registry later. Avoid exit-time destructor ordering against the registry.
    static auto* instance = new ProcessMemoryMetrics();
    return instance;
}

ProcessMemoryMetrics::~ProcessMemoryMetrics() {
    if (_registry != nullptr) {
        _registry->deregister_hook(_s_hook_name);
        _registry = nullptr;
    }
}

void ProcessMemoryMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    if (!registry->register_hook(_s_hook_name, [this] { update(); })) {
        return;
    }
    _install_memory_metrics(registry);
    _registry = registry;
}

void ProcessMemoryMetrics::update() {
    // Use try_lock to avoid blocking concurrent callers since metrics collection
    // is best-effort and the data will be refreshed on the next collection cycle.
    std::unique_lock lock(_update_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        return;
    }

    update_memory_metrics();
}

void ProcessMemoryMetrics::_install_memory_metrics(MetricRegistry* registry) {
    registry->register_metric("jemalloc_allocated_bytes", &jemalloc_allocated_bytes);
    registry->register_metric("jemalloc_active_bytes", &jemalloc_active_bytes);
    registry->register_metric("jemalloc_metadata_bytes", &jemalloc_metadata_bytes);
    registry->register_metric("jemalloc_metadata_thp", &jemalloc_metadata_thp);
    registry->register_metric("jemalloc_resident_bytes", &jemalloc_resident_bytes);
    registry->register_metric("jemalloc_mapped_bytes", &jemalloc_mapped_bytes);
    registry->register_metric("jemalloc_retained_bytes", &jemalloc_retained_bytes);

    registry->register_metric("process_mem_bytes", &process_mem_bytes);
    registry->register_metric("query_mem_bytes", &query_mem_bytes);
    registry->register_metric("connector_scan_pool_mem_bytes", &connector_scan_pool_mem_bytes);
    registry->register_metric("load_mem_bytes", &load_mem_bytes);
    registry->register_metric("metadata_mem_bytes", &metadata_mem_bytes);
    registry->register_metric("tablet_metadata_mem_bytes", &tablet_metadata_mem_bytes);
    registry->register_metric("rowset_metadata_mem_bytes", &rowset_metadata_mem_bytes);
    registry->register_metric("segment_metadata_mem_bytes", &segment_metadata_mem_bytes);
    registry->register_metric("column_metadata_mem_bytes", &column_metadata_mem_bytes);
    registry->register_metric("tablet_schema_mem_bytes", &tablet_schema_mem_bytes);
    registry->register_metric("column_zonemap_index_mem_bytes", &column_zonemap_index_mem_bytes);
    registry->register_metric("ordinal_index_mem_bytes", &ordinal_index_mem_bytes);
    registry->register_metric("bitmap_index_mem_bytes", &bitmap_index_mem_bytes);
    registry->register_metric("bloom_filter_index_mem_bytes", &bloom_filter_index_mem_bytes);
    registry->register_metric("builtin_inverted_index_mem_bytes", &builtin_inverted_index_mem_bytes);
    registry->register_metric("segment_zonemap_mem_bytes", &segment_zonemap_mem_bytes);
    registry->register_metric("short_key_index_mem_bytes", &short_key_index_mem_bytes);
    registry->register_metric("compaction_mem_bytes", &compaction_mem_bytes);
    registry->register_metric("schema_change_mem_bytes", &schema_change_mem_bytes);
    registry->register_metric("storage_page_cache_mem_bytes", &storage_page_cache_mem_bytes);
    registry->register_metric("jit_cache_mem_bytes", &jit_cache_mem_bytes);
    registry->register_metric("update_mem_bytes", &update_mem_bytes);
    registry->register_metric("clone_mem_bytes", &clone_mem_bytes);
    registry->register_metric("consistency_mem_bytes", &consistency_mem_bytes);
    registry->register_metric("datacache_mem_bytes", &datacache_mem_bytes);
    registry->register_metric("vector_index_mem_bytes", &vector_index_mem_bytes);
}

void ProcessMemoryMetrics::update_memory_metrics() {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    LOG(INFO) << "Memory tracking is not available with address sanitizer builds.";
#else
    size_t value = 0;
    // Update the statistics cached by mallctl.
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);
    sz = sizeof(size_t);
    if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
        jemalloc_allocated_bytes.set_value(value);
    }
    if (je_mallctl("stats.active", &value, &sz, nullptr, 0) == 0) {
        jemalloc_active_bytes.set_value(value);
    }
    if (je_mallctl("stats.metadata", &value, &sz, nullptr, 0) == 0) {
        jemalloc_metadata_bytes.set_value(value);
    }
    if (je_mallctl("stats.metadata_thp", &value, &sz, nullptr, 0) == 0) {
        jemalloc_metadata_thp.set_value(value);
    }
    if (je_mallctl("stats.resident", &value, &sz, nullptr, 0) == 0) {
        jemalloc_resident_bytes.set_value(value);
    }
    if (je_mallctl("stats.mapped", &value, &sz, nullptr, 0) == 0) {
        jemalloc_mapped_bytes.set_value(value);
    }
    if (je_mallctl("stats.retained", &value, &sz, nullptr, 0) == 0) {
        jemalloc_retained_bytes.set_value(value);
    }
#endif

#define SET_MEM_METRIC_VALUE(tracker, key)                                  \
    if (RuntimeEnv::GetInstance()->tracker() != nullptr) {                  \
        key.set_value(RuntimeEnv::GetInstance()->tracker()->consumption()); \
    }

    SET_MEM_METRIC_VALUE(process_mem_tracker, process_mem_bytes)
    SET_MEM_METRIC_VALUE(query_pool_mem_tracker, query_mem_bytes)
    SET_MEM_METRIC_VALUE(connector_scan_pool_mem_tracker, connector_scan_pool_mem_bytes)
    SET_MEM_METRIC_VALUE(load_mem_tracker, load_mem_bytes)
    SET_MEM_METRIC_VALUE(metadata_mem_tracker, metadata_mem_bytes)
    SET_MEM_METRIC_VALUE(tablet_metadata_mem_tracker, tablet_metadata_mem_bytes)
    SET_MEM_METRIC_VALUE(rowset_metadata_mem_tracker, rowset_metadata_mem_bytes)
    SET_MEM_METRIC_VALUE(segment_metadata_mem_tracker, segment_metadata_mem_bytes)
    SET_MEM_METRIC_VALUE(column_metadata_mem_tracker, column_metadata_mem_bytes)
    SET_MEM_METRIC_VALUE(tablet_schema_mem_tracker, tablet_schema_mem_bytes)
    SET_MEM_METRIC_VALUE(column_zonemap_index_mem_tracker, column_zonemap_index_mem_bytes)
    SET_MEM_METRIC_VALUE(ordinal_index_mem_tracker, ordinal_index_mem_bytes)
    SET_MEM_METRIC_VALUE(bitmap_index_mem_tracker, bitmap_index_mem_bytes)
    SET_MEM_METRIC_VALUE(bloom_filter_index_mem_tracker, bloom_filter_index_mem_bytes)
    SET_MEM_METRIC_VALUE(builtin_inverted_index_mem_tracker, builtin_inverted_index_mem_bytes)
    SET_MEM_METRIC_VALUE(segment_zonemap_mem_tracker, segment_zonemap_mem_bytes)
    SET_MEM_METRIC_VALUE(short_key_index_mem_tracker, short_key_index_mem_bytes)
    SET_MEM_METRIC_VALUE(compaction_mem_tracker, compaction_mem_bytes)
    SET_MEM_METRIC_VALUE(schema_change_mem_tracker, schema_change_mem_bytes)
    SET_MEM_METRIC_VALUE(page_cache_mem_tracker, storage_page_cache_mem_bytes)
    SET_MEM_METRIC_VALUE(jit_cache_mem_tracker, jit_cache_mem_bytes)
    SET_MEM_METRIC_VALUE(update_mem_tracker, update_mem_bytes)
    SET_MEM_METRIC_VALUE(passthrough_mem_tracker, passthrough_mem_bytes)
    SET_MEM_METRIC_VALUE(brpc_iobuf_mem_tracker, brpc_iobuf_mem_bytes)
    SET_MEM_METRIC_VALUE(clone_mem_tracker, clone_mem_bytes)
    SET_MEM_METRIC_VALUE(consistency_mem_tracker, consistency_mem_bytes)
    SET_MEM_METRIC_VALUE(datacache_mem_tracker, datacache_mem_bytes)
    SET_MEM_METRIC_VALUE(replication_mem_tracker, replication_mem_bytes)
    SET_MEM_METRIC_VALUE(vector_index_mem_tracker, vector_index_mem_bytes)
#undef SET_MEM_METRIC_VALUE
}

} // namespace starrocks
