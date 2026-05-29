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

#include "util/system_metrics.h"

#include <runtime/exec_env.h>
#include <runtime/mem_tracker.h>
#ifdef WITH_TENANN
#include <tenann/index/index_cache.h>
#endif

#include "cache/datacache.h"
#ifdef USE_STAROS
#include "fslib/star_cache_handler.h"
#endif
#include "cache/mem_cache/page_cache.h"
#include "common/config_cache_fwd.h"
#include "exec/query_cache/cache_manager.h"
#include "jemalloc/jemalloc.h"
#include "runtime/runtime_filter_worker.h"

namespace starrocks {

const char* const SystemMetrics::_s_hook_name = "system_metrics";

class QueryCacheMetrics {
public:
    METRIC_DEFINE_INT_GAUGE(query_cache_capacity, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(query_cache_usage, MetricUnit::BYTES);
    METRIC_DEFINE_DOUBLE_GAUGE(query_cache_usage_ratio, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(query_cache_lookup_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(query_cache_hit_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_DOUBLE_GAUGE(query_cache_hit_ratio, MetricUnit::PERCENT);
};

class VectorIndexCacheMetrics {
    friend class SystemMetrics;

public:
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_capacity, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_usage, MetricUnit::BYTES);
    METRIC_DEFINE_DOUBLE_GAUGE(vector_index_cache_usage_ratio, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_lookup_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_hit_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_DOUBLE_GAUGE(vector_index_cache_hit_ratio, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_dynamic_lookup_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_dynamic_hit_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_DOUBLE_GAUGE(vector_index_cache_dynamic_hit_ratio, MetricUnit::PERCENT);

private:
    int _previous_lookup_count;
    int _previous_hit_count;
};

class RuntimeFilterMetrics {
public:
    METRIC_DEFINE_INT_GAUGE(runtime_filter_events_in_queue, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(runtime_filter_bytes_in_queue, MetricUnit::BYTES);
};

SystemMetrics::SystemMetrics() = default;

SystemMetrics* SystemMetrics::instance() {
    // Process-lifetime singleton: instrumentation may touch SystemMetrics before
    // the process metrics registry is constructed, then install it into the
    // registry later. Avoid exit-time destructor ordering against the registry.
    static auto* instance = new SystemMetrics();
    return instance;
}

SystemMetrics::~SystemMetrics() {
    if (_registry != nullptr) {
        _registry->deregister_hook(_s_hook_name);
        _registry = nullptr;
    }
    for (auto& it : _runtime_filter_metrics) {
        delete it.second;
    }
}

void SystemMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    if (!registry->register_hook(_s_hook_name, [this] { update(); })) {
        return;
    }
    _install_memory_metrics(registry);
    _install_query_cache_metrics(registry);
    _install_runtime_filter_metrics(registry);
    _install_vector_index_cache_metrics(registry);
    _registry = registry;
}

void SystemMetrics::update() {
    // Use try_lock to avoid blocking concurrent callers since metrics collection
    // is best-effort and the data will be refreshed on the next collection cycle.
    std::unique_lock lock(_update_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        return;
    }

    update_memory_metrics();
    _update_query_cache_metrics();
    _update_runtime_filter_metrics();
    _update_vector_index_cache_metrics();
}

void SystemMetrics::_install_memory_metrics(MetricRegistry* registry) {
    _memory_metrics = std::make_unique<MemoryMetrics>();
    registry->register_metric("jemalloc_allocated_bytes", &_memory_metrics->jemalloc_allocated_bytes);
    registry->register_metric("jemalloc_active_bytes", &_memory_metrics->jemalloc_active_bytes);
    registry->register_metric("jemalloc_metadata_bytes", &_memory_metrics->jemalloc_metadata_bytes);
    registry->register_metric("jemalloc_metadata_thp", &_memory_metrics->jemalloc_metadata_thp);
    registry->register_metric("jemalloc_resident_bytes", &_memory_metrics->jemalloc_resident_bytes);
    registry->register_metric("jemalloc_mapped_bytes", &_memory_metrics->jemalloc_mapped_bytes);
    registry->register_metric("jemalloc_retained_bytes", &_memory_metrics->jemalloc_retained_bytes);

    registry->register_metric("process_mem_bytes", &_memory_metrics->process_mem_bytes);
    registry->register_metric("query_mem_bytes", &_memory_metrics->query_mem_bytes);
    registry->register_metric("connector_scan_pool_mem_bytes", &_memory_metrics->connector_scan_pool_mem_bytes);
    registry->register_metric("load_mem_bytes", &_memory_metrics->load_mem_bytes);
    registry->register_metric("metadata_mem_bytes", &_memory_metrics->metadata_mem_bytes);
    registry->register_metric("tablet_metadata_mem_bytes", &_memory_metrics->tablet_metadata_mem_bytes);
    registry->register_metric("rowset_metadata_mem_bytes", &_memory_metrics->rowset_metadata_mem_bytes);
    registry->register_metric("segment_metadata_mem_bytes", &_memory_metrics->segment_metadata_mem_bytes);
    registry->register_metric("column_metadata_mem_bytes", &_memory_metrics->column_metadata_mem_bytes);
    registry->register_metric("tablet_schema_mem_bytes", &_memory_metrics->tablet_schema_mem_bytes);
    registry->register_metric("column_zonemap_index_mem_bytes", &_memory_metrics->column_zonemap_index_mem_bytes);
    registry->register_metric("ordinal_index_mem_bytes", &_memory_metrics->ordinal_index_mem_bytes);
    registry->register_metric("bitmap_index_mem_bytes", &_memory_metrics->bitmap_index_mem_bytes);
    registry->register_metric("bloom_filter_index_mem_bytes", &_memory_metrics->bloom_filter_index_mem_bytes);
    registry->register_metric("builtin_inverted_index_mem_bytes", &_memory_metrics->builtin_inverted_index_mem_bytes);
    registry->register_metric("segment_zonemap_mem_bytes", &_memory_metrics->segment_zonemap_mem_bytes);
    registry->register_metric("short_key_index_mem_bytes", &_memory_metrics->short_key_index_mem_bytes);
    registry->register_metric("compaction_mem_bytes", &_memory_metrics->compaction_mem_bytes);
    registry->register_metric("schema_change_mem_bytes", &_memory_metrics->schema_change_mem_bytes);
    registry->register_metric("storage_page_cache_mem_bytes", &_memory_metrics->storage_page_cache_mem_bytes);
    registry->register_metric("jit_cache_mem_bytes", &_memory_metrics->jit_cache_mem_bytes);
    registry->register_metric("update_mem_bytes", &_memory_metrics->update_mem_bytes);
    registry->register_metric("clone_mem_bytes", &_memory_metrics->clone_mem_bytes);
    registry->register_metric("consistency_mem_bytes", &_memory_metrics->consistency_mem_bytes);
    registry->register_metric("datacache_mem_bytes", &_memory_metrics->datacache_mem_bytes);
}

void SystemMetrics::_update_datacache_mem_tracker() {
    int64_t datacache_mem_bytes = 0;
    auto* datacache_mem_tracker = GlobalEnv::GetInstance()->datacache_mem_tracker();
    if (datacache_mem_tracker) {
        LocalMemCacheEngine* local_cache = DataCache::GetInstance()->local_mem_cache();
        if (local_cache != nullptr && local_cache->is_initialized()) {
            auto datacache_metrics = local_cache->cache_metrics();
            datacache_mem_bytes = datacache_metrics.mem_used_bytes;
        }
#ifdef USE_STAROS
        if (!config::datacache_unified_instance_enable) {
            datacache_mem_bytes += staros::starlet::fslib::star_cache_get_memory_usage();
        }
#endif
        datacache_mem_tracker->set(datacache_mem_bytes);
    }
}

void SystemMetrics::_update_pagecache_mem_tracker() {
    auto* pagecache_mem_tracker = GlobalEnv::GetInstance()->page_cache_mem_tracker();
    auto* page_cache = StoragePageCache::instance();
    if (pagecache_mem_tracker && page_cache != nullptr && page_cache->is_initialized()) {
        pagecache_mem_tracker->set(page_cache->memory_usage());
    }
}

void SystemMetrics::update_memory_metrics() {
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
        _memory_metrics->jemalloc_allocated_bytes.set_value(value);
    }
    if (je_mallctl("stats.active", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_active_bytes.set_value(value);
    }
    if (je_mallctl("stats.metadata", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_metadata_bytes.set_value(value);
    }
    if (je_mallctl("stats.metadata_thp", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_metadata_thp.set_value(value);
    }
    if (je_mallctl("stats.resident", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_resident_bytes.set_value(value);
    }
    if (je_mallctl("stats.mapped", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_mapped_bytes.set_value(value);
    }
    if (je_mallctl("stats.retained", &value, &sz, nullptr, 0) == 0) {
        _memory_metrics->jemalloc_retained_bytes.set_value(value);
    }
#endif

    _update_datacache_mem_tracker();
    _update_pagecache_mem_tracker();

#define SET_MEM_METRIC_VALUE(tracker, key)                                                  \
    if (GlobalEnv::GetInstance()->tracker() != nullptr) {                                   \
        _memory_metrics->key.set_value(GlobalEnv::GetInstance()->tracker()->consumption()); \
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
#undef SET_MEM_METRIC_VALUE
}

void SystemMetrics::_update_query_cache_metrics() {
    auto* cache_mgr = ExecEnv::GetInstance()->cache_mgr();
    if (UNLIKELY(cache_mgr == nullptr)) {
        return;
    }
    auto capacity = cache_mgr->capacity();
    auto usage = cache_mgr->memory_usage();
    auto lookup_count = cache_mgr->lookup_count();
    auto hit_count = cache_mgr->hit_count();
    auto usage_ratio = (capacity == 0L) ? 0.0 : double(usage) / double(capacity);
    auto hit_ratio = (lookup_count == 0L) ? 0.0 : double(hit_count) / double(lookup_count);
    _query_cache_metrics->query_cache_capacity.set_value(capacity);
    _query_cache_metrics->query_cache_usage.set_value(usage);
    _query_cache_metrics->query_cache_usage_ratio.set_value(usage_ratio);
    _query_cache_metrics->query_cache_lookup_count.set_value(lookup_count);
    _query_cache_metrics->query_cache_hit_count.set_value(hit_count);
    _query_cache_metrics->query_cache_hit_ratio.set_value(hit_ratio);
}

void SystemMetrics::_install_query_cache_metrics(MetricRegistry* registry) {
    _query_cache_metrics = std::make_unique<QueryCacheMetrics>();
    registry->register_metric("query_cache_capacity", &_query_cache_metrics->query_cache_capacity);
    registry->register_metric("query_cache_usage", &_query_cache_metrics->query_cache_usage);
    registry->register_metric("query_cache_usage_ratio", &_query_cache_metrics->query_cache_usage_ratio);
    registry->register_metric("query_cache_lookup_count", &_query_cache_metrics->query_cache_lookup_count);
    registry->register_metric("query_cache_hit_count", &_query_cache_metrics->query_cache_hit_count);
    registry->register_metric("query_cache_hit_ratio", &_query_cache_metrics->query_cache_hit_ratio);
}

void SystemMetrics::_install_runtime_filter_metrics(MetricRegistry* registry) {
    for (int i = 0; i < EventType::MAX_COUNT; i++) {
        auto* metrics = new RuntimeFilterMetrics();
        const auto& type = EventTypeToString((EventType)i);
#define REGISTER_RUNTIME_FILTER_METRIC(name) \
    registry->register_metric(#name, MetricLabels().add("type", type), &metrics->name)
        REGISTER_RUNTIME_FILTER_METRIC(runtime_filter_events_in_queue);
        REGISTER_RUNTIME_FILTER_METRIC(runtime_filter_bytes_in_queue);
        _runtime_filter_metrics.emplace(type, metrics);
#undef REGISTER_RUNTIME_FILTER_METRIC
    }
}

void SystemMetrics::_update_runtime_filter_metrics() {
    auto* runtime_filter_worker = ExecEnv::GetInstance()->runtime_filter_worker();
    if (UNLIKELY(runtime_filter_worker == nullptr)) {
        return;
    }
    const auto* metrics = runtime_filter_worker->metrics();
    for (int i = 0; i < EventType::MAX_COUNT; i++) {
        const auto& event_name = EventTypeToString((EventType)i);
        auto iter = _runtime_filter_metrics.find(event_name);
        if (iter == _runtime_filter_metrics.end()) {
            continue;
        }
        iter->second->runtime_filter_events_in_queue.set_value(metrics->event_nums[i]);
        iter->second->runtime_filter_bytes_in_queue.set_value(metrics->runtime_filter_bytes[i]);
    }
}

void SystemMetrics::_install_vector_index_cache_metrics(MetricRegistry* registry) {
    _vector_index_cache_metrics = std::make_unique<VectorIndexCacheMetrics>();
    registry->register_metric("vector_index_cache_capacity", &_vector_index_cache_metrics->vector_index_cache_capacity);
    registry->register_metric("vector_index_cache_usage", &_vector_index_cache_metrics->vector_index_cache_usage);
    registry->register_metric("vector_index_cache_usage_ratio",
                              &_vector_index_cache_metrics->vector_index_cache_usage_ratio);
    registry->register_metric("vector_index_cache_lookup_count",
                              &_vector_index_cache_metrics->vector_index_cache_lookup_count);
    registry->register_metric("vector_index_cache_hit_count",
                              &_vector_index_cache_metrics->vector_index_cache_hit_count);
    registry->register_metric("vector_index_cache_hit_ratio",
                              &_vector_index_cache_metrics->vector_index_cache_hit_ratio);
    registry->register_metric("vector_index_cache_dynamic_lookup_count",
                              &_vector_index_cache_metrics->vector_index_cache_dynamic_lookup_count);
    registry->register_metric("vector_index_cache_dynamic_hit_count",
                              &_vector_index_cache_metrics->vector_index_cache_dynamic_hit_count);
    registry->register_metric("vector_index_cache_dynamic_hit_ratio",
                              &_vector_index_cache_metrics->vector_index_cache_dynamic_hit_ratio);
}

void SystemMetrics::_update_vector_index_cache_metrics() {
#ifdef WITH_TENANN
    auto* index_cache = tenann::IndexCache::GetGlobalInstance();
    if (UNLIKELY(index_cache == nullptr)) {
        return;
    }
    auto capacity = index_cache->capacity();
    auto usage = index_cache->memory_usage();
    auto lookup_count = index_cache->lookup_count();
    auto hit_count = index_cache->hit_count();
#else
    auto capacity = 0;
    auto usage = 0;
    auto lookup_count = 0;
    auto hit_count = 0;
#endif
    auto usage_ratio = (capacity == 0L) ? 0.0 : double(usage) / double(capacity);
    auto hit_ratio = (lookup_count == 0L) ? 0.0 : double(hit_count) / double(lookup_count);
    auto dynamic_lookup_count = lookup_count - _vector_index_cache_metrics->_previous_lookup_count;
    auto dynamic_hit_count = hit_count - _vector_index_cache_metrics->_previous_hit_count;
    auto dynamic_hit_ratio =
            (dynamic_lookup_count == 0) ? 0.0 : double(dynamic_hit_count) / double(dynamic_lookup_count);
    _vector_index_cache_metrics->vector_index_cache_capacity.set_value(capacity);
    _vector_index_cache_metrics->vector_index_cache_usage.set_value(usage);
    _vector_index_cache_metrics->vector_index_cache_usage_ratio.set_value(usage_ratio);
    _vector_index_cache_metrics->vector_index_cache_lookup_count.set_value(lookup_count);
    _vector_index_cache_metrics->vector_index_cache_hit_count.set_value(hit_count);
    _vector_index_cache_metrics->vector_index_cache_hit_ratio.set_value(hit_ratio);
    _vector_index_cache_metrics->vector_index_cache_dynamic_lookup_count.set_value(dynamic_lookup_count);
    _vector_index_cache_metrics->vector_index_cache_dynamic_hit_count.set_value(dynamic_hit_count);
    _vector_index_cache_metrics->vector_index_cache_dynamic_hit_ratio.set_value(dynamic_hit_ratio);

    _vector_index_cache_metrics->_previous_lookup_count = lookup_count;
    _vector_index_cache_metrics->_previous_hit_count = hit_count;
}

} // namespace starrocks
