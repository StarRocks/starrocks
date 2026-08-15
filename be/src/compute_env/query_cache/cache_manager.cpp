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

#include "compute_env/query_cache/cache_manager.h"

#include "base/metrics.h"
#include "base/utility/defer_op.h"
#include "compute_env/query_cache/query_cache_metrics.h"

namespace starrocks::query_cache {

namespace {
const char* const kQueryCacheMetricsHookName = "query_cache_metrics";
} // namespace

CacheManager::CacheManager(size_t capacity) : _cache(capacity) {}

CacheManager::~CacheManager() {
    if (_metrics_registry != nullptr) {
        _metrics_registry->deregister_hook(kQueryCacheMetricsHookName);
    }
}

static void delete_cache_entry(const CacheKey& key, void* value) {
    auto* cache_value = (CacheValue*)value;
    delete cache_value;
}

Status CacheManager::install_metrics(MetricRegistry* registry) {
    if (registry == nullptr) {
        return Status::OK();
    }
    if (_metrics_registry != nullptr) {
        DCHECK_EQ(_metrics_registry, registry);
        return Status::OK();
    }

    auto metrics = std::make_unique<QueryCacheMetrics>();
    if (!metrics->install(registry)) {
        return Status::InternalError("register query cache metrics failed");
    }
    if (!registry->register_hook(kQueryCacheMetricsHookName, [this] { _update_metrics(); })) {
        metrics->hide();
        return Status::InternalError("register query cache metrics hook failed");
    }

    _metrics = std::move(metrics);
    _metrics_registry = registry;
    _update_metrics();
    return Status::OK();
}

void CacheManager::populate(const std::string& key, const CacheValue& value) {
    auto* cache_value = new CacheValue(value);
    size_t value_size = cache_value->size();
    auto* handle = _cache.insert(key, cache_value, value_size, &delete_cache_entry, CachePriority::NORMAL);
    _cache.release(handle);
}

StatusOr<CacheValue> CacheManager::probe(const std::string& key) {
    auto* handle = _cache.lookup(key);
    if (handle == nullptr) {
        return Status::NotFound("CacheMiss");
    }
    DeferOp defer([this, handle]() { _cache.release(handle); });
    CacheValue cache_value(*reinterpret_cast<CacheValue*>(_cache.value(handle)));
    return cache_value;
}

size_t CacheManager::memory_usage() {
    return _cache.get_memory_usage();
}

size_t CacheManager::capacity() {
    return _cache.get_capacity();
}

size_t CacheManager::lookup_count() {
    return _cache.get_lookup_count();
}

size_t CacheManager::hit_count() {
    return _cache.get_hit_count();
}

void CacheManager::invalidate_all() {
    auto old_capacity = _cache.get_capacity();
    // set capacity of cache to zero, the cache shall prune all cache entries.
    _cache.set_capacity(0);
    _cache.set_capacity(old_capacity);
}

void CacheManager::_update_metrics() {
    if (_metrics != nullptr) {
        _metrics->update(capacity(), memory_usage(), lookup_count(), hit_count());
    }
}

} // namespace starrocks::query_cache
