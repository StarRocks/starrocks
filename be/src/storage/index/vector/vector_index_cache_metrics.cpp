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

#include "storage/index/vector/vector_index_cache_metrics.h"

namespace starrocks {

namespace {

const char* const kVectorIndexCacheMetricsHookName = "vector_index_cache_metrics";

} // namespace

VectorIndexCacheMetrics* VectorIndexCacheMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new VectorIndexCacheMetrics();
    return instance;
}

VectorIndexCacheMetrics::~VectorIndexCacheMetrics() {
    hide();
}

bool VectorIndexCacheMetrics::install(MetricRegistry* registry) {
    DCHECK(registry != nullptr);
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return _registry == registry;
    }

    if (!registry->register_metric("vector_index_cache_capacity", &vector_index_cache_capacity) ||
        !registry->register_metric("vector_index_cache_usage", &vector_index_cache_usage) ||
        !registry->register_metric("vector_index_cache_usage_ratio", &vector_index_cache_usage_ratio) ||
        !registry->register_metric("vector_index_cache_lookup_count", &vector_index_cache_lookup_count) ||
        !registry->register_metric("vector_index_cache_hit_count", &vector_index_cache_hit_count) ||
        !registry->register_metric("vector_index_cache_hit_ratio", &vector_index_cache_hit_ratio) ||
        !registry->register_metric("vector_index_cache_dynamic_lookup_count",
                                   &vector_index_cache_dynamic_lookup_count) ||
        !registry->register_metric("vector_index_cache_dynamic_hit_count", &vector_index_cache_dynamic_hit_count) ||
        !registry->register_metric("vector_index_cache_dynamic_hit_ratio", &vector_index_cache_dynamic_hit_ratio) ||
        !registry->register_hook(kVectorIndexCacheMetricsHookName, [this] { refresh(); })) {
        hide();
        return false;
    }

    _registry = registry;
    return true;
}

void VectorIndexCacheMetrics::hide() {
    if (_registry != nullptr) {
        _registry->deregister_hook(kVectorIndexCacheMetricsHookName);
        _registry = nullptr;
    }
    vector_index_cache_capacity.hide();
    vector_index_cache_usage.hide();
    vector_index_cache_usage_ratio.hide();
    vector_index_cache_lookup_count.hide();
    vector_index_cache_hit_count.hide();
    vector_index_cache_hit_ratio.hide();
    vector_index_cache_dynamic_lookup_count.hide();
    vector_index_cache_dynamic_hit_count.hide();
    vector_index_cache_dynamic_hit_ratio.hide();
}

void VectorIndexCacheMetrics::update(size_t capacity, size_t usage, uint64_t lookup_count, uint64_t hit_count) {
    _capacity.store(capacity, std::memory_order_relaxed);
    _usage.store(usage, std::memory_order_relaxed);
    _lookup_count.store(lookup_count, std::memory_order_relaxed);
    _hit_count.store(hit_count, std::memory_order_relaxed);
}

void VectorIndexCacheMetrics::refresh() {
    std::lock_guard lock(_refresh_mutex);

    const auto capacity = _capacity.load(std::memory_order_relaxed);
    const auto usage = _usage.load(std::memory_order_relaxed);
    const auto lookup_count = _lookup_count.load(std::memory_order_relaxed);
    const auto hit_count = _hit_count.load(std::memory_order_relaxed);
    const auto dynamic_lookup_count = _delta_since_last_refresh(lookup_count, _previous_lookup_count);
    const auto dynamic_hit_count = _delta_since_last_refresh(hit_count, _previous_hit_count);
    const auto usage_ratio = (capacity == 0) ? 0.0 : static_cast<double>(usage) / static_cast<double>(capacity);
    const auto hit_ratio =
            (lookup_count == 0) ? 0.0 : static_cast<double>(hit_count) / static_cast<double>(lookup_count);
    const auto dynamic_hit_ratio = (dynamic_lookup_count == 0) ? 0.0
                                                               : static_cast<double>(dynamic_hit_count) /
                                                                         static_cast<double>(dynamic_lookup_count);

    vector_index_cache_capacity.set_value(static_cast<int64_t>(capacity));
    vector_index_cache_usage.set_value(static_cast<int64_t>(usage));
    vector_index_cache_usage_ratio.set_value(usage_ratio);
    vector_index_cache_lookup_count.set_value(static_cast<int64_t>(lookup_count));
    vector_index_cache_hit_count.set_value(static_cast<int64_t>(hit_count));
    vector_index_cache_hit_ratio.set_value(hit_ratio);
    vector_index_cache_dynamic_lookup_count.set_value(static_cast<int64_t>(dynamic_lookup_count));
    vector_index_cache_dynamic_hit_count.set_value(static_cast<int64_t>(dynamic_hit_count));
    vector_index_cache_dynamic_hit_ratio.set_value(dynamic_hit_ratio);

    _previous_lookup_count = lookup_count;
    _previous_hit_count = hit_count;
}

uint64_t VectorIndexCacheMetrics::_delta_since_last_refresh(uint64_t current, uint64_t previous) {
    return current >= previous ? current - previous : current;
}

} // namespace starrocks
