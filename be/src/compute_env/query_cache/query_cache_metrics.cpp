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

#include "compute_env/query_cache/query_cache_metrics.h"

namespace starrocks::query_cache {

bool QueryCacheMetrics::install(MetricRegistry* registry) {
    DCHECK(registry != nullptr);
    if (!registry->register_metric("query_cache_capacity", &query_cache_capacity) ||
        !registry->register_metric("query_cache_usage", &query_cache_usage) ||
        !registry->register_metric("query_cache_usage_ratio", &query_cache_usage_ratio) ||
        !registry->register_metric("query_cache_lookup_count", &query_cache_lookup_count) ||
        !registry->register_metric("query_cache_hit_count", &query_cache_hit_count) ||
        !registry->register_metric("query_cache_hit_ratio", &query_cache_hit_ratio)) {
        hide();
        return false;
    }
    return true;
}

void QueryCacheMetrics::hide() {
    query_cache_capacity.hide();
    query_cache_usage.hide();
    query_cache_usage_ratio.hide();
    query_cache_lookup_count.hide();
    query_cache_hit_count.hide();
    query_cache_hit_ratio.hide();
}

void QueryCacheMetrics::update(size_t capacity, size_t usage, size_t lookup_count, size_t hit_count) {
    const double usage_ratio = (capacity == 0) ? 0.0 : static_cast<double>(usage) / static_cast<double>(capacity);
    const double hit_ratio =
            (lookup_count == 0) ? 0.0 : static_cast<double>(hit_count) / static_cast<double>(lookup_count);

    query_cache_capacity.set_value(static_cast<int64_t>(capacity));
    query_cache_usage.set_value(static_cast<int64_t>(usage));
    query_cache_usage_ratio.set_value(usage_ratio);
    query_cache_lookup_count.set_value(static_cast<int64_t>(lookup_count));
    query_cache_hit_count.set_value(static_cast<int64_t>(hit_count));
    query_cache_hit_ratio.set_value(hit_ratio);
}

} // namespace starrocks::query_cache
