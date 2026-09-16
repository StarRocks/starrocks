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

#include <cstddef>

#include "base/metrics.h"

namespace starrocks::query_cache {

class QueryCacheMetrics {
public:
    bool install(MetricRegistry* registry);
    void hide();
    void update(size_t capacity, size_t usage, size_t lookup_count, size_t hit_count);

    METRIC_DEFINE_INT_GAUGE(query_cache_capacity, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(query_cache_usage, MetricUnit::BYTES);
    METRIC_DEFINE_DOUBLE_GAUGE(query_cache_usage_ratio, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(query_cache_lookup_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(query_cache_hit_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_DOUBLE_GAUGE(query_cache_hit_ratio, MetricUnit::PERCENT);
};

} // namespace starrocks::query_cache
