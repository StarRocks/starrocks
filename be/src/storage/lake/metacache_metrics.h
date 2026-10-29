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

#include <array>
#include <memory>

#include "base/metrics.h"
#include "storage/lake/metacache.h"

namespace starrocks::lake {

class MetacacheMetrics {
public:
    MetacacheMetrics() = default;
    ~MetacacheMetrics();

    static MetacacheMetrics* instance();
    void install(MetricRegistry* registry);
    void refresh(const Metacache* cache);

    METRIC_DEFINE_INT_GAUGE(lake_metacache_usage_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(lake_metacache_capacity_bytes, MetricUnit::BYTES);

private:
    MetricRegistry* _registry = nullptr;
    std::array<std::unique_ptr<IntGauge>, kMetacacheTypes.size()> _payload_bytes;
};

} // namespace starrocks::lake
