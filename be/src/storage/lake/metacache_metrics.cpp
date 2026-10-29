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

#include "storage/lake/metacache_metrics.h"

#include "storage/lake/tablet_manager.h"
#include "storage/storage_env.h"

namespace starrocks::lake {

MetacacheMetrics* MetacacheMetrics::instance() {
    // Registered metrics have registry back-pointers; keep the process singleton
    // alive through registry teardown, as with the other storage metric owners.
    static auto* metrics = new MetacacheMetrics();
    return metrics;
}

MetacacheMetrics::~MetacacheMetrics() {
    if (_registry != nullptr) {
        _registry->deregister_hook("lake_metacache_metrics");
    }
}

void MetacacheMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;
    registry->register_metric("lake_metacache_usage_bytes", &lake_metacache_usage_bytes);
    registry->register_metric("lake_metacache_capacity_bytes", &lake_metacache_capacity_bytes);
    for (size_t i = 0; i < kMetacacheTypes.size(); ++i) {
        _payload_bytes[i] = std::make_unique<IntGauge>(MetricUnit::BYTES);
        registry->register_metric("lake_metacache_payload_bytes", MetricLabels().add("type", kMetacacheTypes[i]),
                                  _payload_bytes[i].get());
    }
    registry->register_hook("lake_metacache_metrics", [this] {
        auto* manager = StorageEnv::GetInstance()->lake_tablet_manager();
        refresh(manager != nullptr ? manager->metacache() : nullptr);
    });
}

void MetacacheMetrics::refresh(const Metacache* cache) {
    lake_metacache_usage_bytes.set_value(cache != nullptr ? cache->memory_usage() : 0);
    lake_metacache_capacity_bytes.set_value(cache != nullptr ? cache->capacity() : 0);
    const auto payload = cache != nullptr ? cache->payload_bytes() : MetacachePayloadBytes{};
    for (size_t i = 0; i < payload.size(); ++i) {
        _payload_bytes[i]->set_value(payload[i]);
    }
}

} // namespace starrocks::lake
