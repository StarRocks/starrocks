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

#include <cstdint>
#include <string>

#include "base/metrics.h"
#include "common/util/table_metrics.h"

namespace starrocks {

// ProcessMetricsRegistry is the dependency-neutral owner for BE/CN process metric registries.
// Concrete module metric groups are installed by higher-level composition code.
class ProcessMetricsRegistry {
public:
    explicit ProcessMetricsRegistry(std::string root_registry_name);
    ~ProcessMetricsRegistry() = default;

    MetricRegistry* root_registry() { return &_root_registry; }
    TableMetricsManager* table_metrics_mgr() { return &_table_metrics_mgr; }
    TableMetricsPtr table_metrics(uint64_t table_id) { return _table_metrics_mgr.get_table_metrics(table_id); }

    void collect_root(MetricsVisitor* visitor) { _root_registry.collect(visitor); }
    void collect_table(MetricsVisitor* visitor) { _table_metrics_mgr.metric_registry()->collect(visitor); }

private:
    MetricRegistry _root_registry;
    TableMetricsManager _table_metrics_mgr;
};

} // namespace starrocks
