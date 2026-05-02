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

#include <memory>

#include "base/metrics.h"

namespace starrocks {

// Server-level spill counters aggregated across all spillable operators.
// Only split by storage_type (local vs. remote); per-operator breakdown
// already lives in each operator's RuntimeProfile.
class SpillMetrics {
public:
    struct LabeledCounters {
        std::unique_ptr<IntCounter> trigger_total;
        std::unique_ptr<IntCounter> bytes_write_total;
        std::unique_ptr<IntCounter> bytes_read_total;
        std::unique_ptr<IntCounter> blocks_write_total;
        std::unique_ptr<IntCounter> blocks_read_total;
        std::unique_ptr<IntCounter> write_io_duration_ns_total;
        std::unique_ptr<IntCounter> read_io_duration_ns_total;
    };

    SpillMetrics() = default;
    explicit SpillMetrics(MetricRegistry* registry) { install(registry); }
    ~SpillMetrics() = default;

    static SpillMetrics* instance();

    void install(MetricRegistry* registry);

    LabeledCounters* get(bool is_remote) { return _registry == nullptr ? nullptr : (is_remote ? &_remote : &_local); }

    IntGauge* local_disk_bytes_used() { return _local_disk_bytes_used.get(); }
    IntGauge* remote_disk_bytes_used() { return _remote_disk_bytes_used.get(); }

private:
    LabeledCounters _local;
    LabeledCounters _remote;
    std::unique_ptr<IntGauge> _local_disk_bytes_used;
    std::unique_ptr<IntGauge> _remote_disk_bytes_used;
    MetricRegistry* _registry = nullptr;
};

} // namespace starrocks
