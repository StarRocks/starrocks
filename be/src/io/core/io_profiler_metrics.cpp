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

#include "io/core/io_profiler_metrics.h"

#include <string>

#include "base/metrics.h"
#include "io/core/io_profiler.h"

namespace starrocks {

class IOProfilerMetrics::IOMetrics {
public:
    METRIC_DEFINE_INT_GAUGE(read_ops, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(read_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(write_ops, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(write_bytes, MetricUnit::BYTES);
};

IOProfilerMetrics::~IOProfilerMetrics() {
    for (auto* metrics : _io_metrics) {
        delete metrics;
    }
}

IOProfilerMetrics* IOProfilerMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new IOProfilerMetrics();
    return instance;
}

void IOProfilerMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }

    for (uint32_t i = 0; i < IOProfiler::TAG::TAG_END; i++) {
        std::string tag_name = IOProfiler::tag_to_string(i);
        auto* metrics = new IOMetrics();
#define REGISTER_IO_METRIC(name) \
    registry->register_metric("io_" #name, MetricLabels().add("tag", tag_name), &metrics->name);
        REGISTER_IO_METRIC(read_ops);
        REGISTER_IO_METRIC(read_bytes);
        REGISTER_IO_METRIC(write_ops);
        REGISTER_IO_METRIC(write_bytes);
#undef REGISTER_IO_METRIC

        _io_metrics.emplace_back(metrics);
    }
    _registry = registry;
}

void IOProfilerMetrics::record_read(uint32_t tag, int64_t bytes) {
    if (UNLIKELY(tag >= _io_metrics.size())) {
        return;
    }
    auto* metrics = _io_metrics[tag];
    metrics->read_ops.increment(1);
    metrics->read_bytes.increment(bytes);
}

void IOProfilerMetrics::record_write(uint32_t tag, int64_t bytes) {
    if (UNLIKELY(tag >= _io_metrics.size())) {
        return;
    }
    auto* metrics = _io_metrics[tag];
    metrics->write_ops.increment(1);
    metrics->write_bytes.increment(bytes);
}

} // namespace starrocks
