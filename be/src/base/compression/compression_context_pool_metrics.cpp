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

#include "base/compression/compression_context_pool_metrics.h"

#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "base/logging.h"
#include "base/metrics.h"

namespace starrocks::compression {

namespace {

struct PoolMetricEntry {
    PoolMetricEntry(std::string metric_name, const std::atomic<size_t>* created_counter)
            : metric_name(std::move(metric_name)), created_counter(created_counter) {}

    std::string metric_name;
    const std::atomic<size_t>* created_counter;
    std::unique_ptr<UIntGauge> gauge;
    bool installed = false;
};

struct PoolMetricState {
    std::mutex mutex;
    MetricRegistry* metrics = nullptr;
    std::vector<std::unique_ptr<PoolMetricEntry>> entries;
};

PoolMetricState& pool_metric_state() {
    // Avoid static destruction ordering problems between pool singletons and the metrics registry.
    static auto* state = new PoolMetricState();
    return *state;
}

void install_entry_locked(PoolMetricState* state, PoolMetricEntry* entry) {
    if (state->metrics == nullptr || entry->installed) {
        return;
    }

    entry->gauge = std::make_unique<UIntGauge>(MetricUnit::NOUNIT);
    auto* gauge = entry->gauge.get();

    bool metric_registered = state->metrics->register_metric(entry->metric_name, gauge);
    DCHECK(metric_registered) << "duplicate compression context pool metric: " << entry->metric_name;
    if (!metric_registered) {
        entry->gauge.reset();
        return;
    }

    bool hook_registered = state->metrics->register_hook(
            entry->metric_name, [gauge, entry]() { gauge->set_value(entry->created_counter->load()); });
    DCHECK(hook_registered) << "duplicate compression context pool metric hook: " << entry->metric_name;
    if (!hook_registered) {
        gauge->hide();
        entry->gauge.reset();
        return;
    }

    entry->installed = true;
}

} // namespace

void register_compression_context_pool_metric(const std::string& pool_name,
                                              const std::atomic<size_t>* created_counter) {
    DCHECK(created_counter != nullptr);
    auto& state = pool_metric_state();
    std::lock_guard<std::mutex> lock(state.mutex);
    auto entry = std::make_unique<PoolMetricEntry>(pool_name + "_context_pool_create_count", created_counter);
    auto* entry_ptr = entry.get();
    state.entries.emplace_back(std::move(entry));
    install_entry_locked(&state, entry_ptr);
}

void install_compression_context_pool_metrics(MetricRegistry* metrics) {
    if (metrics == nullptr) {
        return;
    }

    auto& state = pool_metric_state();
    std::lock_guard<std::mutex> lock(state.mutex);
    if (state.metrics != nullptr) {
        DCHECK(state.metrics == metrics);
        return;
    }

    state.metrics = metrics;
    for (const auto& entry : state.entries) {
        install_entry_locked(&state, entry.get());
    }
}

} // namespace starrocks::compression
