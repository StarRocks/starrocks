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

#include "orchestration/orchestration_metrics.h"

#include <gtest/gtest.h>

#include <atomic>

#include "runtime/runtime_filter_worker_event.h"

namespace starrocks::orchestration {

namespace {

MetricLabels runtime_filter_event_labels(EventType type) {
    return MetricLabels().add("type", EventTypeToString(type));
}

Metric* runtime_filter_events_metric(MetricRegistry* registry, EventType type) {
    return registry->get_metric("runtime_filter_events_in_queue", runtime_filter_event_labels(type));
}

Metric* runtime_filter_bytes_metric(MetricRegistry* registry, EventType type) {
    return registry->get_metric("runtime_filter_bytes_in_queue", runtime_filter_event_labels(type));
}

} // namespace

TEST(OrchestrationMetricsTest, RegistersRuntimeFilterMetricsForEveryEventType) {
    MetricRegistry registry("test_registry");
    RuntimeFilterWorkerMetrics runtime_filter_metrics;
    OrchestrationMetrics metrics;

    metrics.install(&registry, [&runtime_filter_metrics] { return &runtime_filter_metrics; });

    for (int i = 0; i < EventType::MAX_COUNT; i++) {
        auto type = static_cast<EventType>(i);
        auto* events_metric = runtime_filter_events_metric(&registry, type);
        ASSERT_NE(nullptr, events_metric) << EventTypeToString(type);
        EXPECT_EQ(MetricType::GAUGE, events_metric->type());
        EXPECT_EQ(MetricUnit::NOUNIT, events_metric->unit());

        auto* bytes_metric = runtime_filter_bytes_metric(&registry, type);
        ASSERT_NE(nullptr, bytes_metric) << EventTypeToString(type);
        EXPECT_EQ(MetricType::GAUGE, bytes_metric->type());
        EXPECT_EQ(MetricUnit::BYTES, bytes_metric->unit());
    }
}

TEST(OrchestrationMetricsTest, UpdatesRuntimeFilterMetricsFromProvider) {
    MetricRegistry registry("test_registry");
    RuntimeFilterWorkerMetrics runtime_filter_metrics;
    OrchestrationMetrics metrics;
    metrics.install(&registry, [&runtime_filter_metrics] { return &runtime_filter_metrics; });

    runtime_filter_metrics.event_nums[RECEIVE_TOTAL_RF].store(7, std::memory_order_relaxed);
    runtime_filter_metrics.runtime_filter_bytes[RECEIVE_TOTAL_RF].store(2048, std::memory_order_relaxed);
    runtime_filter_metrics.event_nums[SEND_PART_RF].store(3, std::memory_order_relaxed);
    runtime_filter_metrics.runtime_filter_bytes[SEND_PART_RF].store(4096, std::memory_order_relaxed);

    registry.trigger_hook();

    EXPECT_STREQ("7", runtime_filter_events_metric(&registry, RECEIVE_TOTAL_RF)->to_string().c_str());
    EXPECT_STREQ("2048", runtime_filter_bytes_metric(&registry, RECEIVE_TOTAL_RF)->to_string().c_str());
    EXPECT_STREQ("3", runtime_filter_events_metric(&registry, SEND_PART_RF)->to_string().c_str());
    EXPECT_STREQ("4096", runtime_filter_bytes_metric(&registry, SEND_PART_RF)->to_string().c_str());
}

TEST(OrchestrationMetricsTest, DeregistersRuntimeFilterMetricsOnDestruction) {
    MetricRegistry registry("test_registry");
    RuntimeFilterWorkerMetrics runtime_filter_metrics;

    {
        OrchestrationMetrics metrics;
        metrics.install(&registry, [&runtime_filter_metrics] { return &runtime_filter_metrics; });
        ASSERT_NE(nullptr, runtime_filter_events_metric(&registry, RECEIVE_TOTAL_RF));
        ASSERT_NE(nullptr, runtime_filter_bytes_metric(&registry, RECEIVE_TOTAL_RF));
    }

    EXPECT_EQ(nullptr, runtime_filter_events_metric(&registry, RECEIVE_TOTAL_RF));
    EXPECT_EQ(nullptr, runtime_filter_bytes_metric(&registry, RECEIVE_TOTAL_RF));
    registry.trigger_hook();
}

} // namespace starrocks::orchestration
