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

#include "runtime/stream_load/stream_load_metrics.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

namespace {

void assert_metric_value(MetricRegistry* registry, const std::string& name, const std::string& value) {
    auto* metric = registry->get_metric(name);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

void assert_metric_value(MetricRegistry* registry, const std::string& name, const MetricLabels& labels,
                         const std::string& value) {
    auto* metric = registry->get_metric(name, labels);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

} // namespace

TEST(StreamLoadMetricsTest, InstallRegistersTxnAndLoadMetrics) {
    MetricRegistry registry("test_registry");
    StreamLoadMetrics metrics(&registry);
    metrics.install(&registry);

    metrics.txn_begin_request_total.increment(3);
    assert_metric_value(&registry, "txn_request", MetricLabels().add("type", "begin"), "3");

    metrics.txn_commit_request_total.increment(4);
    assert_metric_value(&registry, "txn_request", MetricLabels().add("type", "commit"), "4");

    metrics.txn_rollback_request_total.increment(5);
    assert_metric_value(&registry, "txn_request", MetricLabels().add("type", "rollback"), "5");

    metrics.txn_exec_plan_total.increment(6);
    assert_metric_value(&registry, "txn_request", MetricLabels().add("type", "exec"), "6");

    metrics.stream_receive_bytes_total.increment(7);
    assert_metric_value(&registry, "stream_load", MetricLabels().add("type", "receive_bytes"), "7");

    metrics.stream_load_rows_total.increment(8);
    assert_metric_value(&registry, "stream_load", MetricLabels().add("type", "load_rows"), "8");

    metrics.load_rows_total.increment(9);
    assert_metric_value(&registry, "load_rows", "9");

    metrics.load_bytes_total.increment(10);
    assert_metric_value(&registry, "load_bytes", "10");
}

TEST(StreamLoadMetricsTest, InstallRegistersHttpStreamLoadMetrics) {
    MetricRegistry registry("test_registry");
    StreamLoadMetrics metrics(&registry);

    metrics.streaming_load_requests_total.increment(11);
    assert_metric_value(&registry, "streaming_load_requests_total", "11");

    metrics.streaming_load_bytes.increment(12);
    assert_metric_value(&registry, "streaming_load_bytes", "12");

    metrics.streaming_load_duration_ms.increment(13);
    assert_metric_value(&registry, "streaming_load_duration_ms", "13");

    metrics.streaming_load_current_processing.set_value(14);
    assert_metric_value(&registry, "streaming_load_current_processing", "14");
}

TEST(StreamLoadMetricsTest, InstallRegistersTransactionStreamLoadMetrics) {
    MetricRegistry registry("test_registry");
    StreamLoadMetrics metrics(&registry);

    metrics.transaction_streaming_load_requests_total.increment(15);
    assert_metric_value(&registry, "transaction_streaming_load_requests_total", "15");

    metrics.transaction_streaming_load_bytes.increment(16);
    assert_metric_value(&registry, "transaction_streaming_load_bytes", "16");

    metrics.transaction_streaming_load_duration_ms.increment(17);
    assert_metric_value(&registry, "transaction_streaming_load_duration_ms", "17");

    metrics.transaction_streaming_load_current_processing.set_value(18);
    assert_metric_value(&registry, "transaction_streaming_load_current_processing", "18");
}

} // namespace starrocks
