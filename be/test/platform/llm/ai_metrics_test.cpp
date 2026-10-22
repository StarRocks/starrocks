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

#include "platform/llm/ai_metrics.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

namespace {

void assert_metric_value(MetricRegistry* registry, const std::string& name, const std::string& value) {
    auto* metric = registry->get_metric(name);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

} // namespace

TEST(AIMetricsTest, InstallRegistersAcceptedAttemptCounters) {
    MetricRegistry registry("test_registry");
    AIMetrics metrics(&registry);
    metrics.install(&registry);

    metrics.record_accepted_attempt(false);
    metrics.record_accepted_attempt(true);
    metrics.record_timeout();

    assert_metric_value(&registry, "ai_http_requests_total", "2");
    assert_metric_value(&registry, "ai_http_retries_total", "1");
    assert_metric_value(&registry, "ai_http_timeouts_total", "1");
}

} // namespace starrocks
