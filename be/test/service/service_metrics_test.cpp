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

#include "service/service_metrics.h"

#include <google/protobuf/descriptor.h>
#include <gtest/gtest.h>

#include <string>

#include "gen_cpp/internal_service.pb.h"
#include "gen_cpp/lake_service.pb.h"

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

TEST(ServiceMetricsTest, InstallRegistersServiceMetrics) {
    MetricRegistry registry("test_registry");
    ServiceMetrics metrics(&registry);
    metrics.install(&registry);

    metrics.short_circuit_request_total.increment(5);
    assert_metric_value(&registry, "short_circuit_request_total", "5");

    metrics.short_circuit_request_duration_us.increment(6);
    assert_metric_value(&registry, "short_circuit_request_duration_us", "6");
}

TEST(ServiceMetricsTest, InstallRegistersInternalServiceRpcPayloadMetrics) {
    MetricRegistry registry("test_registry");
    ServiceMetrics metrics(&registry);

    const auto* service = PInternalService::descriptor();
    for (int i = 0; i < service->method_count(); ++i) {
        const auto* method = service->method(i);
        auto* method_metrics = metrics.internal_service_rpc_metrics(method);
        ASSERT_NE(nullptr, method_metrics) << method->name();

        MetricLabels labels;
        labels.add("method", method->name());
        method_metrics->request_bytes.increment(i + 1);
        method_metrics->response_bytes.increment(2 * (i + 1));
        assert_metric_value(&registry, "internal_service_rpc_request_bytes", labels, std::to_string(i + 1));
        assert_metric_value(&registry, "internal_service_rpc_response_bytes", labels, std::to_string(2 * (i + 1)));
    }
}

TEST(ServiceMetricsTest, InternalServiceRpcMetricsIgnoreForeignMethods) {
    MetricRegistry registry("test_registry");
    ServiceMetrics metrics(&registry);

    ASSERT_EQ(nullptr, metrics.internal_service_rpc_metrics(nullptr));
    ASSERT_EQ(nullptr,
              metrics.internal_service_rpc_metrics(LakeService::descriptor()->FindMethodByName("publish_version")));
}

TEST(ServiceMetricsTest, InternalServiceRpcMetricsAreEmptyBeforeInstall) {
    ServiceMetrics metrics;
    ASSERT_EQ(nullptr,
              metrics.internal_service_rpc_metrics(PInternalService::descriptor()->FindMethodByName("get_info")));
}

} // namespace starrocks
