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

#include <vector>

#include "base/metrics.h"
#include "gutil/macros.h"

namespace google::protobuf {
class MethodDescriptor;
class ServiceDescriptor;
} // namespace google::protobuf

namespace starrocks {

class ServiceMetrics {
public:
    struct RpcMethodMetrics {
        RpcMethodMetrics() = default;
        // `MetricRegistry::register_metric` registers metrics by addresses, so they must never be copied or moved.
        DISALLOW_COPY_AND_MOVE(RpcMethodMetrics);

        METRIC_DEFINE_INT_COUNTER(request_bytes, MetricUnit::BYTES);
        METRIC_DEFINE_INT_COUNTER(response_bytes, MetricUnit::BYTES);
    };

    ServiceMetrics() = default;
    explicit ServiceMetrics(MetricRegistry* registry) { install(registry); }
    ~ServiceMetrics() = default;

    static ServiceMetrics* instance();

    void install(MetricRegistry* registry);

    // Returns the payload counters of `method`, or nullptr when the metrics have not been installed
    // yet or `method` does not belong to PInternalService.
    RpcMethodMetrics* internal_service_rpc_metrics(const google::protobuf::MethodDescriptor* method);

    METRIC_DEFINE_INT_GAUGE(build_info, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_COUNTER(short_circuit_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(short_circuit_request_duration_us, MetricUnit::MICROSECONDS);

private:
    void _install_internal_service_rpc_metrics(MetricRegistry* registry);

    MetricRegistry* _registry = nullptr;
    const google::protobuf::ServiceDescriptor* _internal_service_descriptor = nullptr;
    // Indexed by google::protobuf::MethodDescriptor::index(). Built once at its final size.
    std::vector<RpcMethodMetrics> _internal_service_rpc_metrics;
};

} // namespace starrocks
