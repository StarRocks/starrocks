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

#include "gutil/macros.h"

namespace starrocks {

StreamLoadMetrics* StreamLoadMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new StreamLoadMetrics();
    return instance;
}

void StreamLoadMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;

    registry->register_metric("txn_request", MetricLabels().add("type", "begin"), &txn_begin_request_total);
    registry->register_metric("txn_request", MetricLabels().add("type", "commit"), &txn_commit_request_total);
    registry->register_metric("txn_request", MetricLabels().add("type", "rollback"), &txn_rollback_request_total);
    registry->register_metric("txn_request", MetricLabels().add("type", "exec"), &txn_exec_plan_total);

    registry->register_metric("stream_load", MetricLabels().add("type", "receive_bytes"), &stream_receive_bytes_total);
    registry->register_metric("stream_load", MetricLabels().add("type", "load_rows"), &stream_load_rows_total);
    registry->register_metric("load_rows", &load_rows_total);
    registry->register_metric("load_bytes", &load_bytes_total);

    registry->register_metric("streaming_load_requests_total", &streaming_load_requests_total);
    registry->register_metric("streaming_load_bytes", &streaming_load_bytes);
    registry->register_metric("streaming_load_duration_ms", &streaming_load_duration_ms);
    registry->register_metric("streaming_load_current_processing", &streaming_load_current_processing);

    registry->register_metric("transaction_streaming_load_requests_total", &transaction_streaming_load_requests_total);
    registry->register_metric("transaction_streaming_load_bytes", &transaction_streaming_load_bytes);
    registry->register_metric("transaction_streaming_load_duration_ms", &transaction_streaming_load_duration_ms);
    registry->register_metric("transaction_streaming_load_current_processing",
                              &transaction_streaming_load_current_processing);
}

} // namespace starrocks
