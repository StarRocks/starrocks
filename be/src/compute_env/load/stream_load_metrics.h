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

#include "base/metrics.h"

namespace starrocks {

class StreamLoadMetrics {
public:
    StreamLoadMetrics() = default;
    explicit StreamLoadMetrics(MetricRegistry* registry) { install(registry); }
    ~StreamLoadMetrics() = default;

    static StreamLoadMetrics* instance();

    void install(MetricRegistry* registry);

    METRIC_DEFINE_INT_COUNTER(txn_begin_request_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(txn_commit_request_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(txn_rollback_request_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(txn_exec_plan_total, MetricUnit::OPERATIONS);

    METRIC_DEFINE_INT_COUNTER(stream_receive_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(stream_load_rows_total, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(load_rows_total, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(load_bytes_total, MetricUnit::BYTES);

    METRIC_DEFINE_INT_COUNTER(streaming_load_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(streaming_load_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(streaming_load_duration_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_GAUGE(streaming_load_current_processing, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(transaction_streaming_load_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(transaction_streaming_load_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(transaction_streaming_load_duration_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_GAUGE(transaction_streaming_load_current_processing, MetricUnit::REQUESTS);

private:
    MetricRegistry* _registry = nullptr;
};

} // namespace starrocks
