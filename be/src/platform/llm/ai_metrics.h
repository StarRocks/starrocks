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

// Process-wide AI transport counters. An accepted attempt is one for which AIHttpClient::submit returns OK.
class AIMetrics {
public:
    AIMetrics() = default;
    explicit AIMetrics(MetricRegistry* registry) { install(registry); }
    ~AIMetrics() = default;

    static AIMetrics* instance();

    void install(MetricRegistry* registry);

    // Records one accepted HTTP attempt. Retries are the accepted attempts after the initial attempt.
    void record_accepted_attempt(bool retry) {
        ai_http_requests_total.increment(1);
        if (retry) ai_http_retries_total.increment(1);
    }

    // Records one accepted attempt that terminates because its transport or request/query deadline expires.
    void record_timeout() { ai_http_timeouts_total.increment(1); }

    METRIC_DEFINE_INT_COUNTER(ai_http_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(ai_http_retries_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(ai_http_timeouts_total, MetricUnit::REQUESTS);

private:
    MetricRegistry* _registry = nullptr;
};

} // namespace starrocks
