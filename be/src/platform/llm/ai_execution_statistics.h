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

#include <algorithm>
#include <cstdint>
#include <limits>

namespace starrocks {

// A task-private accumulator, published by value at terminal completion. It
// contains no query, operator, provider identity, or request/response payload.
// Usage counts distinguish a missing field from an explicitly reported zero.
struct AIExecutionStatistics {
    int64_t task_count = 0;
    int64_t request_count = 0;
    int64_t retry_count = 0;
    int64_t timeout_count = 0;
    int64_t error_count = 0;
    int64_t http_time_ns = 0;
    int64_t prompt_tokens = 0;
    int64_t completion_tokens = 0;
    int64_t total_tokens = 0;
    int64_t prompt_usage_count = 0;
    int64_t completion_usage_count = 0;
    int64_t total_usage_count = 0;

    void add(const AIExecutionStatistics& other) noexcept {
        task_count = saturated_add(task_count, other.task_count);
        request_count = saturated_add(request_count, other.request_count);
        retry_count = saturated_add(retry_count, other.retry_count);
        timeout_count = saturated_add(timeout_count, other.timeout_count);
        error_count = saturated_add(error_count, other.error_count);
        http_time_ns = saturated_add(http_time_ns, other.http_time_ns);
        prompt_tokens = saturated_add(prompt_tokens, other.prompt_tokens);
        completion_tokens = saturated_add(completion_tokens, other.completion_tokens);
        total_tokens = saturated_add(total_tokens, other.total_tokens);
        prompt_usage_count = saturated_add(prompt_usage_count, other.prompt_usage_count);
        completion_usage_count = saturated_add(completion_usage_count, other.completion_usage_count);
        total_usage_count = saturated_add(total_usage_count, other.total_usage_count);
    }

    bool empty() const noexcept {
        return task_count == 0 && request_count == 0 && retry_count == 0 && timeout_count == 0 && error_count == 0 &&
               http_time_ns == 0 && prompt_tokens == 0 && completion_tokens == 0 && total_tokens == 0 &&
               prompt_usage_count == 0 && completion_usage_count == 0 && total_usage_count == 0;
    }

private:
    static int64_t saturated_add(int64_t left, int64_t right) noexcept {
        left = std::max<int64_t>(0, left);
        right = std::max<int64_t>(0, right);
        const int64_t maximum = std::numeric_limits<int64_t>::max();
        return left > maximum - right ? maximum : left + right;
    }
};

} // namespace starrocks
