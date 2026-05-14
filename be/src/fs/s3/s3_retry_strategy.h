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

#include <aws/core/client/RetryStrategy.h>

namespace starrocks {

// Enhanced S3 retry strategy with global throttling.
// Features:
// - Exponential backoff with jitter (prevents thundering herd)
// - Global throttling: when one request gets 429/503, all threads slow down
// - Smart retry decisions (no retry for permanent redirects)
// - Handles both AWS and TOS (ByteDance) error codes
class S3RetryStrategy : public Aws::Client::RetryStrategy {
public:
    struct Config {
        unsigned int max_retries = 10;
        unsigned int initial_delay_ms = 25;
        unsigned int max_delay_ms = 10000;
    };

    explicit S3RetryStrategy(const Config& config);
    ~S3RetryStrategy() override = default;

    // Aws::Client::RetryStrategy interface
    bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const override;

    long CalculateDelayBeforeNextRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
                                       long attemptedRetries) const override;

    long GetMaxAttempts() const override;

    void RequestBookkeeping(const Aws::Client::HttpResponseOutcome& outcome) override;
    void RequestBookkeeping(const Aws::Client::HttpResponseOutcome& outcome,
                            const Aws::Client::AWSError<Aws::Client::CoreErrors>& lastError) override;

private:
    bool is_retryable_error(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error) const;
    bool is_throttling_error(int status_code) const;

    Config _config;
};

} // namespace starrocks
