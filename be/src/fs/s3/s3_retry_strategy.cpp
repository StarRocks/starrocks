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

#include "fs/s3/s3_retry_strategy.h"

#include <aws/core/client/AWSError.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/Outcome.h>

#include <algorithm>
#include <random>

#include "common/logging.h"
#include "io/s3_global_throttle.h"

namespace starrocks {

S3RetryStrategy::S3RetryStrategy(const Config& config) : _config(config) {}

bool S3RetryStrategy::ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
                                  long attemptedRetries) const {
    // Don't retry permanent redirects (301)
    if (error.GetResponseCode() == Aws::Http::HttpResponseCode::MOVED_PERMANENTLY) {
        return false;
    }

    // Max retries exceeded
    if (attemptedRetries >= static_cast<long>(_config.max_retries)) {
        return false;
    }

    // Check if error is retryable
    return is_retryable_error(error);
}

bool S3RetryStrategy::is_retryable_error(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error) const {
    // Standard AWS retryable errors
    if (error.ShouldRetry()) {
        return true;
    }

    int status = static_cast<int>(error.GetResponseCode());

    // Throttling errors (429, 503)
    if (is_throttling_error(status)) {
        return true;
    }

    // Network errors
    if (error.GetErrorType() == Aws::Client::CoreErrors::NETWORK_CONNECTION) {
        return true;
    }

    // TOS-specific errors (ByteDance)
    const auto& name = error.GetExceptionName();
    if (name == "ExceedAccountQPSLimit" || name == "ExceedAccountRateLimit" || name == "ExceedBucketQPSLimit" ||
        name == "ExceedBucketRateLimit") {
        return true;
    }

    return false;
}

bool S3RetryStrategy::is_throttling_error(int status_code) const {
    return status_code == 429 || // Too Many Requests
           status_code == 503 || // Service Unavailable
           status_code == 509;   // Bandwidth Limit Exceeded
}

long S3RetryStrategy::CalculateDelayBeforeNextRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
                                                    long attemptedRetries) const {
    // Exponential backoff: initial_delay * 2^attempts
    uint64_t backoff_pow = 1UL << std::clamp(attemptedRetries, 0L, 31L);
    uint64_t delay_ms = _config.initial_delay_ms * backoff_pow;

    // Add 10% jitter to prevent thundering herd
    // Use thread-local RNG to avoid data races when multiple threads retry concurrently
    constexpr double kJitterFactor = 0.1;
    thread_local std::mt19937 tl_rng{std::random_device{}()};
    std::uniform_real_distribution<double> dist(1.0, 1.0 + kJitterFactor);
    delay_ms = static_cast<uint64_t>(delay_ms * dist(tl_rng));

    // Cap at max delay
    delay_ms = std::min(delay_ms, static_cast<uint64_t>(_config.max_delay_ms));

    // Update global throttle so all threads slow down on throttling errors
    if (is_throttling_error(static_cast<int>(error.GetResponseCode()))) {
        io::S3GlobalThrottle::record_backoff(delay_ms);
    }

    VLOG(12) << "S3 retry in " << delay_ms << " ms (attempt " << attemptedRetries + 1 << "/" << _config.max_retries
             << ")";

    return static_cast<long>(delay_ms);
}

long S3RetryStrategy::GetMaxAttempts() const {
    return _config.max_retries + 1;
}

void S3RetryStrategy::RequestBookkeeping(const Aws::Client::HttpResponseOutcome& outcome) {
    if (!outcome.IsSuccess()) {
        const auto& error = outcome.GetError();
        if (error.ShouldRetry()) {
            LOG(WARNING) << "S3 request failed with retryable error: " << static_cast<int>(error.GetResponseCode())
                         << " " << error.GetMessage();
        }
    }
}

void S3RetryStrategy::RequestBookkeeping(const Aws::Client::HttpResponseOutcome& outcome,
                                         const Aws::Client::AWSError<Aws::Client::CoreErrors>& lastError) {
    RequestBookkeeping(outcome);
}

} // namespace starrocks
