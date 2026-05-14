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
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <string>
#include <thread>

#include "io/s3_global_throttle.h"

namespace starrocks {

class S3RetryStrategyTest : public ::testing::Test {
protected:
    // Helper to create AWS error with specific HTTP status code
    static Aws::Client::AWSError<Aws::Client::CoreErrors> make_error(
            int http_code, Aws::Client::CoreErrors error_type = Aws::Client::CoreErrors::SERVICE_UNAVAILABLE,
            bool retryable = false) {
        Aws::Client::AWSError<Aws::Client::CoreErrors> error(error_type, retryable);
        error.SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(http_code));
        return error;
    }

    // Helper to create TOS-specific error
    static Aws::Client::AWSError<Aws::Client::CoreErrors> make_tos_error(const std::string& exception_name) {
        Aws::Client::AWSError<Aws::Client::CoreErrors> error(Aws::Client::CoreErrors::SERVICE_UNAVAILABLE, false);
        error.SetExceptionName(exception_name.c_str());
        error.SetResponseCode(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS);
        return error;
    }

    // Helper to create network error
    static Aws::Client::AWSError<Aws::Client::CoreErrors> make_network_error() {
        Aws::Client::AWSError<Aws::Client::CoreErrors> error(Aws::Client::CoreErrors::NETWORK_CONNECTION, true);
        error.SetResponseCode(Aws::Http::HttpResponseCode::REQUEST_NOT_MADE);
        return error;
    }

    S3RetryStrategy::Config default_config() {
        S3RetryStrategy::Config config;
        config.max_retries = 10;
        config.initial_delay_ms = 25;
        config.max_delay_ms = 10000;
        return config;
    }
};

// Test: 429/503/509 errors should retry
TEST_F(S3RetryStrategyTest, test_should_retry_throttling) {
    auto config = default_config();
    S3RetryStrategy strategy(config);

    // 429 Too Many Requests
    auto error_429 = make_error(429);
    EXPECT_TRUE(strategy.ShouldRetry(error_429, 0));

    // 503 Service Unavailable
    auto error_503 = make_error(503);
    EXPECT_TRUE(strategy.ShouldRetry(error_503, 0));

    // 509 Bandwidth Limit Exceeded
    auto error_509 = make_error(509);
    EXPECT_TRUE(strategy.ShouldRetry(error_509, 0));
}

// Test: 301 Permanent Redirect should NOT retry
TEST_F(S3RetryStrategyTest, test_should_not_retry_301) {
    auto config = default_config();
    S3RetryStrategy strategy(config);

    auto error = make_error(301);
    error.SetResponseCode(Aws::Http::HttpResponseCode::MOVED_PERMANENTLY);
    EXPECT_FALSE(strategy.ShouldRetry(error, 0));
}

// Test: 403 Forbidden should NOT retry (unless AWS marks it retryable)
TEST_F(S3RetryStrategyTest, test_should_not_retry_403) {
    auto config = default_config();
    S3RetryStrategy strategy(config);

    auto error = make_error(403, Aws::Client::CoreErrors::ACCESS_DENIED, false);
    EXPECT_FALSE(strategy.ShouldRetry(error, 0));
}

// Test: 404 Not Found should NOT retry
TEST_F(S3RetryStrategyTest, test_should_not_retry_404) {
    auto config = default_config();
    S3RetryStrategy strategy(config);

    auto error = make_error(404, Aws::Client::CoreErrors::RESOURCE_NOT_FOUND, false);
    EXPECT_FALSE(strategy.ShouldRetry(error, 0));
}

// Test: Max retries exceeded
TEST_F(S3RetryStrategyTest, test_max_retries_exceeded) {
    auto config = default_config();
    config.max_retries = 3;
    S3RetryStrategy strategy(config);

    auto error = make_error(503);

    // Should retry for attempts 0, 1, 2
    EXPECT_TRUE(strategy.ShouldRetry(error, 0));
    EXPECT_TRUE(strategy.ShouldRetry(error, 1));
    EXPECT_TRUE(strategy.ShouldRetry(error, 2));

    // Should NOT retry after max_retries
    EXPECT_FALSE(strategy.ShouldRetry(error, 3));
    EXPECT_FALSE(strategy.ShouldRetry(error, 4));
}

// Test: Exponential backoff calculation (with 10% jitter always enabled)
TEST_F(S3RetryStrategyTest, test_exponential_backoff) {
    auto config = default_config();
    config.initial_delay_ms = 25;
    config.max_delay_ms = 10000;
    S3RetryStrategy strategy(config);

    auto error = make_error(503);

    // Exponential backoff: 25 * 2^n with 10% jitter
    // Test range: base <= delay <= base * 1.1
    auto check_delay = [&](int attempt, long base) {
        long delay = strategy.CalculateDelayBeforeNextRetry(error, attempt);
        EXPECT_GE(delay, base) << "Attempt " << attempt;
        EXPECT_LE(delay, static_cast<long>(base * 1.1) + 1) << "Attempt " << attempt;
    };

    check_delay(0, 25);   // 25 * 1
    check_delay(1, 50);   // 25 * 2
    check_delay(2, 100);  // 25 * 4
    check_delay(3, 200);  // 25 * 8
    check_delay(4, 400);  // 25 * 16
    check_delay(5, 800);  // 25 * 32
    check_delay(8, 6400); // 25 * 256
}

// Test: Backoff capped at max_delay
TEST_F(S3RetryStrategyTest, test_backoff_capped) {
    auto config = default_config();
    config.initial_delay_ms = 25;
    config.max_delay_ms = 1000; // Low cap for testing
    S3RetryStrategy strategy(config);

    auto error = make_error(503);

    // 25 * 2^6 = 1600 > 1000, should be capped at max_delay
    // With 10% jitter: range [1000, 1100] but capped at max_delay = 1000
    long delay6 = strategy.CalculateDelayBeforeNextRetry(error, 6);
    EXPECT_GE(delay6, 1000);
    EXPECT_LE(delay6, 1100);

    // Higher attempts should also be capped
    long delay10 = strategy.CalculateDelayBeforeNextRetry(error, 10);
    EXPECT_GE(delay10, 1000);
    EXPECT_LE(delay10, 1100);
}

// Test: Jitter adds randomness within expected range (10% jitter hardcoded)
TEST_F(S3RetryStrategyTest, test_jitter_range) {
    auto config = default_config();
    config.initial_delay_ms = 100;
    config.max_delay_ms = 10000;
    S3RetryStrategy strategy(config);

    auto error = make_error(503);

    // Run multiple times to test jitter
    long min_delay = LONG_MAX;
    long max_delay = 0;
    const int iterations = 100;

    for (int i = 0; i < iterations; ++i) {
        long delay = strategy.CalculateDelayBeforeNextRetry(error, 0);
        min_delay = std::min(min_delay, delay);
        max_delay = std::max(max_delay, delay);
    }

    // Base delay is 100ms
    // With 10% jitter: should be in range [100, 110]
    EXPECT_GE(min_delay, 100) << "Min delay should be >= base delay";
    EXPECT_LE(max_delay, 110) << "Max delay should be <= base * (1 + jitter)";

    // Verify there's actual variation (jitter is working)
    if (iterations > 10) {
        EXPECT_NE(min_delay, max_delay) << "Jitter should produce variation";
    }
}

// Test: TOS-specific errors (ByteDance)
TEST_F(S3RetryStrategyTest, test_tos_specific_errors) {
    auto config = default_config();
    S3RetryStrategy strategy(config);

    // ExceedAccountQPSLimit
    auto error1 = make_tos_error("ExceedAccountQPSLimit");
    EXPECT_TRUE(strategy.ShouldRetry(error1, 0));

    // ExceedAccountRateLimit
    auto error2 = make_tos_error("ExceedAccountRateLimit");
    EXPECT_TRUE(strategy.ShouldRetry(error2, 0));

    // ExceedBucketQPSLimit
    auto error3 = make_tos_error("ExceedBucketQPSLimit");
    EXPECT_TRUE(strategy.ShouldRetry(error3, 0));

    // ExceedBucketRateLimit
    auto error4 = make_tos_error("ExceedBucketRateLimit");
    EXPECT_TRUE(strategy.ShouldRetry(error4, 0));
}

// Test: Network connection errors should retry
TEST_F(S3RetryStrategyTest, test_network_error_retry) {
    auto config = default_config();
    S3RetryStrategy strategy(config);

    auto error = make_network_error();
    EXPECT_TRUE(strategy.ShouldRetry(error, 0));
}

// Test: Global throttle time update (always enabled now)
TEST_F(S3RetryStrategyTest, test_global_throttle_update) {
    // Reset global throttle
    io::S3GlobalThrottle::wait(); // Wait out any existing throttle window

    auto config = default_config();
    S3RetryStrategy strategy(config);

    auto error = make_error(429); // Throttling error

    // This should update global throttle time (always enabled)
    long delay = strategy.CalculateDelayBeforeNextRetry(error, 0);
    EXPECT_GT(delay, 0);

    // Global throttle should be set (we can't easily verify the exact time,
    // but we can verify the function doesn't crash)
}

// Test: GetMaxAttempts returns correct value
TEST_F(S3RetryStrategyTest, test_get_max_attempts) {
    auto config = default_config();
    config.max_retries = 5;
    S3RetryStrategy strategy(config);

    // GetMaxAttempts = max_retries + 1 (initial attempt + retries)
    EXPECT_EQ(6, strategy.GetMaxAttempts());
}

// Test: AWS SDK marked retryable errors
TEST_F(S3RetryStrategyTest, test_aws_sdk_retryable) {
    auto config = default_config();
    S3RetryStrategy strategy(config);

    // Error marked as retryable by AWS SDK
    auto error = make_error(500, Aws::Client::CoreErrors::SERVICE_UNAVAILABLE, true);
    EXPECT_TRUE(strategy.ShouldRetry(error, 0));
}

// Test: Backoff doesn't overflow with high attempt counts
TEST_F(S3RetryStrategyTest, test_backoff_no_overflow) {
    auto config = default_config();
    config.initial_delay_ms = 25;
    config.max_delay_ms = 10000;
    S3RetryStrategy strategy(config);

    auto error = make_error(503);

    // Very high attempt count shouldn't cause overflow
    // With 10% jitter: range [10000, 11000]
    long delay = strategy.CalculateDelayBeforeNextRetry(error, 100);
    EXPECT_GE(delay, 10000);
    EXPECT_LE(delay, 11000);

    delay = strategy.CalculateDelayBeforeNextRetry(error, 1000);
    EXPECT_GE(delay, 10000);
    EXPECT_LE(delay, 11000);
}

// Test: Thread-local RNG doesn't cause issues across threads (10% jitter hardcoded)
TEST_F(S3RetryStrategyTest, test_thread_safety_jitter) {
    auto config = default_config();
    S3RetryStrategy strategy(config);

    auto error = make_error(503);

    std::atomic<int> success_count{0};
    const int num_threads = 4;
    const int iterations = 100;

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < iterations; ++i) {
                long delay = strategy.CalculateDelayBeforeNextRetry(error, 0);
                // 25 * [1.0, 1.1] = [25, 27.5] -> [25, 28]
                if (delay >= 25 && delay <= 28) {
                    success_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All delays should be in valid range
    EXPECT_EQ(num_threads * iterations, success_count.load());
}

} // namespace starrocks
