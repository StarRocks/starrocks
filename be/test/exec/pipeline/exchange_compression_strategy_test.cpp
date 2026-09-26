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

// Pins the reward arithmetic of the adaptive exchange compression strategy: known
// bytes/time in, expected win/loss out.
//
// The strategy exposes no accessor for its Thompson-sampling counters, so a verdict is
// observed through decide(). That is a random draw, but the two states are separated by
// many orders of magnitude: after 20 wins P(decide) = 1 - 2^-23, after 20 losses
// P(decide) = 3.3e-5. The thresholds below sit in the empty space between them, so the
// assertions are robust without being seeded.

#include "exec/pipeline/exchange/exchange_compression_strategy.h"

#include <gtest/gtest.h>

#include <cstdint>

#include "common/config_compression_fwd.h"

namespace starrocks::pipeline {

namespace {

constexpr uint64_t kMiB = 1024 * 1024;
constexpr uint64_t kMs = 1000 * 1000;

constexpr int kFeedbackRounds = 20;
constexpr int kDecisions = 1000;
// A strategy fed only wins decides to compress essentially always; one fed only losses
// essentially never. See the file comment for the underlying probabilities.
constexpr int kMostlyCompressThreshold = 990;
constexpr int kRarelyCompressThreshold = 5;

int count_compress_decisions(ExchangeCompressionStrategy& strategy) {
    int decisions_to_compress = 0;
    for (int i = 0; i < kDecisions; ++i) {
        decisions_to_compress += strategy.decide() ? 1 : 0;
    }
    return decisions_to_compress;
}

} // namespace

class ExchangeCompressionStrategyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Pin the reward thresholds so the arithmetic below is independent of ambient config.
        _saved_expected_ratio = config::lz4_expected_compression_ratio;
        _saved_expected_speed_mbps = config::lz4_expected_compression_speed_mbps;
        config::lz4_expected_compression_ratio = 2.1;
        config::lz4_expected_compression_speed_mbps = 600;
    }

    void TearDown() override {
        config::lz4_expected_compression_ratio = _saved_expected_ratio;
        config::lz4_expected_compression_speed_mbps = _saved_expected_speed_mbps;
    }

private:
    double _saved_expected_ratio = 0;
    double _saved_expected_speed_mbps = 0;
};

// GIVEN 5MiB compressed to 2MiB in 8ms: ratio 2.5, speed 625MB/s
// WHEN that feedback is reported repeatedly
// THEN the strategy keeps compressing, because reward = (2.5/2.1) * (625/600) = 1.24 > 1
//
// Regression guard: 625MB/s is 0.66 bytes/ns, so an integer division of bytes by ns
// truncates the speed to 0 and turns this clear win into a loss.
TEST_F(ExchangeCompressionStrategyTest, good_compression_below_one_byte_per_nanosecond_is_a_win) {
    ExchangeCompressionStrategy strategy;

    for (int i = 0; i < kFeedbackRounds; ++i) {
        strategy.feedback(5 * kMiB, 2 * kMiB, /*serialization_time_ns=*/kMs, /*compression_time_ns=*/8 * kMs);
    }

    EXPECT_GE(count_compress_decisions(strategy), kMostlyCompressThreshold);
}

// GIVEN 19MiB compressed to 10MiB in 19ms: ratio 1.9, speed 1000MB/s
// WHEN that feedback is reported repeatedly
// THEN the strategy keeps compressing, because reward = (1.9/2.1) * (1000/600) = 1.51 > 1
//
// Regression guard: this speed survives an integer division (1 byte/ns), but truncating
// the ratio 1.9 to 1 drags the reward down to 0.76 and turns the win into a loss.
TEST_F(ExchangeCompressionStrategyTest, fractional_compression_ratio_is_not_truncated) {
    ExchangeCompressionStrategy strategy;

    for (int i = 0; i < kFeedbackRounds; ++i) {
        strategy.feedback(19 * kMiB, 10 * kMiB, /*serialization_time_ns=*/kMs, /*compression_time_ns=*/19 * kMs);
    }

    EXPECT_GE(count_compress_decisions(strategy), kMostlyCompressThreshold);
}

// GIVEN 5MiB compressed to only 4MiB in 16ms: ratio 1.25, speed 312.5MB/s
// WHEN that feedback is reported repeatedly
// THEN the strategy stops compressing, because reward = (1.25/2.1) * (312.5/600) = 0.31 < 1
TEST_F(ExchangeCompressionStrategyTest, poor_compression_is_a_loss) {
    ExchangeCompressionStrategy strategy;

    for (int i = 0; i < kFeedbackRounds; ++i) {
        strategy.feedback(5 * kMiB, 4 * kMiB, /*serialization_time_ns=*/kMs, /*compression_time_ns=*/16 * kMs);
    }

    EXPECT_LE(count_compress_decisions(strategy), kRarelyCompressThreshold);
}

// GIVEN degenerate samples with a zero byte count or a zero duration
// WHEN they are reported repeatedly
// THEN they are ignored, so the strategy keeps its initial bias towards compressing
TEST_F(ExchangeCompressionStrategyTest, degenerate_feedback_does_not_change_the_verdict) {
    ExchangeCompressionStrategy strategy;

    for (int i = 0; i < kFeedbackRounds; ++i) {
        strategy.feedback(0, 2 * kMiB, kMs, 8 * kMs);
        strategy.feedback(5 * kMiB, 0, kMs, 8 * kMs);
        strategy.feedback(5 * kMiB, 2 * kMiB, kMs, 0);
    }

    // The initial state is alpha=3, beta=1, i.e. P(decide) = 0.875. Had the degenerate
    // samples been counted as losses, P(decide) would have collapsed to ~1e-14.
    EXPECT_GT(count_compress_decisions(strategy), kDecisions / 2);
}

} // namespace starrocks::pipeline
