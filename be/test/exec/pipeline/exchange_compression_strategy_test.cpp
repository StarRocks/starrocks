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

#include "exec/pipeline/exchange/exchange_compression_strategy.h"

#include <gtest/gtest.h>

#include "common/config_compression_fwd.h"

// starrocks_test_objs is compiled with -fno-access-control, so _alpha/_beta are readable here
// without adding a test accessor to the production header.

namespace starrocks::pipeline {

namespace {

// The reward is
//     (ratio / lz4_expected_compression_ratio) * (speed_mbps / lz4_expected_compression_speed_mbps)
// and feedback() counts a success iff that exceeds 1.0.  Every case below pins both expectations
// to the shipped defaults so the arithmetic in the comments is checkable by hand.
constexpr double kExpectedRatio = 2.1;
constexpr double kExpectedSpeedMbps = 600;

// bytes/ns -> MB/s.  One byte per nanosecond is 953.67 MB/s, which is why the reward's speed term
// used to collapse: compression slower than that truncated to zero under integer division.
constexpr double kBytesPerNsToMbps = 1e9 / 1024 / 1024;

class ExchangeCompressionStrategyTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_ratio = config::lz4_expected_compression_ratio;
        _saved_speed = config::lz4_expected_compression_speed_mbps;
        config::lz4_expected_compression_ratio = kExpectedRatio;
        config::lz4_expected_compression_speed_mbps = kExpectedSpeedMbps;
    }
    void TearDown() override {
        config::lz4_expected_compression_ratio = _saved_ratio;
        config::lz4_expected_compression_speed_mbps = _saved_speed;
    }

private:
    double _saved_ratio = 0;
    double _saved_speed = 0;
};

// serialization_time_ns is accepted but unused by feedback(); pass something plausible so the
// call sites read like the real ones.
constexpr uint64_t kSerializationTimeNs = 1'000'000;

} // namespace

// Isolates the RATIO truncation.  90 MB -> 60 MB in exactly 90,000,000 ns is exactly one byte per
// nanosecond, so the speed term is 953.67 MB/s whether it is computed in integers or in doubles --
// the only thing that differs between the two versions is the ratio:
//
//   ratio 1.5  ->  (1.5 / 2.1) * (953.67 / 600) = 1.1353  -> success
//   ratio 1    ->  (1.0 / 2.1) * (953.67 / 600) = 0.7569  -> failure
//
// uint64 / uint64 truncates 1.5 to 1, which is what used to turn this into a failure.
TEST_F(ExchangeCompressionStrategyTest, FractionalRatioIsNotTruncated) {
    ExchangeCompressionStrategy strategy;
    const double alpha_before = strategy._alpha;
    const double beta_before = strategy._beta;

    strategy.feedback(90'000'000, 60'000'000, kSerializationTimeNs, 90'000'000);

    EXPECT_DOUBLE_EQ(alpha_before + 1, strategy._alpha);
    EXPECT_DOUBLE_EQ(beta_before, strategy._beta);
}

// Isolates the SPEED truncation.  The ratio is exactly 4, so it survives integer division
// unchanged; only the speed differs:
//
//   4000 bytes in 8000 ns = 0.5 bytes/ns = 476.84 MB/s  ->  (4 / 2.1) * (476.84 / 600) = 1.5138
//   uint64 division truncates 0.5 bytes/ns to 0         ->  reward 0
//
// Any codec running slower than 953.67 MB/s used to land here, which on analytic data is most of
// them.
TEST_F(ExchangeCompressionStrategyTest, SubBytePerNanosecondSpeedIsNotTruncatedToZero) {
    ExchangeCompressionStrategy strategy;
    const double alpha_before = strategy._alpha;
    const double beta_before = strategy._beta;

    strategy.feedback(4000, 1000, kSerializationTimeNs, 8000);

    EXPECT_DOUBLE_EQ(alpha_before + 1, strategy._alpha);
    EXPECT_DOUBLE_EQ(beta_before, strategy._beta);
}

// The case measured on TPC-DS Q67 (SF1000): lz4 compressed 85.8 MB down to 64.4 MB, a ratio of
// 1.33, at roughly 1 GB/s.  Both truncations fire at once here:
//
//   truncated:  (1 / 2.1) * (953.67 / 600) = 0.7569  -> failure
//   correct:    (1.3333 / 2.1) * (1000.0 / 600) = 1.0582  -> success
//
// This is the whole bug.  The profile showed the exchange serializing 16.9 GB while only 85.8 MB
// (0.5%) ever reached the codec, because the very first feedbacks pushed _beta up and the sampler
// never gave compression another chance.
TEST_F(ExchangeCompressionStrategyTest, RealisticLz4FeedbackCountsAsSuccess) {
    ExchangeCompressionStrategy strategy;
    const double alpha_before = strategy._alpha;
    const double beta_before = strategy._beta;

    // 90 MB -> 67.5 MB (ratio 1.3333) at 1000 MB/s.
    strategy.feedback(90'000'000, 67'500'000, kSerializationTimeNs, 85'834'000);

    EXPECT_DOUBLE_EQ(alpha_before + 1, strategy._alpha);
    EXPECT_DOUBLE_EQ(beta_before, strategy._beta);
}

// The fix must not turn the strategy into "always compress".  Data that genuinely does not pay for
// itself -- ratio 1.1 at 190.73 MB/s, reward 0.1665 -- still has to count as a failure.
TEST_F(ExchangeCompressionStrategyTest, SlowAndPoorlyCompressingFeedbackCountsAsFailure) {
    ExchangeCompressionStrategy strategy;
    const double alpha_before = strategy._alpha;
    const double beta_before = strategy._beta;

    strategy.feedback(1'000'000, 909'091, kSerializationTimeNs, 5'000'000);

    EXPECT_DOUBLE_EQ(alpha_before, strategy._alpha);
    EXPECT_DOUBLE_EQ(beta_before + 1, strategy._beta);
}

// Right at the boundary the comparison is strictly greater-than, so a reward of exactly 1.0 has to
// count as a failure.  4,000,000 bytes in 4,000,000 ns is exactly one byte per nanosecond, so the
// speed term is exactly kBytesPerNsToMbps and the ratio is exactly 2.0; pinning the two
// expectations to those same values makes the reward exactly 1.0, with no floating-point slack.
TEST_F(ExchangeCompressionStrategyTest, RewardIsStrictlyGreaterThanOne) {
    constexpr uint64_t kUncompressed = 4'000'000;
    constexpr uint64_t kCompressed = 2'000'000;
    constexpr uint64_t kTimeNs = 4'000'000;
    config::lz4_expected_compression_speed_mbps = kBytesPerNsToMbps;

    {
        config::lz4_expected_compression_ratio = 2.0; // reward is exactly 1.0
        ExchangeCompressionStrategy strategy;
        const double alpha_before = strategy._alpha;
        const double beta_before = strategy._beta;
        strategy.feedback(kUncompressed, kCompressed, kSerializationTimeNs, kTimeNs);
        EXPECT_DOUBLE_EQ(alpha_before, strategy._alpha);
        EXPECT_DOUBLE_EQ(beta_before + 1, strategy._beta);
    }
    {
        config::lz4_expected_compression_ratio = 1.9; // reward is 2.0 / 1.9 = 1.0526
        ExchangeCompressionStrategy strategy;
        const double alpha_before = strategy._alpha;
        const double beta_before = strategy._beta;
        strategy.feedback(kUncompressed, kCompressed, kSerializationTimeNs, kTimeNs);
        EXPECT_DOUBLE_EQ(alpha_before + 1, strategy._alpha);
        EXPECT_DOUBLE_EQ(beta_before, strategy._beta);
    }
}

// The guard at the top of feedback(): a degenerate sample must not move either counter, and in
// particular must not divide by zero.
TEST_F(ExchangeCompressionStrategyTest, DegenerateFeedbackIsIgnored) {
    ExchangeCompressionStrategy strategy;
    const double alpha_before = strategy._alpha;
    const double beta_before = strategy._beta;

    strategy.feedback(0, 60'000'000, kSerializationTimeNs, 90'000'000);
    strategy.feedback(90'000'000, 0, kSerializationTimeNs, 90'000'000);
    strategy.feedback(90'000'000, 60'000'000, kSerializationTimeNs, 0);

    EXPECT_DOUBLE_EQ(alpha_before, strategy._alpha);
    EXPECT_DOUBLE_EQ(beta_before, strategy._beta);
}

// The user-visible consequence, end to end.  Feeding realistic lz4 numbers has to leave the sampler
// choosing compression; before the fix the same feedback drove _beta up and decide() went
// permanently false for the rest of the query.  With _alpha = 103 and _beta = 1 the Beta posterior
// has mean 0.99, so the 90% bar below is not a tight one.
TEST_F(ExchangeCompressionStrategyTest, SamplerKeepsCompressingAfterRealisticFeedback) {
    ExchangeCompressionStrategy strategy;
    for (int i = 0; i < 100; ++i) {
        strategy.feedback(90'000'000, 67'500'000, kSerializationTimeNs, 85'834'000);
    }

    int compress = 0;
    for (int i = 0; i < 200; ++i) {
        compress += strategy.decide();
    }
    EXPECT_GE(compress, 180) << "sampler abandoned compression despite consistently good feedback";
}

// And the converse, so the test above cannot pass by the sampler simply always saying yes.
TEST_F(ExchangeCompressionStrategyTest, SamplerStopsCompressingAfterBadFeedback) {
    ExchangeCompressionStrategy strategy;
    for (int i = 0; i < 100; ++i) {
        strategy.feedback(1'000'000, 909'091, kSerializationTimeNs, 5'000'000);
    }

    int compress = 0;
    for (int i = 0; i < 200; ++i) {
        compress += strategy.decide();
    }
    EXPECT_LE(compress, 20) << "sampler kept compressing despite consistently bad feedback";
}

} // namespace starrocks::pipeline
