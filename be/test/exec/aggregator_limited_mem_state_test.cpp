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

#include <gtest/gtest.h>

#include "exec/aggregator.h"

namespace starrocks {

// Unit tests for LimitedMemAggState::clamp_budget, the guard behind the streaming
// pre-aggregation OOM fix.
//
// AggregateStreamingSinkOperator / AggregateDistinctStreamingSinkOperator::set_execute_mode()
// latch the LIMITED_MEM budget into LimitedMemAggState::limited_memory_size exactly once (the
// driver's enter_low_memory_mode() is a one-shot latch). has_limited() then evaluates
// `limited_memory_size > 0 && memory_usage() >= limited_memory_size`. If the budget were
// latched to 0, has_limited() would be false forever, degrading LIMITED_MEM back to AUTO with
// no memory cap and letting a high-cardinality group-by OOM. clamp_budget() computes
// min(usage, cap) but floors the result at 1 so the budget is never 0.
class LimitedMemAggStateTest : public ::testing::Test {};

// Representative cap: config::streaming_agg_limited_memory_size default = 128 MiB.
static constexpr int64_t kCap = 128LL * 1024 * 1024;

// Heart of the fix: a 0 usage must be floored to 1, never left at 0.
TEST_F(LimitedMemAggStateTest, zero_usage_is_floored_to_one) {
    EXPECT_EQ(1u, LimitedMemAggState::clamp_budget(0, kCap));
}

// Positive usage below the cap passes through unchanged.
TEST_F(LimitedMemAggStateTest, usage_below_cap_passthrough) {
    EXPECT_EQ(1u, LimitedMemAggState::clamp_budget(1, kCap));
    // 24 = empty phmap flat_hash_map dump_bound (sizeof(size_t) * 3); 256 = default fixed hash map.
    EXPECT_EQ(24u, LimitedMemAggState::clamp_budget(24, kCap));
    EXPECT_EQ(256u, LimitedMemAggState::clamp_budget(256, kCap));
    EXPECT_EQ(static_cast<size_t>(kCap - 1), LimitedMemAggState::clamp_budget(kCap - 1, kCap));
}

// Usage at or above the cap is clamped down to the cap.
TEST_F(LimitedMemAggStateTest, usage_at_or_above_cap_clamped) {
    EXPECT_EQ(static_cast<size_t>(kCap), LimitedMemAggState::clamp_budget(kCap, kCap));
    EXPECT_EQ(static_cast<size_t>(kCap), LimitedMemAggState::clamp_budget(kCap + 1, kCap));
    EXPECT_EQ(static_cast<size_t>(kCap), LimitedMemAggState::clamp_budget(kCap * 4, kCap));
}

// The invariant that actually prevents the OOM regression: the budget is always > 0, so
// has_limited() can never be permanently disabled, whatever memory usage is reported.
TEST_F(LimitedMemAggStateTest, budget_is_always_positive) {
    const int64_t usages[] = {0, 1, 24, 256, kCap - 1, kCap, kCap + 1, kCap * 10};
    for (int64_t usage : usages) {
        EXPECT_GT(LimitedMemAggState::clamp_budget(usage, kCap), 0u) << "usage=" << usage;
    }
}

} // namespace starrocks
