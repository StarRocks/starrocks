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

// Coverage for the typed BlockReason primitive and the park-time check logic, plus the one base-layer
// operator that owns a mask here (the spill-process pump). The per-family operator masks are asserted in
// each family's own test (block_reason_agg_test / block_reason_join_test), where those operators live.
//
// Standing up a real PipelineDriver to call verify_block_reason_covered() needs a QueryContext +
// FragmentContext + Pipeline (heavy fixtures). So, following the intermediate_block_event_test convention,
// this test runs the exact decision logic the driver core evaluates against a lightweight stub operator,
// and drives the real SpillMetrics counter that the release-path fail-soft increments.

#include "exec/pipeline/primitives/block_reason.h"

#include <gtest/gtest.h>

#include <cstdint>

#include "base/metrics.h"
#include "compute_env/spill/spill_metrics.h"
#include "exec/pipeline/spill_process_operator.h"

namespace starrocks::pipeline {

namespace {

// Stub of the operator surface that verify_block_reason_covered() reads. It is not a real Operator (which
// needs a factory + runtime profile); the check only calls covered_wakeups()/block_reason().
struct StubOp {
    uint32_t covered = 0;
    BlockReason reason = BlockReason::NONE;

    uint32_t covered_wakeups() const { return covered; }
    BlockReason block_reason() const { return reason; }
};

// Outcome of the park-time check for one operator (mirrors verify_block_reason_covered's check_one).
struct CheckResult {
    bool unmapped_intermediate = false; // DCHECK: wakeable intermediate returned NONE while parked
    bool uncovered = false;             // DCHECK in debug / metric + WARN in release
};

CheckResult check_one(const StubOp& op, bool is_intermediate) {
    CheckResult res;
    const uint32_t covered = op.covered_wakeups();
    if (covered == 0) {
        return res; // operator declares no wakeup -> not checked
    }
    const BlockReason reason = op.block_reason();
    if (is_intermediate && reason == BlockReason::NONE) {
        res.unmapped_intermediate = true;
    }
    if (reason == BlockReason::NONE) {
        return res;
    }
    if ((covered & block_reason_bit(reason)) == 0) {
        res.uncovered = true;
    }
    return res;
}

} // namespace

// The one-hot bit helper: NONE has no bit, every other reason has a distinct non-zero bit.
TEST(BlockReasonTest, bit_helper_is_one_hot) {
    ASSERT_EQ(block_reason_bit(BlockReason::NONE), 0u);

    const BlockReason reasons[] = {BlockReason::WAIT_FLUSH, BlockReason::WAIT_RESTORE, BlockReason::WAIT_CHANNEL,
                                   BlockReason::WAIT_LATCH, BlockReason::WAIT_BUILD};
    uint32_t seen = 0;
    for (auto r : reasons) {
        const uint32_t bit = block_reason_bit(r);
        ASSERT_NE(bit, 0u);
        ASSERT_EQ(bit & (bit - 1), 0u); // exactly one bit set
        ASSERT_EQ(seen & bit, 0u);      // distinct from every other reason
        seen |= bit;
    }
}

// The one base-layer operator that declares a mask: the spill-process pump. Channel empty -> WAIT_CHANNEL,
// full writer -> WAIT_RESTORE; both reasons it can name on a false branch are in its declared mask, so on a
// correct branch it can never fail the uncovered check. (The mask is taken from the operator header, the
// single source of truth.)
TEST(BlockReasonTest, spill_process_mask_covers_its_reasons) {
    constexpr uint32_t mask = SpillProcessOperator::kCoveredWakeups;
    ASSERT_NE(mask & block_reason_bit(BlockReason::WAIT_CHANNEL), 0u);
    ASSERT_NE(mask & block_reason_bit(BlockReason::WAIT_RESTORE), 0u);
    ASSERT_FALSE(check_one(StubOp{mask, BlockReason::WAIT_CHANNEL}, /*is_intermediate=*/false).uncovered);
    ASSERT_FALSE(check_one(StubOp{mask, BlockReason::WAIT_RESTORE}, /*is_intermediate=*/false).uncovered);
}

// An operator that declares no wakeup (covered == 0) is never checked, even with a reason set.
TEST(BlockReasonTest, no_declared_wakeup_is_not_checked) {
    StubOp op{0, BlockReason::WAIT_FLUSH};
    auto res = check_one(op, /*is_intermediate=*/false);
    ASSERT_FALSE(res.uncovered);
    ASSERT_FALSE(res.unmapped_intermediate);
}

// NONE on a wakeable edge is fine (not blocked on a spill reason). NONE on a wakeable intermediate while
// classified INTERMEDIATE_BLOCK is an unmapped false branch -> the detect DCHECK fires. The masks are
// synthetic: the logic does not depend on a specific operator.
TEST(BlockReasonTest, none_reason_unmapped_intermediate_detect) {
    StubOp edge{block_reason_bit(BlockReason::WAIT_FLUSH), BlockReason::NONE};
    ASSERT_FALSE(check_one(edge, /*is_intermediate=*/false).unmapped_intermediate);

    StubOp interior{block_reason_bit(BlockReason::WAIT_LATCH), BlockReason::NONE};
    ASSERT_TRUE(check_one(interior, /*is_intermediate=*/true).unmapped_intermediate);
}

// Uncovered simulation: a (test) operator names a reason whose bit is NOT in its mask -> the check flags
// uncovered, and the release fail-soft path ticks the real spill_parked_with_uncovered_reason_total.
TEST(BlockReasonTest, uncovered_reason_flags_and_ticks_metric) {
    // Mask covers only WAIT_FLUSH, but the operator blocks on WAIT_RESTORE (no bit) -> uncovered.
    StubOp op{block_reason_bit(BlockReason::WAIT_FLUSH), BlockReason::WAIT_RESTORE};
    auto res = check_one(op, /*is_intermediate=*/false);
    ASSERT_TRUE(res.uncovered);

    MetricRegistry registry("block_reason_test_registry");
    SpillMetrics metrics(&registry);
    metrics.install(&registry);
    auto* counter = metrics.parked_with_uncovered_reason_total();
    ASSERT_NE(counter, nullptr);

    const int64_t before = counter->value();
    // Mirror the release fail-soft branch of verify_block_reason_covered().
    if (res.uncovered) {
        counter->increment(1);
    }
    ASSERT_EQ(counter->value(), before + 1);
}

} // namespace starrocks::pipeline
