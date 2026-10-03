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

// Coverage for the driver-core wakeable-intermediate gate.
//
// Standing up a real PipelineDriver needs a QueryContext + FragmentContext + Pipeline (heavy fixtures). So,
// following the spillable_agg_event_test convention, this test runs the exact decision logic the driver
// core evaluates against lightweight stub operators:
//   * the _has_wakeable_intermediates scan (PipelineDriver::prepare),
//   * the INTERMEDIATE_BLOCK classification gate (process()),
//   * the interior-pair predicate _has_intermediate_block() (check_is_ready/is_not_blocked),
//   * the is_still_pending_finish() any_of aggregate.
// These are copies, so if a driver-core body changes, they must change with it.

#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace starrocks::pipeline {

namespace {

// Minimal stub of the operator surface the driver-core decision logic reads. It is intentionally not
// a real Operator (which needs a factory + runtime profile); the loops under test only call
// supports_intermediate_wakeup()/has_output()/need_input()/is_finished()/pending_finish().
struct StubOp {
    bool wakeup = false;
    bool output = true;
    bool input = true;
    bool finished = false;
    bool pending = false;

    bool supports_intermediate_wakeup() const { return wakeup; }
    bool has_output() const { return output; }
    bool need_input() const { return input; }
    bool is_finished() const { return finished; }
    bool pending_finish() const { return pending; }
};

using Chain = std::vector<StubOp>;

// Copy of PipelineDriver::prepare's wakeable-intermediate registry build: interior operators
// are index [1, n-2]; the index form avoids the begin()+1/end()-1 UB at size < 2.
bool has_wakeable_intermediates(const Chain& ops) {
    for (size_t i = 1; i + 1 < ops.size(); ++i) {
        if (ops[i].supports_intermediate_wakeup()) {
            return true;
        }
    }
    return false;
}

// Copy of PipelineDriver::_has_intermediate_block() (consulted by check_is_ready/is_not_blocked for
// an INTERMEDIATE_BLOCK driver). first_unfinished mirrors the driver's _first_unfinished.
bool has_intermediate_block(const Chain& ops, size_t first_unfinished = 0) {
    const size_t n = ops.size();
    for (size_t i = first_unfinished; i + 1 < n; ++i) {
        const bool curr_is_interior = i > 0;
        const bool next_is_interior = (i + 1) < (n - 1);
        if (curr_is_interior && !ops[i].has_output() && !ops[i].is_finished()) {
            return true;
        }
        if (next_is_interior && !ops[i + 1].need_input()) {
            return true;
        }
    }
    return false;
}

// Copy of the process() classification predicate for the INTERMEDIATE_BLOCK branch: park only when an
// interior pair is blocked AND some interior operator promised a wakeup AND the driver is not yielding.
bool classifies_intermediate_block(const Chain& ops, bool should_yield) {
    return has_intermediate_block(ops) && !should_yield && has_wakeable_intermediates(ops);
}

// Copy of PipelineDriver::is_still_pending_finish() (any_of over the whole chain).
bool is_still_pending_finish(const Chain& ops) {
    for (const auto& op : ops) {
        if (op.pending_finish()) {
            return true;
        }
    }
    return false;
}

// Minimal stub of the OperatorFactory surface the fragment gate reads.
struct StubFactory {
    bool support = false; // support_event_scheduler()
    bool wakeup = false;  // supports_intermediate_wakeup()

    bool support_event_scheduler() const { return support; }
    bool supports_intermediate_wakeup() const { return wakeup; }
};

using FactoryChain = std::vector<StubFactory>;

// Copy of Pipeline::all_support_event_scheduler(): the edges (index 0 and last) must always support it; an
// intermediate factory is checked only when it declares an interior wakeup. Plain interiors
// (project/limit/accumulate, default-false support) are skipped so they do not demote the fragment.
bool all_support_event_scheduler(const FactoryChain& fs) {
    const size_t n = fs.size();
    for (size_t i = 0; i < n; ++i) {
        const bool is_edge = (i == 0) || (i + 1 == n);
        if (is_edge || fs[i].supports_intermediate_wakeup()) {
            if (!fs[i].support_event_scheduler()) {
                return false;
            }
        }
    }
    return true;
}

} // namespace

// A no-spill chain with no wakeable intermediate is not classified INTERMEDIATE_BLOCK even when an
// interior pair is blocked -- the driver stays READY (the default, kept polling), so it never sleeps with
// nobody to wake it.
TEST(IntermediateBlockGateTest, no_wakeable_intermediate_stays_ready) {
    // source -> mid -> mid -> sink; the first interior producer stalls (no output, not finished).
    Chain ops(4);
    ops[1].output = false; // interior block present
    for (auto& op : ops) {
        op.wakeup = false; // nobody promises a wakeup
    }

    ASSERT_TRUE(has_intermediate_block(ops));
    ASSERT_FALSE(has_wakeable_intermediates(ops));
    // Gate is closed: the driver does not park, it falls through to READY.
    ASSERT_FALSE(classifies_intermediate_block(ops, /*should_yield=*/false));
}

// The wakeable path: with a wakeable interior operator and an interior block, the driver is classified
// INTERMEDIATE_BLOCK; once the blocking pair opens, the interior-pair re-check (check_is_ready) reports
// ready again.
TEST(IntermediateBlockGateTest, wakeable_intermediate_parks_then_releases) {
    Chain ops(4);
    ops[1].wakeup = true;  // the spillable probe analogue declares a wakeup
    ops[1].output = false; // and is currently blocked (interior producer stalled)

    ASSERT_TRUE(has_wakeable_intermediates(ops));
    ASSERT_TRUE(has_intermediate_block(ops));
    ASSERT_TRUE(classifies_intermediate_block(ops, /*should_yield=*/false));

    // A notify fires; the blocking predicate has flipped (output available now). The interior re-check
    // that a re-scheduled INTERMEDIATE_BLOCK driver runs in check_is_ready then reports no block.
    ops[1].output = true;
    ASSERT_FALSE(has_intermediate_block(ops));

    // should_yield drivers are time-sliced, not blocked: never parked even with a wakeable block.
    ops[1].output = false;
    ASSERT_FALSE(classifies_intermediate_block(ops, /*should_yield=*/true));
}

// UB-safety: the registry loop and the interior-pair predicate are well-defined for the
// degenerate single/empty chains (no interior operator exists), where begin()+1/end()-1 would be UB.
TEST(IntermediateBlockGateTest, degenerate_chains_have_no_interior) {
    ASSERT_FALSE(has_wakeable_intermediates(Chain{}));
    ASSERT_FALSE(has_intermediate_block(Chain{}));

    Chain one(1);
    one[0].wakeup = true;
    one[0].output = false;
    ASSERT_FALSE(has_wakeable_intermediates(one));
    ASSERT_FALSE(has_intermediate_block(one));

    // size 2: source + sink, still no interior operator.
    Chain two(2);
    two[0].output = false;
    two[1].input = false;
    two[0].wakeup = true;
    two[1].wakeup = true;
    ASSERT_FALSE(has_wakeable_intermediates(two));
    ASSERT_FALSE(has_intermediate_block(two));
}

// Edge classification: a stalled source edge (i == 0) and a back-pressured sink edge are not interior
// blocks (they map to INPUT_EMPTY / OUTPUT_FULL), so the interior predicate ignores them.
TEST(IntermediateBlockGateTest, edges_are_not_interior_blocks) {
    {
        // Source edge stalled: classified INPUT_EMPTY upstream, not an interior block.
        Chain ops(4);
        ops[0].output = false;
        ASSERT_FALSE(has_intermediate_block(ops));
    }
    {
        // Sink edge refuses input: classified OUTPUT_FULL, not an interior block.
        Chain ops(4);
        ops[3].input = false;
        ASSERT_FALSE(has_intermediate_block(ops));
    }
    {
        // A finished interior producer is not a block (it is being closed, not waiting).
        Chain ops(4);
        ops[1].output = false;
        ops[1].finished = true;
        ASSERT_FALSE(has_intermediate_block(ops));
    }
}

// is_still_pending_finish aggregates pending_finish over the whole chain (any_of). An interior
// operator with pending_finish=true (e.g. a spilled probe with in-flight load/restore IO) keeps the
// driver alive; once it drains, the aggregate clears. Edge-only pending_finish is covered too.
TEST(IntermediateBlockGateTest, pending_finish_any_of_covers_interior) {
    Chain ops(4);
    ASSERT_FALSE(is_still_pending_finish(ops));

    // Interior operator owns in-flight IO: a source||sink-only check would miss this.
    ops[1].pending = true;
    ASSERT_TRUE(is_still_pending_finish(ops));

    // IO drains -> aggregate clears, the driver may finalize.
    ops[1].pending = false;
    ASSERT_FALSE(is_still_pending_finish(ops));

    // Edge pending_finish is covered too (the any_of subsumes a front||back check).
    ops[0].pending = true;
    ASSERT_TRUE(is_still_pending_finish(ops));
    ops[0].pending = false;
    ops[3].pending = true;
    ASSERT_TRUE(is_still_pending_finish(ops));
}

// Fragment gate: a plain interior factory that opts out (default-false support, not wakeable --
// project / limit / chunk_accumulate) must not demote the fragment off the event scheduler.
TEST(IntermediateBlockGateTest, non_wakeable_intermediate_does_not_demote) {
    // source(support) -> project(no support, not wakeable) -> sink(support).
    FactoryChain fs(3);
    fs[0].support = true;  // source edge
    fs[2].support = true;  // sink edge
    fs[1].support = false; // plain interior, opts out
    fs[1].wakeup = false;  // and is not wakeable
    ASSERT_TRUE(all_support_event_scheduler(fs));

    // Several plain interiors (project, limit, accumulate) -- still reachable.
    FactoryChain many(5);
    many[0].support = true;
    many[4].support = true;
    for (size_t i = 1; i + 1 < many.size(); ++i) {
        many[i].support = false;
        many[i].wakeup = false;
    }
    ASSERT_TRUE(all_support_event_scheduler(many));
}

// A wakeable interior (the spillable probe analogue) that opts out (support == false, e.g. the join flip
// is off) must demote the fragment: its interior waits are driven by foreign events and would hang on the
// event scheduler until its build twin flips too.
TEST(IntermediateBlockGateTest, wakeable_intermediate_opt_out_demotes) {
    FactoryChain fs(3);
    fs[0].support = true;  // source edge
    fs[2].support = true;  // sink edge
    fs[1].wakeup = true;   // wakeable (probe)
    fs[1].support = false; // but opts out -> fragment demoted
    ASSERT_FALSE(all_support_event_scheduler(fs));

    // Once the wakeable interior flips on, the fragment is reachable again.
    fs[1].support = true;
    ASSERT_TRUE(all_support_event_scheduler(fs));
}

// Edges always count: a non-supporting edge demotes regardless of interiors.
TEST(IntermediateBlockGateTest, edges_always_gate) {
    {
        FactoryChain fs(3);
        fs[0].support = false; // source edge opts out
        fs[1].support = true;
        fs[2].support = true;
        ASSERT_FALSE(all_support_event_scheduler(fs));
    }
    {
        FactoryChain fs(3);
        fs[0].support = true;
        fs[1].support = true;
        fs[2].support = false; // sink edge opts out
        ASSERT_FALSE(all_support_event_scheduler(fs));
    }
}

} // namespace starrocks::pipeline
