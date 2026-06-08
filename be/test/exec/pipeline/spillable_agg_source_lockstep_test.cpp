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

// Machine-checked equivalence test for the spilled blocking-agg SOURCE families that were moved onto the
// event scheduler: distinct-blocking, partitionwise-agg, partitionwise-distinct.
//
// Each source operator carries two independent predicates that must agree: has_output() (the driver's
// INPUT_EMPTY classifier) and _blocked_on() (the parking-reason predicate that block_reason() reads). The
// contract this test enforces by machine (instead of by review discipline) is, on the domain where the
// driver can actually park (spilled, not yet finished):
//
//     has_output() == (_blocked_on() == std::nullopt)
//
// i.e. the source reports work ready exactly when it is not parked on a spill block, and every spilled park
// (has_output() == false there) names a reason in covered_wakeups() -- never NONE -- so the driver knows
// which trigger will wake it. Two states sit off that park domain and are the documented exceptions:
//   * Non-spilled park: has_output() == false while !spilled() -- parks on NONE on purpose (the wakeup
//     comes from the aggregator-pip source observable; the named WAIT_RESTORE/WAIT_FLUSH reasons are only
//     declared once spilled()).
//   * Finished / all-partitions-drained: the source drives to EOS, the driver never parks it.
// Off the park domain block_reason() is never read by the driver, so _blocked_on()'s value there is
// don't-care and is not asserted (the operator makes no promise about a predicate that is never read on a
// non-spilled or finished source).
//
// Real predicate vs model. A full source round-trip needs an Aggregator + ConjugateOperator + prepared
// Spiller + per-partition SpillerReaders + a driver (heavy fixtures; that path runs through the full
// executor). The Aggregator is a concrete heavyweight built from a TPlanNode + params + exprs (not
// mockable), the source ctors take a concrete AggregatorPtr / DistinctSourceOperatorPtr / ConjugateOperatorPtr
// plus partition-reader state, and on a bare Spiller the operators reach is_full()/has_output_data() which
// deref the unallocated writer/reader. So the real predicate bodies cannot be called in a unit test.
// Following the convention of this directory (block_reason_test, spillable_agg_event_test,
// spillable_hash_join_build_event_test all keep predicate copies in step with the operators), each family
// below transcribes has_output() and _blocked_on() literally, branch for branch, as two functions over one
// explicit SourceState carrier. Keep these in step with the operator bodies: when someone adds or edits a
// branch in the operator they must edit the matching transcription here, and this test re-proves the two
// predicates still agree.
//
// Why this catches drift. The two transcriptions are independent functions, exactly as in the operator. The
// matrix below drives the named park-states the review called out (not-spilled / sink-incomplete /
// reader-in-flight / spilled-eos / cancel / error) and a bounded reachable cross-product, and cross-checks
// the two predicates on each. If a future change adds a park branch to has_output() without the matching
// runnable/park branch in _blocked_on() -- the classic "covered in one, forgotten in the other" miss -- the
// equality fails for that state, and the named-reason assertion fails if the new spilled park returns NONE.
// The drift sentinel at the end proves the test is non-vacuous by feeding it exactly that bug.

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

#include "exec/pipeline/aggregate/spillable_aggregate_distinct_blocking_operator.h"
#include "exec/pipeline/aggregate/spillable_partitionwise_aggregate_operator.h"
#include "exec/pipeline/aggregate/spillable_partitionwise_distinct_operator.h"
#include "exec/pipeline/primitives/block_reason.h"

namespace starrocks::pipeline {

namespace {

bool reason_is_named_and_covered(BlockReason reason, uint32_t covered_mask) {
    return reason != BlockReason::NONE && (covered_mask & block_reason_bit(reason)) != 0;
}

// ---------------------------------------------------------------------------------------------------------
// Family 1: distinct-blocking source.
//
// Literal transcription of SpillableAggregateDistinctBlockingSourceOperator::has_output() / ::_blocked_on() /
// ::is_finished() (spillable_aggregate_distinct_blocking_operator.cpp). has_output() is not computed
// directly from _blocked_on(): it leads with the base AggregateDistinctBlockingSourceOperator::has_output()
// check unconditionally; the spilled tail then matches _blocked_on()'s runnable disjunction.
// ---------------------------------------------------------------------------------------------------------
namespace distinct_blocking {

struct State {
    bool is_finished = false;     // _is_finished (set_finished latch)
    bool base_has_output = false; // AggregateDistinctBlockingSourceOperator::has_output()
    bool spilled = false;
    bool accumulator_has_output = false;
    bool spiller_has_output_data = false;
    bool task_status_ok = true; // !task_status().ok() == spill task error
    bool spiller_is_cancel = false;
    bool is_spilled_eos = false;
    bool has_last_chunk = true;
};

// LOCKSTEP COPY of has_output().
bool has_output(const State& s) {
    if (s.is_finished) {
        return false;
    }
    if (s.base_has_output) {
        return true;
    }
    if (!s.spilled) {
        return false;
    }
    if (s.accumulator_has_output) {
        return true;
    }
    if (s.spiller_has_output_data) {
        return true;
    }
    // RETURN_TRUE_IF_SPILL_TASK_ERROR(spiller): a failed spiller is runnable.
    if (!s.task_status_ok) {
        return true;
    }
    if (s.spiller_is_cancel) {
        return true;
    }
    if (s.is_spilled_eos && s.has_last_chunk) {
        return true;
    }
    return false;
}

// LOCKSTEP COPY of _blocked_on().
std::optional<BlockReason> blocked_on(const State& s) {
    if (s.is_finished) {
        return std::nullopt;
    }
    if (s.base_has_output) {
        return std::nullopt;
    }
    if (!s.spilled) {
        return std::nullopt; // documented non-spilled NONE-park
    }
    if (s.accumulator_has_output || s.spiller_has_output_data) {
        return std::nullopt;
    }
    if (!s.task_status_ok) {
        return std::nullopt;
    }
    if (s.spiller_is_cancel) {
        return std::nullopt;
    }
    if (s.is_spilled_eos && s.has_last_chunk) {
        return std::nullopt;
    }
    return BlockReason::WAIT_RESTORE;
}

// LOCKSTEP COPY of is_finished() (the spilled-eos terminal; base path not part of the park matrix).
bool is_finished(const State& s) {
    if (s.is_finished) {
        return true;
    }
    if (!s.spilled) {
        return false; // base is_finished -- not a spill park, not modeled
    }
    if (s.accumulator_has_output) {
        return false;
    }
    if (s.spiller_is_cancel) {
        return true;
    }
    return s.is_spilled_eos && !s.has_last_chunk;
}

bool spilled(const State& s) {
    return s.spilled;
}

// The named park-states the review called out, as explicit reachable rows. Each row is one scenario.
std::vector<State> named_scenarios() {
    std::vector<State> v;
    // not-spilled, nothing buffered: the documented NONE-park (has_output false, blocked nullopt).
    v.push_back(State{});
    // not-spilled but base has output: runnable.
    v.push_back([] {
        State s;
        s.base_has_output = true;
        return s;
    }());
    // spilled, restore in flight, nothing buffered, not eos: the genuine WAIT_RESTORE park.
    v.push_back([] {
        State s;
        s.spilled = true;
        return s;
    }());
    // spilled, accumulator buffered: runnable.
    v.push_back([] {
        State s;
        s.spilled = true;
        s.accumulator_has_output = true;
        return s;
    }());
    // spilled, spiller output buffered: runnable.
    v.push_back([] {
        State s;
        s.spilled = true;
        s.spiller_has_output_data = true;
        return s;
    }());
    // spilled, spill task error: runnable (carries status out).
    v.push_back([] {
        State s;
        s.spilled = true;
        s.task_status_ok = false;
        return s;
    }());
    // spilled, cancel: runnable.
    v.push_back([] {
        State s;
        s.spilled = true;
        s.spiller_is_cancel = true;
        return s;
    }());
    // spilled, eos with last chunk pending: runnable (emits the eos chunk).
    v.push_back([] {
        State s;
        s.spilled = true;
        s.is_spilled_eos = true;
        s.has_last_chunk = true;
        return s;
    }());
    // spilled, eos, last chunk consumed: finished (drives to EOS, not parked).
    v.push_back([] {
        State s;
        s.spilled = true;
        s.is_spilled_eos = true;
        s.has_last_chunk = false;
        return s;
    }());
    // set_finished latch: finished.
    v.push_back([] {
        State s;
        s.is_finished = true;
        return s;
    }());
    return v;
}

// Bounded reachable cross-product to catch drift outside the hand-picked rows. Pin the unreachable combos out
// (a finished latch is terminal; we vary the spilled sub-state independently).
std::vector<State> reachable_cross() {
    std::vector<State> v;
    for (bool sp : {false, true})
        for (bool base : {false, true})
            for (bool acc : {false, true})
                for (bool sod : {false, true})
                    for (bool ok : {false, true})
                        for (bool cancel : {false, true})
                            for (bool eos : {false, true})
                                for (bool last : {false, true}) {
                                    State s;
                                    s.spilled = sp;
                                    s.base_has_output = base;
                                    s.accumulator_has_output = acc;
                                    s.spiller_has_output_data = sod;
                                    s.task_status_ok = ok;
                                    s.spiller_is_cancel = cancel;
                                    s.is_spilled_eos = eos;
                                    s.has_last_chunk = last;
                                    v.push_back(s);
                                }
    return v;
}

} // namespace distinct_blocking

// ---------------------------------------------------------------------------------------------------------
// Families 2 & 3: partitionwise-agg and partitionwise-distinct sources.
//
// The two share an identical has_output()/_blocked_on()/is_finished() shape (only the wrapped sub-op type
// differs: _pw_agg vs _pw_distinct). Literal transcription of
// SpillablePartitionWiseAggregateSourceOperator (and the distinct twin). _blocked_on() is a manual switch
// that repeats has_output()'s nested branch order.
//
// Reachability note encoded in the state model: !is_sink_complete strictly PRECEDES any partition processing
// (the source cannot have pulled partitions, advanced _curr_partition_idx, or driven a reader before the sink
// signaled it finished flushing). So in any reachable state, is_sink_complete==false implies partitions_empty
// and !all_partitions_processed. The scenario rows respect this; the cross-product gates on it.
// ---------------------------------------------------------------------------------------------------------
namespace partitionwise {

struct State {
    bool task_status_ok = true;
    bool spilled = false;
    bool base_has_output = false;  // _non_pw_agg->has_output(), only consulted when !spilled
    bool is_sink_complete = false; // _non_pw_agg->aggregator()->is_sink_complete()
    bool partitions_empty = true;
    bool all_partitions_processed = false; // _curr_partition_idx >= _partitions.size()
    bool reader_present = false;           // _curr_partition_reader != nullptr
    bool reader_has_restore_task = false;
    bool reader_has_output_data = false;
    bool curr_partition_eos = false;
    bool pw_is_finished = false; // _pw_agg->is_finished()
};

// LOCKSTEP COPY of has_output().
bool has_output(const State& s) {
    // RETURN_TRUE_IF_SPILL_TASK_ERROR(spiller): a failed spiller is runnable.
    if (!s.task_status_ok) {
        return true;
    }
    if (!s.spilled) {
        return s.base_has_output;
    }
    if (!s.is_sink_complete) {
        return false;
    }
    if (s.partitions_empty) {
        return true;
    }
    if (s.all_partitions_processed) {
        return false;
    }
    if (!s.reader_present || !s.reader_has_restore_task || s.reader_has_output_data) {
        return true;
    }
    if (s.curr_partition_eos && !s.pw_is_finished) {
        return true;
    }
    return false;
}

// LOCKSTEP COPY of _blocked_on().
std::optional<BlockReason> blocked_on(const State& s) {
    if (!s.task_status_ok) {
        return std::nullopt;
    }
    if (!s.spilled) {
        return std::nullopt; // documented non-spilled NONE-park
    }
    if (!s.is_sink_complete) {
        return BlockReason::WAIT_FLUSH;
    }
    if (s.partitions_empty) {
        return std::nullopt;
    }
    if (s.all_partitions_processed) {
        return std::nullopt;
    }
    if (!s.reader_present || !s.reader_has_restore_task || s.reader_has_output_data) {
        return std::nullopt;
    }
    if (s.curr_partition_eos && !s.pw_is_finished) {
        return std::nullopt;
    }
    return BlockReason::WAIT_RESTORE;
}

// LOCKSTEP COPY of is_finished() (spilled branch: all partitions drained; base path not part of the matrix).
bool is_finished(const State& s) {
    if (!s.spilled) {
        return false;
    }
    return !s.partitions_empty && s.all_partitions_processed;
}

bool spilled(const State& s) {
    return s.spilled;
}

// A state is reachable only if is_sink_complete precedes partition processing (see the namespace note).
bool reachable(const State& s) {
    if (!s.is_sink_complete && (!s.partitions_empty || s.all_partitions_processed)) {
        return false;
    }
    return true;
}

std::vector<State> named_scenarios() {
    std::vector<State> v;
    // not-spilled: the documented NONE-park (has_output false, blocked nullopt) / runnable base.
    v.push_back(State{});
    v.push_back([] {
        State s;
        s.base_has_output = true;
        return s;
    }());
    // spilled, sink still flushing: the WAIT_FLUSH park.
    v.push_back([] {
        State s;
        s.spilled = true;
        s.is_sink_complete = false;
        return s;
    }());
    // spilled, sink done, partitions not yet pulled: runnable (first pull fetches partitions).
    v.push_back([] {
        State s;
        s.spilled = true;
        s.is_sink_complete = true;
        s.partitions_empty = true;
        return s;
    }());
    // spilled, reader present, restore in flight, no data, not eos: the WAIT_RESTORE park.
    v.push_back([] {
        State s;
        s.spilled = true;
        s.is_sink_complete = true;
        s.partitions_empty = false;
        s.reader_present = true;
        s.reader_has_restore_task = true;
        return s;
    }());
    // spilled, reader has buffered output: runnable.
    v.push_back([] {
        State s;
        s.spilled = true;
        s.is_sink_complete = true;
        s.partitions_empty = false;
        s.reader_present = true;
        s.reader_has_restore_task = true;
        s.reader_has_output_data = true;
        return s;
    }());
    // spilled, reader absent (need to create it): runnable.
    v.push_back([] {
        State s;
        s.spilled = true;
        s.is_sink_complete = true;
        s.partitions_empty = false;
        s.reader_present = false;
        return s;
    }());
    // spilled, partition drained into pw_agg, pw_agg still has output: runnable.
    v.push_back([] {
        State s;
        s.spilled = true;
        s.is_sink_complete = true;
        s.partitions_empty = false;
        s.reader_present = true;
        s.reader_has_restore_task = true;
        s.curr_partition_eos = true;
        s.pw_is_finished = false;
        return s;
    }());
    // spilled, spill task error: runnable.
    v.push_back([] {
        State s;
        s.spilled = true;
        s.task_status_ok = false;
        return s;
    }());
    // spilled, all partitions processed: finished (drives to EOS).
    v.push_back([] {
        State s;
        s.spilled = true;
        s.is_sink_complete = true;
        s.partitions_empty = false;
        s.all_partitions_processed = true;
        return s;
    }());
    return v;
}

std::vector<State> reachable_cross() {
    std::vector<State> v;
    for (bool ok : {false, true})
        for (bool sp : {false, true})
            for (bool base : {false, true})
                for (bool sink : {false, true})
                    for (bool pe : {false, true})
                        for (bool app : {false, true})
                            for (bool rp : {false, true})
                                for (bool rrt : {false, true})
                                    for (bool rod : {false, true})
                                        for (bool eos : {false, true})
                                            for (bool pwf : {false, true}) {
                                                State s{ok, sp, base, sink, pe, app, rp, rrt, rod, eos, pwf};
                                                if (reachable(s)) {
                                                    v.push_back(s);
                                                }
                                            }
    return v;
}

} // namespace partitionwise

// The core cross-check for one State. block_reason() is read by the driver only while parked: a parked
// driver is one whose source has has_output() == false and is not finished, i.e. spilled && !finished (the
// non-spilled "park" comes from the aggregator-pip observable, not block_reason()). On that park domain the
// two predicates must agree, and every park must name a covered reason. Off the domain (non-spilled, or
// finished/draining-to-EOS) _blocked_on() is never read by the operator, so its value there is don't-care
// and is not asserted (the operator makes no promise about a predicate the driver never reads on a
// finished/non-spilled source).
template <class State, class HasOutput, class BlockedOn, class IsFinished, class Spilled>
void assert_invariant(const State& s, HasOutput has_output_fn, BlockedOn blocked_on_fn, IsFinished is_finished_fn,
                      Spilled spilled_fn, uint32_t covered_mask, const std::string& family) {
    const bool out = has_output_fn(s);
    const std::optional<BlockReason> blocked = blocked_on_fn(s);
    const bool on_park_domain = spilled_fn(s) && !is_finished_fn(s);

    if (!on_park_domain) {
        // Off the park domain: a documented non-spilled NONE-park or a finished/terminal source.
        // block_reason() is not read here; _blocked_on()'s value is don't-care. The one thing that must
        // still hold is that the source does not report has_output() on a finished state -- but that is
        // is_finished()'s contract, exercised separately, not this check's. Nothing to assert against
        // _blocked_on() here.
        return;
    }

    // THE PARALLEL ORACLE: has_output() true iff not parked on a spill block.
    ASSERT_EQ(out, !blocked.has_value()) << family << ": has_output/_blocked_on disagree on the park domain";

    if (out) {
        ASSERT_FALSE(blocked.has_value()) << family << ": runnable state must not name a reason";
    } else {
        ASSERT_TRUE(blocked.has_value()) << family << ": spilled park must name a reason (not NONE)";
        ASSERT_TRUE(reason_is_named_and_covered(*blocked, covered_mask))
                << family << ": spilled park named an uncovered/NONE reason " << block_reason_name(*blocked);
    }
}

} // namespace

// distinct-blocking source: named scenarios + reachable cross-product, every spilled park covered, the genuine
// restore park reached.
TEST(SpillableAggSourceLockstepTest, distinct_blocking_has_output_matches_blocked_on) {
    using namespace distinct_blocking;
    constexpr uint32_t mask = SpillableAggregateDistinctBlockingSourceOperator::kCoveredWakeups;
    auto h = [](const State& x) { return has_output(x); };
    auto b = [](const State& x) { return blocked_on(x); };
    auto f = [](const State& x) { return is_finished(x); };
    auto sp = [](const State& x) { return spilled(x); };

    bool saw_restore_park = false;
    auto run = [&](const std::vector<State>& states) {
        for (const auto& s : states) {
            assert_invariant(s, h, b, f, sp, mask, "distinct_blocking");
            if (spilled(s) && !is_finished(s) && !has_output(s) && blocked_on(s) == BlockReason::WAIT_RESTORE) {
                saw_restore_park = true;
            }
        }
    };
    run(named_scenarios());
    run(reachable_cross());
    ASSERT_TRUE(saw_restore_park); // guards against a vacuous all-runnable matrix
}

// partitionwise-agg source: same cross-check, reaching both spilled parks (WAIT_FLUSH and WAIT_RESTORE).
TEST(SpillableAggSourceLockstepTest, partitionwise_agg_has_output_matches_blocked_on) {
    using namespace partitionwise;
    constexpr uint32_t mask = SpillablePartitionWiseAggregateSourceOperator::kCoveredWakeups;
    auto h = [](const State& x) { return has_output(x); };
    auto b = [](const State& x) { return blocked_on(x); };
    auto f = [](const State& x) { return is_finished(x); };
    auto sp = [](const State& x) { return spilled(x); };

    bool saw_flush = false, saw_restore = false;
    auto run = [&](const std::vector<State>& states) {
        for (const auto& s : states) {
            assert_invariant(s, h, b, f, sp, mask, "partitionwise_agg");
            if (spilled(s) && !is_finished(s) && !has_output(s)) {
                if (blocked_on(s) == BlockReason::WAIT_FLUSH) saw_flush = true;
                if (blocked_on(s) == BlockReason::WAIT_RESTORE) saw_restore = true;
            }
        }
    };
    run(named_scenarios());
    run(reachable_cross());
    ASSERT_TRUE(saw_flush);
    ASSERT_TRUE(saw_restore);
}

// partitionwise-distinct source: shares the partitionwise shape; asserted against its OWN covered mask so the
// two families' masks are each independently proven (they are equal, but the test does not assume it).
TEST(SpillableAggSourceLockstepTest, partitionwise_distinct_has_output_matches_blocked_on) {
    using namespace partitionwise;
    constexpr uint32_t mask = SpillablePartitionWiseDistinctSourceOperator::kCoveredWakeups;
    auto h = [](const State& x) { return has_output(x); };
    auto b = [](const State& x) { return blocked_on(x); };
    auto f = [](const State& x) { return is_finished(x); };
    auto sp = [](const State& x) { return spilled(x); };

    bool saw_flush = false, saw_restore = false;
    auto run = [&](const std::vector<State>& states) {
        for (const auto& s : states) {
            assert_invariant(s, h, b, f, sp, mask, "partitionwise_distinct");
            if (spilled(s) && !is_finished(s) && !has_output(s)) {
                if (blocked_on(s) == BlockReason::WAIT_FLUSH) saw_flush = true;
                if (blocked_on(s) == BlockReason::WAIT_RESTORE) saw_restore = true;
            }
        }
    };
    run(named_scenarios());
    run(reachable_cross());
    ASSERT_TRUE(saw_flush);
    ASSERT_TRUE(saw_restore);
}

// Drift sentinel: a deliberately broken transcription that drops the WAIT_RESTORE park from _blocked_on()
// (returns nullopt) while has_output() still parks (returns false) -- the exact "covered in one, forgotten
// in the other" miss. The cross-check must reject it, proving the matrix tests above are non-vacuous and
// would catch a future real drift.
TEST(SpillableAggSourceLockstepTest, oracle_rejects_unmatched_park_branch) {
    using namespace partitionwise;

    auto buggy_blocked_on = [](const State& s) -> std::optional<BlockReason> {
        auto r = blocked_on(s);
        if (r == BlockReason::WAIT_RESTORE) {
            return std::nullopt; // forget to mark the restore park -> nullopt while has_output()==false
        }
        return r;
    };

    bool oracle_fired = false;
    for (const auto& s : reachable_cross()) {
        if (!(spilled(s) && !is_finished(s))) {
            continue; // only the park domain carries the equality
        }
        if (has_output(s) != !buggy_blocked_on(s).has_value()) {
            oracle_fired = true;
            break;
        }
    }
    ASSERT_TRUE(oracle_fired) << "the lockstep oracle failed to detect a forgotten _blocked_on park branch";

    // And concretely on a spilled-restore park: real names it, the bug forgets it -> inequality.
    State parked;
    parked.spilled = true;
    parked.is_sink_complete = true;
    parked.partitions_empty = false;
    parked.reader_present = true;
    parked.reader_has_restore_task = true;
    ASSERT_FALSE(has_output(parked));
    ASSERT_EQ(blocked_on(parked), BlockReason::WAIT_RESTORE);
    ASSERT_FALSE(buggy_blocked_on(parked).has_value());
}

} // namespace starrocks::pipeline
