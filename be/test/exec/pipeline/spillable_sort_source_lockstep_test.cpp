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

// Machine-checked equivalence test for the spilled sort SOURCE operators moved onto the event scheduler:
// the single-merge LocalMergeSortSourceOperator and the parallel-merge LocalParallelMergeSortSourceOperator.
//
// Each source carries has_output() (the driver's INPUT_EMPTY/LOCAL_WAITING classifier) and block_reason()
// (the parking-reason predicate the park-time verify_block_reason_covered reads). The contract this test
// enforces by machine, on the domain where the driver can actually park (has_output() == false, not
// finished):
//   * every park names a reason that is either WAIT_RESTORE (covered by covered_wakeups()) or NONE, never an
//     uncovered reason;
//   * safety -- a park that a partition spiller must wake (still building, or a spilled partition not yet
//     restored: !is_partition_sort_finished() || !is_partition_ready()) names WAIT_RESTORE, never NONE. A
//     false NONE there is the dangerous miss (the park-time net goes silent on a restore-park), so it is the
//     property the drift sentinel at the end feeds the exact bug.
//
// The two sources differ in shape:
//   * Single-merge has one park axis -- both has_output() gates (is_partition_sort_finished /
//     is_partition_ready) are spiller-woken -- so every park is WAIT_RESTORE.
//   * Parallel-merge has a mixed model -- the restore axis (!is_partition_sort_finished() ||
//     !is_partition_ready()) names WAIT_RESTORE, while the merge-path stage-coordination axis (all sinks
//     done, every partition restored, the merger still cascading) is woken by the merger's own observable,
//     not a spiller, so it parks on NONE. block_reason() decides by is_partition_ready(), not by recomputing
//     has_output()'s merger-based conjunction.
//
// Real predicate vs model. A full source round-trip needs a prepared Spiller + per-partition SpillerReaders +
// a SortContext + a driver (heavy fixtures); on a bare Spiller the operators reach has_output_data() which
// derefs the unallocated reader. So, following the convention of this directory
// (spillable_agg_source_lockstep_test), each source below transcribes has_output() / block_reason() /
// is_finished() literally, branch for branch, over an explicit State carrier. Keep these in step with the
// operator bodies: when someone adds or edits a branch in the operator they must edit the matching
// transcription here, and this test re-proves the predicates still agree and that no restore-park goes NONE.
// The masks are pinned to the real operator constants (kCoveredWakeups) so a mask edit is caught too.

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

#include "exec/pipeline/primitives/block_reason.h"
#include "exec/pipeline/sort/local_merge_sort_source_operator.h"
#include "exec/pipeline/sort/local_parallel_merge_sort_source_operator.h"

namespace starrocks::pipeline {

namespace {

bool reason_named_and_covered(BlockReason reason, uint32_t covered_mask) {
    return reason != BlockReason::NONE && (covered_mask & block_reason_bit(reason)) != 0;
}

// ---------------------------------------------------------------------------------------------------------
// Single-merge source: LocalMergeSortSourceOperator.
//
// has_output() = !spiller_task_status().ok() ? true
//              : is_partition_sort_finished() && !is_output_finished() && is_partition_ready()
// is_finished() = is_partition_sort_finished() && is_output_finished()
// block_reason() = (_is_finished || is_finished()) ? NONE : has_output() ? NONE : WAIT_RESTORE
//   (one axis: both non-terminal has_output() gates are spiller-woken, so every park is WAIT_RESTORE. The
//    spiller_task_error short-circuit makes a restore/flush task error report runnable so pull_chunk()
//    propagates it -- the error reaches the source as a source wakeup only and is otherwise unobserved.)
// ---------------------------------------------------------------------------------------------------------
namespace single_merge {

struct State {
    bool finished_latch = false;          // _is_finished (set_finishing latch)
    bool partition_sort_finished = false; // is_partition_sort_finished()
    bool output_finished = false;         // is_output_finished()
    bool partition_ready = false;         // is_partition_ready()
    bool spiller_task_error = false;      // !spiller_task_status().ok() -- any partition spiller task errored
};

// Literal copy of has_output().
bool has_output(const State& s) {
    if (s.spiller_task_error) {
        return true;
    }
    return s.partition_sort_finished && !s.output_finished && s.partition_ready;
}

// Literal copy of is_finished().
bool is_finished(const State& s) {
    return s.partition_sort_finished && s.output_finished;
}

// Literal copy of block_reason().
BlockReason block_reason(const State& s) {
    if (s.finished_latch || is_finished(s)) {
        return BlockReason::NONE;
    }
    if (has_output(s)) {
        return BlockReason::NONE;
    }
    return BlockReason::WAIT_RESTORE;
}

std::vector<State> cross() {
    std::vector<State> v;
    for (bool fl : {false, true})
        for (bool psf : {false, true})
            for (bool of : {false, true})
                for (bool pr : {false, true})
                    for (bool err : {false, true}) {
                        v.push_back(State{fl, psf, of, pr, err});
                    }
    return v;
}

} // namespace single_merge

// ---------------------------------------------------------------------------------------------------------
// Parallel-merge source: LocalParallelMergeSortSourceOperator.
//
// has_output() = !spiller_task_status().ok() ? true
//              : is_partition_sort_finished() && !_is_finished && !is_current_stage_finished() && !is_pending()
// is_finished() = _is_finished
// block_reason() = (_is_finished || is_finished()) ? NONE
//                : has_output() ? NONE
//                : (!is_partition_sort_finished() || !is_partition_ready()) ? WAIT_RESTORE
//                : NONE
//   (mixed: restore axis -> WAIT_RESTORE; merge-CPU stage axis -> NONE. The spiller_task_error short-circuit
//    makes a restore/flush task error report runnable so pull_chunk() propagates it -- the merger's
//    pending/stage gates never observe a task error, only has_output_data.)
// ---------------------------------------------------------------------------------------------------------
namespace parallel_merge {

struct State {
    bool finished_latch = false;          // _is_finished (flipped in pull_chunk by _merger->is_finished())
    bool partition_sort_finished = false; // is_partition_sort_finished()
    bool current_stage_finished = false;  // _merger->is_current_stage_finished()
    bool is_pending = false;              // _merger->is_pending()
    bool partition_ready = false;         // is_partition_ready() -- the restore/merge-CPU discriminator
    bool spiller_task_error = false;      // !spiller_task_status().ok() -- any partition spiller task errored
};

// Literal copy of has_output().
bool has_output(const State& s) {
    if (s.spiller_task_error) {
        return true;
    }
    if (!s.partition_sort_finished) {
        return false;
    }
    if (s.finished_latch) {
        return false;
    }
    if (s.current_stage_finished) {
        return false;
    }
    if (s.is_pending) {
        return false;
    }
    return true;
}

// Literal copy of is_finished().
bool is_finished(const State& s) {
    return s.finished_latch;
}

// Literal copy of block_reason().
BlockReason block_reason(const State& s) {
    if (s.finished_latch || is_finished(s)) {
        return BlockReason::NONE;
    }
    if (has_output(s)) {
        return BlockReason::NONE;
    }
    if (!s.partition_sort_finished || !s.partition_ready) {
        return BlockReason::WAIT_RESTORE;
    }
    return BlockReason::NONE;
}

std::vector<State> cross() {
    std::vector<State> v;
    for (bool fl : {false, true})
        for (bool psf : {false, true})
            for (bool csf : {false, true})
                for (bool pend : {false, true})
                    for (bool pr : {false, true})
                        for (bool err : {false, true}) {
                            v.push_back(State{fl, psf, csf, pend, pr, err});
                        }
    return v;
}

} // namespace parallel_merge

} // namespace

// Single-merge: every park is WAIT_RESTORE (one spiller axis), and the genuine restore park is reached.
TEST(SpillableSortSourceLockstepTest, single_merge_every_park_names_covered_restore) {
    using namespace single_merge;
    constexpr uint32_t mask = LocalMergeSortSourceOperator::kCoveredWakeups;
    ASSERT_NE(mask & block_reason_bit(BlockReason::WAIT_RESTORE), 0u);

    bool saw_restore_park = false;
    for (const auto& s : cross()) {
        const bool parked = !has_output(s) && !is_finished(s) && !s.finished_latch;
        const BlockReason r = block_reason(s);
        if (!parked) {
            // Runnable or finished: block_reason() must be NONE (the driver does not consult it here).
            ASSERT_EQ(r, BlockReason::NONE) << "single_merge: non-parked state named a reason";
            continue;
        }
        // Parked: the single spiller axis -> WAIT_RESTORE, covered, never NONE.
        ASSERT_EQ(r, BlockReason::WAIT_RESTORE) << "single_merge: a park returned " << block_reason_name(r);
        ASSERT_TRUE(reason_named_and_covered(r, mask)) << "single_merge: restore park is uncovered";
        saw_restore_park = true;
    }
    ASSERT_TRUE(saw_restore_park); // guards against a vacuous all-runnable matrix
}

// Parallel-merge: the mixed model. Restore axis -> WAIT_RESTORE (covered); merge-CPU axis -> NONE (generic).
// Safety: a parked state with a not-ready partition must name WAIT_RESTORE, never NONE. Both park kinds are
// reached so the matrix is non-vacuous.
TEST(SpillableSortSourceLockstepTest, parallel_merge_mixed_model_restore_park_never_none) {
    using namespace parallel_merge;
    constexpr uint32_t mask = LocalParallelMergeSortSourceOperator::kCoveredWakeups;
    ASSERT_NE(mask & block_reason_bit(BlockReason::WAIT_RESTORE), 0u);

    bool saw_restore_park = false, saw_merge_cpu_park = false;
    for (const auto& s : cross()) {
        const bool parked = !has_output(s) && !is_finished(s) && !s.finished_latch;
        const BlockReason r = block_reason(s);
        if (!parked) {
            ASSERT_EQ(r, BlockReason::NONE) << "parallel_merge: non-parked state named a reason";
            continue;
        }
        // Park-domain invariant: block_reason is WAIT_RESTORE exactly on the spiller axis.
        const bool on_restore_axis = !s.partition_sort_finished || !s.partition_ready;
        if (on_restore_axis) {
            // Safety: the restore park must be WAIT_RESTORE (the draft predicate's bug returned NONE here).
            ASSERT_EQ(r, BlockReason::WAIT_RESTORE) << "parallel_merge: restore park returned " << block_reason_name(r);
            ASSERT_TRUE(reason_named_and_covered(r, mask)) << "parallel_merge: restore park is uncovered";
            saw_restore_park = true;
        } else {
            // All sinks done AND every partition restored: the merge-CPU stage axis, woken by the merger, NONE.
            ASSERT_EQ(r, BlockReason::NONE) << "parallel_merge: merge-CPU park named a spiller reason";
            saw_merge_cpu_park = true;
        }
    }
    ASSERT_TRUE(saw_restore_park);   // a genuine restore park exists
    ASSERT_TRUE(saw_merge_cpu_park); // a genuine merge-CPU NONE-park exists (mixed model is non-trivial)
}

// Drift sentinel: the rejected draft predicate that named only the build axis (!is_partition_sort_finished())
// as WAIT_RESTORE and dropped the steady-state leaf-restore park (is_partition_sort_finished() &&
// !is_partition_ready()) to NONE -- the exact "NONE on a restore-park" miss. The cross-check must reject it.
TEST(SpillableSortSourceLockstepTest, oracle_rejects_draft_predicate_that_nones_a_restore_park) {
    using namespace parallel_merge;

    // The draft: restore == only the build axis; everything else NONE.
    auto draft_block_reason = [](const State& s) -> BlockReason {
        if (s.finished_latch || is_finished(s)) return BlockReason::NONE;
        if (has_output(s)) return BlockReason::NONE;
        if (!s.partition_sort_finished) return BlockReason::WAIT_RESTORE; // build axis only
        return BlockReason::NONE;                                         // BUG: leaf-restore park -> NONE
    };

    bool oracle_fired = false;
    for (const auto& s : cross()) {
        const bool parked = !has_output(s) && !is_finished(s) && !s.finished_latch;
        if (!parked) continue;
        const bool on_restore_axis = !s.partition_sort_finished || !s.partition_ready;
        if (on_restore_axis && draft_block_reason(s) != BlockReason::WAIT_RESTORE) {
            oracle_fired = true; // the safety property the real predicate upholds and the draft violates
            break;
        }
    }
    ASSERT_TRUE(oracle_fired) << "cross-check failed to detect the draft NONE-on-restore-park bug";

    // Concretely on the steady-state leaf-restore park: sinks done, a partition not yet restored, parked via
    // is_pending. Real names WAIT_RESTORE; the draft forgets it -> NONE.
    State leaf_restore_park;
    leaf_restore_park.partition_sort_finished = true; // all sinks flushed
    leaf_restore_park.partition_ready = false;        // a spilled partition still restoring
    leaf_restore_park.is_pending = true;              // merger pending on that leaf provider
    ASSERT_FALSE(has_output(leaf_restore_park));
    ASSERT_FALSE(is_finished(leaf_restore_park));
    ASSERT_EQ(block_reason(leaf_restore_park), BlockReason::WAIT_RESTORE);
    ASSERT_EQ(draft_block_reason(leaf_restore_park), BlockReason::NONE);
}

// Error-surfacing safety. A partition spiller's restore/flush task error reaches the source as a source
// wakeup only; neither is_partition_ready()/has_output_data() nor the merger's pending gates observe it. The
// source must then report runnable (has_output() true -> block_reason() NONE) so the driver runs pull_chunk()
// and propagates the error -- otherwise it re-parks WAIT_RESTORE with no further wakeup and sleeps until query
// timeout. This is the agg/NLJ sources' RETURN_TRUE_IF_SPILL_TASK_ERROR branch, here for sort's N spillers.
TEST(SpillableSortSourceLockstepTest, single_merge_spiller_task_error_surfaces_runnable) {
    using namespace single_merge;
    // A textbook restore park: sinks done, a spilled partition not yet restored, not finished.
    State park;
    park.partition_sort_finished = true;
    park.output_finished = false;
    park.partition_ready = false;
    ASSERT_FALSE(has_output(park));
    ASSERT_FALSE(is_finished(park));
    ASSERT_EQ(block_reason(park), BlockReason::WAIT_RESTORE);
    // Same state, but a partition spiller recorded a task error: now runnable, not parked.
    park.spiller_task_error = true;
    ASSERT_TRUE(has_output(park));
    ASSERT_FALSE(is_finished(park));
    ASSERT_EQ(block_reason(park), BlockReason::NONE);
}

TEST(SpillableSortSourceLockstepTest, parallel_merge_spiller_task_error_surfaces_runnable) {
    using namespace parallel_merge;
    // The steady-state leaf-restore park: sinks done, a partition still restoring, merger pending on its leaf.
    State park;
    park.partition_sort_finished = true;
    park.partition_ready = false;
    park.is_pending = true;
    ASSERT_FALSE(has_output(park));
    ASSERT_FALSE(is_finished(park));
    ASSERT_EQ(block_reason(park), BlockReason::WAIT_RESTORE);
    // Same state, but a partition spiller recorded a task error: now runnable, not parked.
    park.spiller_task_error = true;
    ASSERT_TRUE(has_output(park));
    ASSERT_FALSE(is_finished(park));
    ASSERT_EQ(block_reason(park), BlockReason::NONE);
}

} // namespace starrocks::pipeline
