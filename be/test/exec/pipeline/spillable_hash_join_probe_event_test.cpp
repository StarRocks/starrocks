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

// Adversarial coverage for the spillable hash-join probe build-side load latch.
//
// A full probe round-trip needs a HashJoiner + prepared Spiller + driver (heavy fixtures); that path runs
// through the full executor. This test drives the latch-slot completion order directly, using the real
// NoBlockCountDownLatch from the operator header.
//
// The completion order under test is the operator's _complete_latch_slot():
//   1. data wakeup  -- source_trigger() only on the task whose count_down() made the latch ready,
//   2. fetch_sub    -- release this task's lifetime slot (_pending_latch_io),
//   3. release      -- source_trigger() by the task whose fetch_sub dropped the counter to zero (never
//                      gated on latch_ready).
//
// Step 3 being unconditional is essential: with N > 1 concurrent load tasks the fetch_sub that drops
// _pending_latch_io to zero need not be the same task whose count_down() returned true. A latch_ready-gated
// release (the bug) would see the counter reach zero with no wakeup pending and wedge a PENDING_FINISH
// driver forever. The fetch_sub == 1 release closes that. The differentiating test below forces the
// adversarial interleaving and asserts that some completion fires a wakeup after _pending_latch_io hits
// zero -- a property the gated form provably lacks (asserted via a side-by-side gated model).

#include <gtest/gtest.h>

#include <atomic>
#include <vector>

#include "exec/pipeline/hashjoin/spillable_hash_join_probe_operator.h"

namespace starrocks::pipeline {

namespace {

// Records source_trigger() calls and the value _pending_latch_io held when each fired. This lets a test ask
// the discriminating question: did any wakeup fire while the counter was already zero (the release the
// gated form drops)?
struct WakeupRecorder {
    std::atomic_int32_t total{0};
    // Count of triggers observed while _pending_latch_io was already at zero (i.e. the release-at-zero).
    std::atomic_int32_t at_zero{0};

    void trigger(int32_t pending_latch_io_after) {
        total.fetch_add(1);
        if (pending_latch_io_after == 0) {
            at_zero.fetch_add(1);
        }
    }
};

// Models the running flush/restore IO of the probe-spiller without a real Spiller.
struct FakeSpillerIo {
    std::atomic_int32_t in_flight{0};
    bool has_running_io_tasks() const { return in_flight.load() > 0; }
};

// Copy of SpillableHashJoinProbeOperator::pending_finish() under the BatchState carrier (I1/I6/I8): the
// batch term reads kLoading, not the latch directly -- a kFailed batch (latch driven to ready by the
// repair) is no longer pinned by the batch term, only by the _pending_latch_io tail. spiller may be null
// (close() resets it), matching the operator's null guard.
bool pending_finish(BatchState batch_state, int32_t pending_latch_io, const FakeSpillerIo* spiller) {
    return batch_state == BatchState::kLoading || pending_latch_io > 0 ||
           (spiller != nullptr && spiller->has_running_io_tasks());
}

// Copy of the pull_chunk batch gate (the switch over BatchState). Returns what the gate routes to: a block
// (kLoading -> park / nullptr), an error (kFailed -> RETURN_IF_ERROR), or proceed (kLoaded / kNone).
enum class GateRoute { kBlock, kError, kProceed };
GateRoute pull_gate_route(BatchState batch_state) {
    switch (batch_state) {
    case BatchState::kLoading:
        return GateRoute::kBlock;
    case BatchState::kFailed:
        return GateRoute::kError;
    case BatchState::kLoaded:
    case BatchState::kNone:
        return GateRoute::kProceed;
    }
    return GateRoute::kProceed;
}

// The completion order: _complete_latch_slot(). The release belongs to whichever task drops the counter to
// zero: fetch_sub returns the prior value, so == 1 identifies that task exactly. pending_finish() reads the
// zero, and a release at a non-zero count would be just a spurious wakeup.
void complete_latch_slot_canon(bool latch_ready, std::atomic_int32_t& pending_latch_io, WakeupRecorder& obs) {
    if (latch_ready) {
        // data wakeup: slot still held, so the counter it observes is the pre-decrement value.
        obs.trigger(pending_latch_io.load());
    }
    if (pending_latch_io.fetch_sub(1) == 1) {
        // release: fired exactly by the zero-dropping task, regardless of latch_ready.
        obs.trigger(0);
    }
}

// The buggy gated form, kept for differentiation only: the release is gated on latch_ready, so a task that
// drops the counter to zero without flipping the latch fires nothing.
void complete_latch_slot_gated(bool latch_ready, std::atomic_int32_t& pending_latch_io, WakeupRecorder& obs) {
    if (latch_ready) {
        obs.trigger(pending_latch_io.load());
    }
    const int32_t after = pending_latch_io.fetch_sub(1) - 1;
    if (latch_ready) { // BUG: release gated on latch_ready
        obs.trigger(after);
    }
}

} // namespace

// While the load batch is in flight (kLoading), the batch gate returns nullptr from pull_chunk before
// _check_partitions / reader-init / restore / probe run. We model the gate over the BatchState carrier and
// assert no downstream state is touched until the last count_down publishes kLoaded. The latch underneath
// is the counting mechanism; the gate reads the carrier, not the latch.
TEST(SpillableProbeLatchTest, pull_gate_blocks_until_batch_loaded) {
    NoBlockCountDownLatch latch;
    std::atomic<BatchState> batch_state{BatchState::kNone};
    latch.reset(2);                          // a 2-partition batch is loading
    batch_state.store(BatchState::kLoading); // published at order time, before submits

    bool readers_initialized = false;
    bool probers_pushed = false;

    auto pull_chunk_body = [&]() -> bool /* produced/touched anything? */ {
        // Batch gate: pull_chunk does not proceed until the batch is loaded. Nothing below runs while the
        // batch loads (kLoading) or after a failure (kError, surfaced as a Status by the operator).
        if (pull_gate_route(batch_state.load()) != GateRoute::kProceed) {
            return false; // returns nullptr / error, no reader init / no prober push
        }
        readers_initialized = true;
        probers_pushed = true;
        return true;
    };

    // First entry: batch still loading -> gate closed, nothing touched.
    ASSERT_FALSE(pull_chunk_body());
    ASSERT_FALSE(readers_initialized);
    ASSERT_FALSE(probers_pushed);

    // Load tasks complete (latch counts down to 0); the last count_down publishes kLoaded before the
    // wakeup (publish-then-notify).
    ASSERT_FALSE(latch.count_down());
    const bool latch_ready = latch.count_down();
    ASSERT_TRUE(latch_ready);
    batch_state.store(latch_ready ? BatchState::kLoaded : BatchState::kFailed);
    ASSERT_TRUE(latch.ready());

    // Re-entry after the load wakeup: gate open (kLoaded), the readers/probers may now be driven.
    ASSERT_TRUE(pull_chunk_body());
    ASSERT_TRUE(readers_initialized);
    ASSERT_TRUE(probers_pushed);
}

// The kFailed regression. A batch load fails: the repair drives the latch to ready and
// publishes kFailed before its notify. The pull gate must route kFailed to an error (return _status()),
// never to proceed -- otherwise the pull would fall into restore/probe against builders that were never
// loaded (the SIGSEGV). The latch being ready is not enough to proceed; the carrier is.
TEST(SpillableProbeLatchTest, pull_gate_kfailed_returns_error_not_restore) {
    NoBlockCountDownLatch latch;
    std::atomic<BatchState> batch_state{BatchState::kNone};
    latch.reset(3); // a 3-partition batch was ordered
    batch_state.store(BatchState::kLoading);

    bool restore_or_probe_ran = false;
    auto pull_chunk_body = [&]() -> GateRoute {
        const GateRoute route = pull_gate_route(batch_state.load());
        if (route == GateRoute::kProceed) {
            restore_or_probe_ran = true; // init readers / restore / probe against the builders
        }
        return route;
    };

    // Failure: the load fails and the repair drives the latch all the way to ready (its completion duty),
    // publishing kFailed before the notify. Note the latch is ready now -- so latch.ready() alone cannot
    // tell a loaded batch from a failed one; only the carrier can.
    latch.count_down();
    latch.count_down();
    const bool latch_ready = latch.count_down();
    ASSERT_TRUE(latch_ready);
    ASSERT_TRUE(latch.ready()); // latch ready even though the batch failed
    batch_state.store(BatchState::kFailed);

    // The gate routes to kError (return the published _status()), and crucially not to kProceed: restore /
    // probe must not run against the unloaded builders.
    ASSERT_EQ(pull_chunk_body(), GateRoute::kError);
    ASSERT_FALSE(restore_or_probe_ran);
}

// Happy path, single-task and multi-task: the completion helper keeps the latch + lifetime + wakeup invariants.
// pending_finish() stays true from before the first submit until the last task's defer releases its slot.
TEST(SpillableProbeLatchTest, canon_happy_path_keeps_invariants) {
    for (size_t n : {size_t{1}, size_t{2}, size_t{3}}) {
        NoBlockCountDownLatch latch;
        std::atomic<BatchState> batch_state{BatchState::kNone};
        std::atomic_int32_t pending_latch_io{0};
        FakeSpillerIo spiller;
        WakeupRecorder obs;

        // Order an n-partition batch: kLoading + latch reset + lifetime slots claimed on the pipeline thread.
        latch.reset(n);
        batch_state.store(BatchState::kLoading);
        for (size_t i = 0; i < n; ++i) {
            pending_latch_io.fetch_add(1);
        }
        ASSERT_TRUE(pending_finish(batch_state.load(), pending_latch_io.load(), &spiller)); // kLoading

        // Tasks complete in submit order (their count_down order matches their fetch_sub order here).
        for (size_t i = 0; i < n; ++i) {
            const bool latch_ready = latch.count_down();
            if (latch_ready) {
                // The last count_down publishes kLoaded before the helper fires the wakeup. The lifetime
                // slot is still held: pending_finish must remain true even at kLoaded, because the data
                // wakeup lands while the driver is still pinned by _pending_latch_io.
                ASSERT_TRUE(latch.ready());
                batch_state.store(BatchState::kLoaded);
                ASSERT_TRUE(pending_finish(batch_state.load(), pending_latch_io.load(), &spiller));
            }
            complete_latch_slot_canon(latch_ready, pending_latch_io, obs);
        }

        ASSERT_TRUE(latch.ready());
        ASSERT_EQ(batch_state.load(), BatchState::kLoaded);
        ASSERT_EQ(pending_latch_io.load(), 0);
        ASSERT_FALSE(pending_finish(batch_state.load(), pending_latch_io.load(), &spiller));
        // One data wakeup (the last count_down) + one release (the zero-dropping fetch_sub).
        ASSERT_EQ(obs.total.load(), 2);
        // The release of the last completion observed the counter at zero: that is the wakeup that frees a
        // PENDING_FINISH driver. In this ordered case the latch-ready task is also the last fetch_sub, so the
        // gated form would coincidentally cover it -- the discriminating case below removes that coincidence.
        ASSERT_GE(obs.at_zero.load(), 1);
    }
}

// Probe-spiller restore/flush IO also pins pending_finish, independent of the batch carrier (kNone).
TEST(SpillableProbeLatchTest, pending_finish_tracks_spiller_io) {
    FakeSpillerIo spiller;
    ASSERT_FALSE(pending_finish(BatchState::kNone, 0, &spiller));

    spiller.in_flight.fetch_add(1);
    ASSERT_TRUE(pending_finish(BatchState::kNone, 0, &spiller));
    spiller.in_flight.fetch_sub(1);
    ASSERT_FALSE(pending_finish(BatchState::kNone, 0, &spiller));

    // Null-safe after close() resets the spiller.
    ASSERT_FALSE(pending_finish(BatchState::kNone, 0, nullptr));
}

// THE DIFFERENTIATING CASE.
//
// Force the adversarial interleaving the operator comment calls out: with N > 1 load tasks, drive all the
// count_down()s first (so the latch flips on some interior task), then release the lifetime slots in reverse
// order. The task that drops _pending_latch_io to zero is therefore the first one to count_down -- whose
// count_down() returned false (latch_ready == false). The release-at-zero is owned by a non-latch-ready
// task.
//
// Unconditional release: some completion fires a wakeup with the counter at zero -> a parked
// PENDING_FINISH driver is freed.
// Gated (buggy) release: the only task that fires a release is the latch-ready one, which released its slot
// while other slots were still held (counter > 0); when the counter finally hits zero no wakeup fires -> the
// driver wedges. We assert both, so the gated form is provably distinguishable (this test would fail on it).
TEST(SpillableProbeLatchTest, unconditional_releaser_wakes_when_last_fetch_sub_is_not_latch_ready) {
    const size_t n = 3;

    // Per-task latch_ready flags captured from count_down() in submit order. Exactly one is true.
    std::vector<bool> latch_ready_of_task(n, false);

    // --- CANON model ---
    {
        NoBlockCountDownLatch latch;
        std::atomic_int32_t pending_latch_io{0};
        WakeupRecorder obs;

        latch.reset(n);
        for (size_t i = 0; i < n; ++i) {
            pending_latch_io.fetch_add(1);
        }

        // Phase 1: every task counts down (latch flips on the last count_down). No slot released yet, so all
        // are still in flight -- this is the legal concurrent window.
        int32_t latch_ready_count = 0;
        for (size_t i = 0; i < n; ++i) {
            latch_ready_of_task[i] = latch.count_down();
            latch_ready_count += latch_ready_of_task[i] ? 1 : 0;
        }
        ASSERT_EQ(latch_ready_count, 1);         // exactly one task saw the latch go ready
        ASSERT_TRUE(latch_ready_of_task[n - 1]); // and it was the last to count_down
        ASSERT_FALSE(latch_ready_of_task[0]);    // the first task did not flip the latch
        ASSERT_TRUE(latch.ready());

        // Phase 2: release the lifetime slots in reverse order. The task that drops the counter to zero is
        // task 0, whose latch_ready is false.
        for (size_t k = 0; k < n; ++k) {
            const size_t i = n - 1 - k; // n-1, n-2, ..., 0
            complete_latch_slot_canon(latch_ready_of_task[i], pending_latch_io, obs);
        }

        ASSERT_EQ(pending_latch_io.load(), 0);
        // The completion helper fires a wakeup observing the counter at zero -- the release of task 0 (not
        // latch_ready).
        ASSERT_GE(obs.at_zero.load(), 1);
    }

    // --- gated (buggy) model: same interleaving, gated release ---
    {
        NoBlockCountDownLatch latch;
        std::atomic_int32_t pending_latch_io{0};
        WakeupRecorder obs;

        latch.reset(n);
        for (size_t i = 0; i < n; ++i) {
            pending_latch_io.fetch_add(1);
        }
        for (size_t i = 0; i < n; ++i) {
            latch_ready_of_task[i] = latch.count_down();
        }
        for (size_t k = 0; k < n; ++k) {
            const size_t i = n - 1 - k;
            complete_latch_slot_gated(latch_ready_of_task[i], pending_latch_io, obs);
        }

        ASSERT_EQ(pending_latch_io.load(), 0);
        // The gated form fired its single release when the latch-ready task (n-1) released its slot, while
        // tasks 0 and 1 were still in flight (counter == 2 at that point). When the counter finally reached
        // zero (task 0's release), no wakeup fired. This is the wedge the unconditional release fixes.
        ASSERT_EQ(obs.at_zero.load(), 0);
    }
}

// Submit-fail mid-loop: when submit fails at slot i of N, the repair publishes the error, publishes kFailed
// before its notify (publish-then-notify), counts the latch down for the un-submitted slots [i, N),
// releases the single outstanding lifetime slot (slot i) through the same completion helper, and -- if the
// latch reached ready -- the data wakeup fires; the unconditional release fires regardless. The probe never
// hangs on kLoading and finalize is allowed once the slot is released. We model the repair's kFailed
// publication and assert it is visible before any wakeup (so a woken driver routes through the kFailed gate,
// not into restore/probe against unloaded builders).
TEST(SpillableProbeLatchTest, submit_failure_repairs_latch_publishes_failed_and_wakes) {
    NoBlockCountDownLatch latch;
    std::atomic<BatchState> batch_state{BatchState::kNone};
    std::atomic_int32_t pending_latch_io{0};
    FakeSpillerIo spiller;

    // Records the BatchState observed at each wakeup, so the test can assert kFailed was published before
    // the first trigger (publish-then-notify).
    BatchState state_at_first_trigger = BatchState::kNone;
    std::atomic_int32_t trigger_count{0};
    auto record_trigger = [&]() {
        if (trigger_count.fetch_add(1) == 0) {
            state_at_first_trigger = batch_state.load();
        }
    };

    const size_t n = 4;
    const size_t fail_at = 1; // slots [0, 1) submitted ok; submit fails at slot 1.
    latch.reset(n);
    batch_state.store(BatchState::kLoading); // order time

    // Slots [0, fail_at) were submitted on the pipeline thread: each claimed its lifetime slot before submit
    // (fetch_add) and runs its count_down()+fetch_sub asynchronously inside its own task defer. Those defers
    // race with the repair below, so the operator holds all of [0, fail_at)'s increments live at submit time
    // and only releases them when the async defers actually run -- it does not release a slot the instant it
    // is claimed. Modelling a naive fetch_add-then-fetch_sub-per-slot would let a non-last interior slot see
    // _pending_latch_io == 0 and fire a release while still kLoading, which the real code never does because
    // the sibling slots' increments are still in flight. So claim all submitted slots up front and keep them
    // held across the repair.
    for (size_t i = 0; i < fail_at; ++i) {
        pending_latch_io.fetch_add(1);
    }
    // The submitted [0, fail_at) tasks count down (none is the last; the repair below counts the remainder),
    // so no data wakeup and no kLoaded publication here. Crucially their lifetime slots stay held -- the
    // fetch_sub for each runs in its own (still-pending) defer, after the repair.
    for (size_t i = 0; i < fail_at; ++i) {
        const bool latch_ready = latch.count_down();
        ASSERT_FALSE(latch_ready);
    }
    ASSERT_FALSE(latch.ready());

    // Slot fail_at is incremented just before its submit (which then fails).
    pending_latch_io.fetch_add(1);

    // Repair on the pipeline thread: publish error (modelled by the kFailed store), drive the latch down for
    // [fail_at, N). The kFailed publication happens before the wakeups.
    bool latch_ready = false;
    for (size_t j = fail_at; j < n; ++j) {
        latch_ready = latch.count_down();
    }
    ASSERT_TRUE(latch_ready);
    batch_state.store(BatchState::kFailed); // publish kFailed before notify

    // Repair completes slot fail_at: _complete_latch_slot's data wakeup (latch_ready) + slot-release fetch_sub
    // (matches operator lines ~520-528). The submitted [0, fail_at) slots are still held, so this fetch_sub
    // cannot drop the counter to zero: the data wakeup fires with kFailed already published, and the release
    // does not fire here.
    if (latch_ready) {
        record_trigger(); // data wakeup, slot still held; observes kFailed
    }
    if (pending_latch_io.fetch_sub(1) == 1) {
        record_trigger(); // release, only if this drops to zero (it does not -- siblings still held)
    }
    ASSERT_GT(pending_latch_io.load(), 0); // the [0, fail_at) slots still pin the driver

    // The async defers of the submitted [0, fail_at) slots finally run (after the repair), releasing their
    // held increments. The last one to run drops the counter to zero and fires the release -- after kFailed.
    for (size_t i = 0; i < fail_at; ++i) {
        if (pending_latch_io.fetch_sub(1) == 1) {
            record_trigger(); // release at zero, observes kFailed
        }
    }

    ASSERT_TRUE(latch.ready());
    ASSERT_EQ(pending_latch_io.load(), 0);
    ASSERT_EQ(batch_state.load(), BatchState::kFailed);
    // Publish-then-notify: the first wakeup observed kFailed already published, so a woken driver routes
    // through the kFailed gate (returns the error), never into restore/probe against unloaded builders.
    ASSERT_GE(trigger_count.load(), 1);
    ASSERT_EQ(state_at_first_trigger, BatchState::kFailed);
    // pending_finish converges: the batch left kLoading (kFailed is not pinned by the batch term) and the
    // lifetime slot is released; only the spiller-IO term could hold it, and there is none here.
    ASSERT_FALSE(pending_finish(batch_state.load(), pending_latch_io.load(), &spiller));
}

} // namespace starrocks::pipeline
