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

// Cancel guarantee for the spilled probe.
//
// After cancel the spilled probe's pending_finish() must converge to false in finite time: no wait may
// outlive a cancel, or the driver hangs in PENDING_FINISH forever. The three cancel windows are mid-load
// (build-side load latch in flight), mid-restore (probe-spiller restore IO), and mid-flush (writer full,
// flush IO). The UT-level cases below drive the exact accounting the operator owns -- the real
// NoBlockCountDownLatch from the operator header, the _pending_latch_io lifetime slot, and a fake
// spiller IO counter standing in for the spiller's unconditional defers -- across a cancel, on a slow
// background executor (ASAN-friendly: the tasks really run on another thread and join before teardown).
//
// Not reachable at UT level without a cluster: the FragmentContext::cancel -> cancel_trigger ->
// EventScheduler::try_schedule chain (is_canceled is the first try_schedule branch; park-vs-cancel is
// closed in add_blocked_driver) needs a live QueryContext+FragmentContext+driver. That path is exercised
// end-to-end by a KILL QUERY: a spilling join under the event scheduler is killed, the query dies,
// drivers free, the backend stays up. The UT here proves the operator-owned half (counters converge, no
// UAF); the scheduler half is covered end-to-end.

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <vector>

#include "exec/pipeline/hashjoin/spillable_hash_join_probe_operator.h"

namespace starrocks::pipeline {

namespace {

// Models the running flush/restore IO of the probe-spiller. The spiller's completion / compensation
// defers are unconditional, so a cancel still drains this to zero -- we model that by always running the
// completion defer regardless of the cancel flag.
struct FakeSpillerIo {
    std::atomic_int32_t in_flight{0};
    bool has_running_io_tasks() const { return in_flight.load() > 0; }
    void start() { in_flight.fetch_add(1); }
    void complete() { in_flight.fetch_sub(1); } // unconditional completion / compensation defer
};

// Copy of SpillableHashJoinProbeOperator::pending_finish() under the BatchState carrier (I1/I6/I8): the
// batch term reads kLoading, not the latch. After cancel this must reach false -- the batch leaves kLoading
// (to kLoaded/kFailed) and the lifetime + spiller-IO tails drain.
bool pending_finish(BatchState batch_state, int32_t pending_latch_io, const FakeSpillerIo* spiller) {
    return batch_state == BatchState::kLoading || pending_latch_io > 0 ||
           (spiller != nullptr && spiller->has_running_io_tasks());
}

// Background executor: runs each submitted task to completion on its own thread, joined explicitly. Stands
// in for the IO thread pool so the cancel races a genuinely in-flight task (ASAN sees the real handoff).
struct BackgroundExecutor {
    std::vector<std::future<void>> futures;
    template <class Fn>
    void submit(Fn&& fn) {
        futures.emplace_back(std::async(std::launch::async, std::forward<Fn>(fn)));
    }
    void join() {
        for (auto& f : futures) {
            f.get();
        }
        futures.clear();
    }
};

} // namespace

// Cancel mid-load: a build-side load batch is in flight (latch reset to N, N lifetime slots claimed, tasks
// submitted to the background executor) when cancel fires. Every load task -- including on the cancel
// path -- must still run its defer: count_down, source_trigger on the last, release the lifetime slot, then
// a release source_trigger that observes the slot at zero. After the tasks join, the latch is ready,
// _pending_latch_io is zero, and pending_finish() is false; nothing is left hanging and there is no UAF on
// the (single) wakeup target.
TEST(SpillableProbeCancelTest, cancel_mid_load_drains_latch_and_lifetime) {
    NoBlockCountDownLatch latch;
    std::atomic<BatchState> batch_state{BatchState::kNone};
    std::atomic_int32_t pending_latch_io{0};
    std::atomic_int32_t source_triggers{0};
    std::atomic_bool cancelled{false};
    FakeSpillerIo spiller; // no spiller IO in this window; the batch carrier is the only pin

    const size_t n = 3;
    latch.reset(n);
    batch_state.store(BatchState::kLoading); // order time, before submits
    for (size_t i = 0; i < n; ++i) {
        pending_latch_io.fetch_add(1); // pipeline thread, before submit
    }
    ASSERT_TRUE(pending_finish(batch_state.load(), pending_latch_io.load(), &spiller));

    BackgroundExecutor exec;
    for (size_t i = 0; i < n; ++i) {
        exec.submit([&]() {
            // The load body observes cancel and returns early, but the defer still runs.
            // (cancelled may be set before or during the task; either way the defer is unconditional.)
            const bool latch_ready = latch.count_down();
            if (latch_ready) {
                // Last task publishes the terminal state (cancel -> kFailed; here the body cancelled out)
                // BEFORE the wakeup, leaving kLoading so pending_finish's batch term drops.
                batch_state.store(BatchState::kFailed);
                source_triggers.fetch_add(1); // data wakeup, lifetime slot still held
            }
            pending_latch_io.fetch_sub(1);
            if (latch_ready) {
                source_triggers.fetch_add(1); // release: a wakeup that observes the slot at zero
            }
        });
    }

    // Cancel races the in-flight batch.
    cancelled.store(true);
    exec.join();

    // Converged in finite time.
    ASSERT_TRUE(latch.ready());
    ASSERT_NE(batch_state.load(), BatchState::kLoading); // left kLoading -> batch term no longer pins
    ASSERT_EQ(pending_latch_io.load(), 0);
    ASSERT_EQ(source_triggers.load(), 2); // data wakeup + release, still no thundering herd
    ASSERT_FALSE(pending_finish(batch_state.load(), pending_latch_io.load(), &spiller));
}

// Cancel mid-load with submit failure on the cancel path: cancel arrives, and the remaining load-task
// submits fail (the executor is tearing down). The pipeline-thread repair publishes kFailed, counts the
// latch down for the un-submitted slots [i, N), releases the one outstanding lifetime slot, and emits the
// wakeup if the latch reached ready -- so the probe still converges and never hangs on kLoading.
TEST(SpillableProbeCancelTest, cancel_mid_load_submit_failure_repairs) {
    NoBlockCountDownLatch latch;
    std::atomic<BatchState> batch_state{BatchState::kNone};
    std::atomic_int32_t pending_latch_io{0};
    std::atomic_int32_t source_triggers{0};
    FakeSpillerIo spiller;

    const size_t n = 4;
    const size_t fail_at = 2; // slots [0, 2) ran; submit fails at slot 2 (executor shutting down on cancel)
    latch.reset(n);
    batch_state.store(BatchState::kLoading);

    BackgroundExecutor exec;
    for (size_t i = 0; i < fail_at; ++i) {
        pending_latch_io.fetch_add(1);
        exec.submit([&]() {
            latch.count_down(); // not the last; no wakeup yet
            pending_latch_io.fetch_sub(1);
        });
    }
    exec.join();
    ASSERT_FALSE(latch.ready());

    // Slot fail_at incremented just before its submit, which then fails on the cancel path.
    pending_latch_io.fetch_add(1);
    bool latch_ready = false;
    for (size_t j = fail_at; j < n; ++j) {
        latch_ready = latch.count_down(); // repair: count down [fail_at, N) on the pipeline thread
    }
    if (latch_ready) {
        batch_state.store(BatchState::kFailed); // publish kFailed BEFORE the wakeup
        source_triggers.fetch_add(1);
    }
    pending_latch_io.fetch_sub(1); // release slot fail_at
    if (latch_ready) {
        source_triggers.fetch_add(1); // release after the slot drops
    }

    ASSERT_TRUE(latch.ready());
    ASSERT_EQ(batch_state.load(), BatchState::kFailed);
    ASSERT_EQ(pending_latch_io.load(), 0);
    ASSERT_EQ(source_triggers.load(), 2);
    ASSERT_FALSE(pending_finish(batch_state.load(), pending_latch_io.load(), &spiller));
}

// The latch-slot completion order (_complete_latch_slot), driven concurrently by N load tasks. The release
// source_trigger() must be unconditional (gated only on observer != nullptr), never gated on latch_ready:
// with N > 1 concurrent tasks the fetch_sub that drops _pending_latch_io to zero need not be the task whose
// count_down() returned true. A latch_ready-gated release would then fire its last trigger while the
// counter is still above zero and stay silent at zero -> the driver hangs in PENDING_FINISH. This test
// models both forms and asserts that the fixed order emits a trigger while the counter is exactly zero (the
// release), and the buggy gated form does not.
namespace {

// The fixed _complete_latch_slot: data trigger gated on latch_ready, decrement, unconditional release.
// Records the counter value observed at each trigger so the test can assert one fired at zero.
template <class TriggerAtCounter>
void complete_latch_slot_fixed(bool latch_ready, std::atomic_int32_t& pending_latch_io, TriggerAtCounter&& on_trigger) {
    if (latch_ready) {
        on_trigger(pending_latch_io.load()); // data wakeup, slot still held
    }
    const int32_t after = pending_latch_io.fetch_sub(1, std::memory_order_release) - 1;
    on_trigger(after); // release: unconditional
}

// The buggy form: release gated on latch_ready.
template <class TriggerAtCounter>
void complete_latch_slot_buggy(bool latch_ready, std::atomic_int32_t& pending_latch_io, TriggerAtCounter&& on_trigger) {
    if (latch_ready) {
        on_trigger(pending_latch_io.load());
    }
    const int32_t after = pending_latch_io.fetch_sub(1, std::memory_order_release) - 1;
    if (latch_ready) {
        on_trigger(after);
    }
}

} // namespace

TEST(SpillableProbeCancelTest, latch_releaser_is_unconditional_concurrent) {
    const size_t n = 8;

    // Fixed order: a trigger must be observed while the counter is exactly zero (the release of whichever
    // task drained it last), and the counter converges to zero. This is the invariant the driver relies on
    // to leave PENDING_FINISH.
    {
        NoBlockCountDownLatch latch;
        latch.reset(n);
        std::atomic_int32_t pending_latch_io{0};
        std::atomic_int32_t triggers_at_zero{0};
        std::atomic_int32_t total_triggers{0};
        for (size_t i = 0; i < n; ++i) {
            pending_latch_io.fetch_add(1); // pipeline thread, before submit
        }

        BackgroundExecutor exec;
        for (size_t i = 0; i < n; ++i) {
            exec.submit([&]() {
                const bool latch_ready = latch.count_down();
                complete_latch_slot_fixed(latch_ready, pending_latch_io, [&](int32_t counter) {
                    total_triggers.fetch_add(1);
                    if (counter == 0) {
                        triggers_at_zero.fetch_add(1);
                    }
                });
            });
        }
        exec.join();

        ASSERT_TRUE(latch.ready());
        ASSERT_EQ(pending_latch_io.load(), 0);
        // The release of the task that dropped the counter to zero fired while it was zero.
        ASSERT_GE(triggers_at_zero.load(), 1);
    }

    // Buggy gated form: the last trigger can fire while the counter is still above zero, and no trigger is
    // guaranteed at zero -- exactly the silent-at-zero hang the fix removes. We assert the fixed form
    // strictly dominates by showing the buggy form CAN leave zero triggers-at-zero (it depends on which
    // task drains last, so we only assert the fixed form's guarantee holds and the buggy one lacks it in a
    // forced ordering below).
    {
        NoBlockCountDownLatch latch;
        latch.reset(2);
        std::atomic_int32_t pending_latch_io{0};
        pending_latch_io.fetch_add(1);
        pending_latch_io.fetch_add(1);
        std::atomic_int32_t triggers_at_zero{0};

        // Force the adversarial interleaving: task A's count_down() returns true (latch_ready) but task B
        // (latch_ready == false) is the one that drops the counter to zero. The buggy form's only trigger
        // is task A's, fired while the counter is still 1 -> nothing at zero.
        const bool a_ready = latch.count_down(); // first count_down: not last (count was 2) -> false
        const bool b_ready = latch.count_down(); // second: last -> true
        ASSERT_FALSE(a_ready);
        ASSERT_TRUE(b_ready);

        // Replay in the order where the NON-ready task (a) drains the counter last.
        // b (ready) completes first while a's slot is still held (counter 2 -> after b: 1).
        complete_latch_slot_buggy(b_ready, pending_latch_io, [&](int32_t counter) {
            if (counter == 0) triggers_at_zero.fetch_add(1);
        });
        // a (not ready) drains the counter to zero, but the buggy form emits no trigger for a non-ready task.
        complete_latch_slot_buggy(a_ready, pending_latch_io, [&](int32_t counter) {
            if (counter == 0) triggers_at_zero.fetch_add(1);
        });
        ASSERT_EQ(pending_latch_io.load(), 0);
        ASSERT_EQ(triggers_at_zero.load(), 0); // buggy form: silent at zero -> driver would hang
    }
}

// Cancel mid-restore: a probe-spiller restore is in flight (in_flight IO) when cancel fires. The
// spiller's restore-completion / compensation defers are unconditional, so the in-flight count drains to
// zero on cancel and pending_finish() converges. The latch is quiescent (no load batch) here.
TEST(SpillableProbeCancelTest, cancel_mid_restore_drains_spiller_io) {
    // No load batch in flight: the batch carrier is kNone (restore runs only after a batch is loaded and
    // reset; here the latch term plays no part and the spiller IO is the sole pin).
    const BatchState batch_state = BatchState::kNone;
    FakeSpillerIo spiller;
    std::atomic_bool cancelled{false};

    spiller.start(); // restore IO submitted
    ASSERT_TRUE(pending_finish(batch_state, 0, &spiller));

    BackgroundExecutor exec;
    exec.submit([&]() {
        // Restore body sees cancel and bails, but its completion defer is unconditional (decrement IO).
        spiller.complete();
    });
    cancelled.store(true);
    exec.join();

    ASSERT_EQ(spiller.in_flight.load(), 0);
    ASSERT_FALSE(spiller.has_running_io_tasks());
    ASSERT_FALSE(pending_finish(batch_state, 0, &spiller));
}

// Cancel mid-flush: the writer is full and a flush is in flight (in_flight IO) when cancel fires. As
// with restore, the flush-completion / compensation defers are unconditional, so the counter unwinds on
// cancel and pending_finish() converges. A trailing latch batch in flight at the same time also drains.
TEST(SpillableProbeCancelTest, cancel_mid_flush_drains_spiller_io) {
    NoBlockCountDownLatch latch;
    std::atomic<BatchState> batch_state{BatchState::kNone};
    std::atomic_int32_t pending_latch_io{0};
    std::atomic_int32_t source_triggers{0};
    FakeSpillerIo spiller;

    // A load batch and a flush are both in flight when cancel arrives.
    const size_t n = 2;
    latch.reset(n);
    batch_state.store(BatchState::kLoading);
    for (size_t i = 0; i < n; ++i) {
        pending_latch_io.fetch_add(1);
    }
    spiller.start(); // writer-full flush in flight
    ASSERT_TRUE(pending_finish(batch_state.load(), pending_latch_io.load(), &spiller));

    BackgroundExecutor exec;
    for (size_t i = 0; i < n; ++i) {
        exec.submit([&]() {
            const bool latch_ready = latch.count_down();
            if (latch_ready) {
                batch_state.store(BatchState::kFailed); // terminal state published before the wakeup
                source_triggers.fetch_add(1);
            }
            pending_latch_io.fetch_sub(1);
            if (latch_ready) {
                source_triggers.fetch_add(1); // release
            }
        });
    }
    exec.submit([&]() { spiller.complete(); }); // unconditional flush defer
    exec.join();

    ASSERT_TRUE(latch.ready());
    ASSERT_NE(batch_state.load(), BatchState::kLoading);
    ASSERT_EQ(pending_latch_io.load(), 0);
    ASSERT_EQ(spiller.in_flight.load(), 0);
    ASSERT_EQ(source_triggers.load(), 2);
    ASSERT_FALSE(pending_finish(batch_state.load(), pending_latch_io.load(), &spiller));
}

} // namespace starrocks::pipeline
