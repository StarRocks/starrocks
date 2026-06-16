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

// Coverage for the spillable hash-join build sink wiring + factory flips.
//
// The build sink is a thin delegator over its HashJoinBuilder's spiller + spill-process channel: its
// prepare() subscribes the driver observer to the spiller's sink list, and its need_input() carries an
// error guard reading task_status(). A full build round-trip needs a HashJoiner + prepared Spiller + driver
// (heavy fixtures; that path runs through the full executor). Following the spillable_agg_event_test
// convention, this test exercises the exact predicate expressions against a real bare Spiller +
// SpillProcessChannel, plus the config flip default. If an operator body changes, these copies must change
// with the operators.

#include <gtest/gtest.h>

#include <memory>

#include "base/testutil/assert.h"
#include "common/config_exec_flow_fwd.h"
#include "compute_env/spill/options.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/spiller_factory.h"
#include "exec/pipeline/primitives/pipeline_observer.h"
#include "exec/pipeline/spill_process_channel.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

namespace {

class CountingObserver final : public PipelineObserver {
public:
    void source_trigger() override { source_count++; }
    void sink_trigger() override { sink_count++; }
    void cancel_trigger() override { cancel_count++; }
    void all_trigger() override {
        source_count++;
        sink_count++;
    }
    void runtime_filter_timeout_trigger() override {}
    std::string debug_string() const override { return "CountingObserver"; }

    std::atomic_int32_t source_count{0};
    std::atomic_int32_t sink_count{0};
    std::atomic_int32_t cancel_count{0};
};

SpillProcessTask make_eof_task() {
    return SpillProcessTask([]() -> StatusOr<ChunkPtr> { return Status::EndOfFile("eos"); });
}

// Predicate copies of SpillableHashJoinBuildOperator bodies under test (the operator reads these
// through _join_builder->spiller() / spill_channel()).
bool build_need_input(spill::Spiller* spiller, SpillProcessChannel* channel, bool is_finished) {
    if (!is_finished && !spiller->task_status().ok()) {
        return true;
    }
    return !is_finished && !(spiller->is_full() || channel->has_task());
}
} // namespace

class SpillableHashJoinBuildEventTest : public ::testing::Test {
public:
    void SetUp() override {
        _state.set_enable_event_scheduler(true);
        // Bare, unprepared spiller: drives observable(), task_status(), the in-flight IO counters, and
        // the channel handshake. We never call is_full() (which would deref the unallocated writer).
        auto factory = spill::make_spilled_factory();
        spill::SpilledOptions opts;
        _spiller = factory->create(opts);
    }

    RuntimeState _state;
    std::shared_ptr<spill::Spiller> _spiller;
};

// The build sink prepare() lands the driver observer on the spiller's sink list; a sink-list
// wakeup (flush / channel-drain completion) hits it.
TEST_F(SpillableHashJoinBuildEventTest, prepare_subscribes_to_sink_list) {
    CountingObserver obs;
    _spiller->observable().subscribe_sink(&_state, &obs);
    ASSERT_EQ(_spiller->observable().observer_count(), 1);

    _spiller->notify_sink_observers();
    ASSERT_EQ(obs.sink_count.load(), 1);
    ASSERT_EQ(obs.source_count.load(), 0);
}

// The poller-mode gate inside subscribe_sink drops the subscription, so prepare() can stay
// unconditional with no observer registered.
TEST_F(SpillableHashJoinBuildEventTest, subscription_gated_off_in_poller_mode) {
    CountingObserver obs;
    _state.set_enable_event_scheduler(false);
    _spiller->observable().subscribe_sink(&_state, &obs);
    ASSERT_EQ(_spiller->observable().observer_count(), 0);
}

// Error guard: a healthy spiller leaves need_input() driven by is_full()/has_task(); a failed
// spiller forces need_input() true so the OUTPUT_FULL sleeper runs and carries the status out.
TEST_F(SpillableHashJoinBuildEventTest, need_input_error_guard) {
    SpillProcessChannel channel;
    channel.set_spiller(_spiller);

    // The error guard short-circuits BEFORE is_full(): that order is the point of the guard (the
    // OUTPUT_FULL sleeper must run on a failed spiller even though the writer state is stuck), and
    // it is also what makes this exercisable on the bare spiller -- is_full() on an unprepared
    // spiller derefs a null writer. The healthy is_full()/has_task() interplay needs a prepared
    // writer and is covered by the spill_test harness and the end-to-end path.
    _spiller->update_spilled_task_status(Status::InternalError("spill io failed"));
    ASSERT_FALSE(_spiller->task_status().ok());
    ASSERT_TRUE(build_need_input(_spiller.get(), &channel, /*is_finished=*/false));

    // The guard wins over channel back-pressure...
    channel.add_spill_task(make_eof_task());
    ASSERT_TRUE(channel.has_task());
    ASSERT_TRUE(build_need_input(_spiller.get(), &channel, /*is_finished=*/false));

    // ...but stands down on a finished operator: close() asserts a finished
    // sink needs no input, and the status has already been carried out.
    ASSERT_FALSE(build_need_input(_spiller.get(), &channel, /*is_finished=*/true));
}

// Config flip: the build/probe factory support_event_scheduler() returns the config; default false
// keeps the join on the poller (the all-factory fragment gate stays off without an explicit opt-in).
TEST_F(SpillableHashJoinBuildEventTest, mirrors_config_defaults_off) {
    ASSERT_FALSE(config::enable_spill_join_events);
}

} // namespace starrocks::pipeline
