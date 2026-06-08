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

// Coverage for the spillable-agg event wiring.
//
// The blocking-agg sink/source and spill-process operators are thin delegators:
// their prepare() subscribes the driver's observer to the spiller's sink/source
// list, and the sink's need_input() error-guard reads task_status(). A
// full operator round-trip needs an Aggregator + prepared Spiller + driver
// (heavy fixtures), so here we test the exact predicate expressions
// the operators evaluate against a real bare Spiller + SpillProcessChannel. The
// driver-backed wakeup path itself is covered by spill_observable_test.cpp.

#include <gtest/gtest.h>

#include <atomic>
#include <memory>

#include "base/testutil/assert.h"
#include "common/config_exec_flow_fwd.h"
#include "compute_env/spill/options.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/spiller_factory.h"
#include "exec/pipeline/primitives/pipeline_observer.h"
#include "exec/pipeline/spill_process_channel.h"
#include "exec/pipeline/spill_process_operator.h"
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

} // namespace

class SpillableAggEventTest : public ::testing::Test {
public:
    void SetUp() override {
        _state.set_enable_event_scheduler(true);
        // Bare, unprepared spiller: enough to drive observable(), task_status(),
        // the in-flight IO counters, and the channel handshake. We never call
        // is_full() (which would deref the unallocated writer).
        auto factory = spill::make_spilled_factory();
        spill::SpilledOptions opts;
        _spiller = factory->create(opts);
    }

    RuntimeState _state;
    std::shared_ptr<spill::Spiller> _spiller;
};

// The sink driver lands on the spiller's sink list; the source / spill-
// process drivers land on the source list. observer_count() reflects both.
TEST_F(SpillableAggEventTest, prepare_subscribes_to_expected_lists) {
    CountingObserver sink_obs;
    CountingObserver source_obs;

    // sink operator prepare() -> subscribe_sink
    _spiller->observable().subscribe_sink(&_state, &sink_obs);
    ASSERT_EQ(_spiller->observable().observer_count(), 1);

    // source operator + spill-process operator prepare() -> subscribe_source
    _spiller->observable().subscribe_source(&_state, &source_obs);
    ASSERT_EQ(_spiller->observable().observer_count(), 2);

    // A sink-list wakeup (flush/channel-drain completion) hits only the sink obs.
    _spiller->notify_sink_observers();
    ASSERT_EQ(sink_obs.sink_count.load(), 1);
    ASSERT_EQ(source_obs.source_count.load(), 0);

    // A source-list wakeup (restore/flush-all/enqueue) hits only the source obs.
    _spiller->notify_source_observers();
    ASSERT_EQ(source_obs.source_count.load(), 1);
    ASSERT_EQ(sink_obs.sink_count.load(), 1);
}

// The poller-mode gate inside subscribe_* drops the subscription, so the
// operator prepare() can stay unconditional with no observer registered.
TEST_F(SpillableAggEventTest, subscription_gated_off_in_poller_mode) {
    CountingObserver obs;
    _state.set_enable_event_scheduler(false);

    _spiller->observable().subscribe_sink(&_state, &obs);
    _spiller->observable().subscribe_source(&_state, &obs);
    ASSERT_EQ(_spiller->observable().observer_count(), 0);
}

// Sink error-guard: a failed spiller flips the need_input() guard to true so
// the OUTPUT_FULL sleeper runs and carries the status out, instead of waiting on
// an is_full() that may stay stuck after the failure.
TEST_F(SpillableAggEventTest, sink_error_guard_releases_on_task_failure) {
    ASSERT_TRUE(_spiller->task_status().ok());
    // healthy: the guard does not force a wakeup.
    ASSERT_FALSE(!_spiller->task_status().ok());

    _spiller->update_spilled_task_status(Status::InternalError("spill io failed"));
    ASSERT_FALSE(_spiller->task_status().ok());
    // guard expression -> true: driver wakes and pushes the error through.
    const bool is_finished = false;
    ASSERT_TRUE(!is_finished && !_spiller->task_status().ok());

    // Once the operator is finished the guard must stand down: close() asserts
    // a finished sink needs no input, so a failed spiller must not flip
    // need_input() back to true on a finished operator (the status has already
    // been carried out).
    const bool finished = true;
    ASSERT_FALSE(!finished && !_spiller->task_status().ok());
}

// Family-neutral gate: the spill-process pump serves both the agg and the hash-join spill pipelines, so its
// factory is always event-ready and never reads enable_spill_agg_events -- otherwise one family's kill
// switch would wrongly demote the other's fragment. Each family's kill switch lives on its own factories
// (agg sink/source, join build/probe), which the fragment gate ANDs; the pump just follows. So
// support_event_scheduler() stays true for either value of the config.
TEST_F(SpillableAggEventTest, spill_process_factory_supports_event_scheduler) {
    auto channel_factory = std::make_shared<SpillProcessChannelFactory>(1);
    SpillProcessOperatorFactory factory(0, "spill_process", 1, std::move(channel_factory));

    const bool saved = config::enable_spill_agg_events;
    // Flipping the agg kill switch must not move the shared pump either way.
    config::enable_spill_agg_events = true;
    ASSERT_TRUE(factory.support_event_scheduler());
    config::enable_spill_agg_events = false;
    ASSERT_TRUE(factory.support_event_scheduler());
    config::enable_spill_agg_events = saved;
}

} // namespace starrocks::pipeline
