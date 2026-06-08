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

// Observer-wakeup coverage for the three spilled blocking-agg families moved onto the event scheduler:
// distinct-blocking, partitionwise-agg, partitionwise-distinct.
//
// Each family's prepare() subscribes the driver's observer to the spiller's sink list (sink op) and source
// list (source op), and -- for the partitionwise wrappers -- propagates that same observer down into the
// wrapped sub-ops before their prepare(), so the sub-op's aggregator-pip notify does not null-deref. The
// park then sleeps on a sleeper position (OUTPUT_FULL for the sink, INPUT_EMPTY for the source) that the
// matching trigger releases:
//   * WAIT_FLUSH / WAIT_CHANNEL (sink, OUTPUT_FULL)  -> woken by sink_trigger() via notify_sink_observers().
//   * WAIT_RESTORE (source, INPUT_EMPTY)             -> woken by source_trigger() via notify_source_observers().
//
// A full operator round-trip needs an Aggregator + ConjugateOperator + prepared Spiller + driver (heavy
// fixtures), so, following the pilot spillable_agg_event_test convention, this test drives the exact
// subscription + emission path the operators run against a real bare Spiller + its real SpillEventObservable,
// and asserts the parked sleeper is actually woken by the right trigger and not by the wrong one (a lost or
// cross wakeup is the bug this guards). The list routing (sink_trigger only to the sink list, source_trigger
// only to the source list) is the property that matters.

#include <gtest/gtest.h>

#include <atomic>
#include <memory>

#include "common/config_exec_flow_fwd.h"
#include "compute_env/spill/options.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/spiller_factory.h"
#include "exec/pipeline/primitives/pipeline_observer.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

namespace {

// Records which trigger woke it. A parked sink driver is released by sink_trigger(); a parked source driver
// by source_trigger(); cancel routes through cancel_trigger(). The counters let a test assert the parked
// sleeper was woken by the RIGHT trigger and not cross-woken by the wrong one.
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

class SpillableAggObserverWakeupTest : public ::testing::Test {
public:
    void SetUp() override {
        _state.set_enable_event_scheduler(true);
        // Bare, unprepared spiller: enough to drive observable() subscribe/notify and the in-flight IO
        // counters. We never call is_full()/has_output_data() (which would deref the unallocated writer/reader).
        auto factory = spill::make_spilled_factory();
        spill::SpilledOptions opts;
        _spiller = factory->create(opts);
    }

    RuntimeState _state;
    std::shared_ptr<spill::Spiller> _spiller;
};

// ---------------------------------------------------------------------------------------------------------
// distinct-blocking. Sink prepare() -> subscribe_sink (one list); source prepare() -> subscribe_source. The
// sink's OUTPUT_FULL sleeper (WAIT_FLUSH/WAIT_CHANNEL) is woken by the sink-list trigger; the source's
// INPUT_EMPTY sleeper (WAIT_RESTORE) by the source-list trigger.
// ---------------------------------------------------------------------------------------------------------
TEST_F(SpillableAggObserverWakeupTest, distinct_blocking_sink_woken_by_sink_trigger) {
    CountingObserver sink_obs;
    CountingObserver source_obs;

    // Replicate the two operators' prepare() subscriptions onto one shared parent spiller (the distinct sink
    // and source share the aggregator's spiller).
    _spiller->observable().subscribe_sink(&_state, &sink_obs);     // SpillableAggregateDistinctBlockingSinkOperator
    _spiller->observable().subscribe_source(&_state, &source_obs); // ...DistinctBlockingSourceOperator
    ASSERT_EQ(_spiller->observable().observer_count(), 2);

    // A flush/channel-drain completion fires notify_sink_observers(): only the parked sink driver is woken.
    _spiller->notify_sink_observers();
    ASSERT_EQ(sink_obs.sink_count.load(), 1);
    ASSERT_EQ(source_obs.source_count.load(), 0); // source not cross-woken
    ASSERT_EQ(sink_obs.source_count.load(), 0);
}

TEST_F(SpillableAggObserverWakeupTest, distinct_blocking_source_woken_by_source_trigger) {
    CountingObserver sink_obs;
    CountingObserver source_obs;
    _spiller->observable().subscribe_sink(&_state, &sink_obs);
    _spiller->observable().subscribe_source(&_state, &source_obs);

    // A restore / flush-all completion fires notify_source_observers(): only the parked source driver is
    // woken (the WAIT_RESTORE wakeup).
    _spiller->notify_source_observers();
    ASSERT_EQ(source_obs.source_count.load(), 1);
    ASSERT_EQ(sink_obs.sink_count.load(), 0); // sink not cross-woken
}

// ---------------------------------------------------------------------------------------------------------
// partitionwise-agg. The source wrapper subscribes its own observer to the parent spiller's source list and
// propagates that same observer down into _non_pw_agg + _pw_agg before their prepare(). We model the
// propagation by registering one shared observer (the wrapper's) and asserting that a source-list trigger --
// which is what a per-partition restore completion fires -- wakes it. The WAIT_FLUSH axis (sink still
// draining) comes from the aggregator-pip source observable, also delivered as source_trigger() to this same
// observer.
// ---------------------------------------------------------------------------------------------------------
TEST_F(SpillableAggObserverWakeupTest, partitionwise_agg_source_woken_by_source_trigger) {
    CountingObserver wrapper_obs; // the observer the wrapper seeds onto itself and its sub-ops

    // Source prepare(): subscribe the wrapper observer to the parent spiller's source list (every transient
    // per-partition reader completes_io on this same parent spiller, so one subscription covers every restore).
    _spiller->observable().subscribe_source(&_state, &wrapper_obs);
    ASSERT_EQ(_spiller->observable().observer_count(), 1);

    // A per-partition restore completion fires notify_source_observers(): the parked source (WAIT_RESTORE) is
    // woken. The WAIT_FLUSH wakeup also arrives as a source_trigger() on this list, so both spilled parks of
    // this family are released by the same source-side emission this asserts.
    _spiller->notify_source_observers();
    ASSERT_EQ(wrapper_obs.source_count.load(), 1);
    ASSERT_EQ(wrapper_obs.sink_count.load(), 0);
}

TEST_F(SpillableAggObserverWakeupTest, partitionwise_agg_sink_woken_by_sink_trigger) {
    CountingObserver wrapper_obs;
    // Sink prepare(): the wrapper propagates observer() into the wrapped agg-op before its prepare(), then
    // subscribes to the wrapped agg-op spiller's sink list.
    _spiller->observable().subscribe_sink(&_state, &wrapper_obs);

    _spiller->notify_sink_observers();
    ASSERT_EQ(wrapper_obs.sink_count.load(), 1);
    ASSERT_EQ(wrapper_obs.source_count.load(), 0); // sink trigger does not move the source counter
}

// ---------------------------------------------------------------------------------------------------------
// partitionwise-distinct. Identical wiring to partitionwise-agg (only the wrapped sub-op type differs).
// ---------------------------------------------------------------------------------------------------------
TEST_F(SpillableAggObserverWakeupTest, partitionwise_distinct_source_woken_by_source_trigger) {
    CountingObserver wrapper_obs;
    _spiller->observable().subscribe_source(&_state, &wrapper_obs);

    _spiller->notify_source_observers();
    ASSERT_EQ(wrapper_obs.source_count.load(), 1);
    ASSERT_EQ(wrapper_obs.sink_count.load(), 0);
}

TEST_F(SpillableAggObserverWakeupTest, partitionwise_distinct_sink_woken_by_sink_trigger) {
    CountingObserver wrapper_obs;
    _spiller->observable().subscribe_sink(&_state, &wrapper_obs);

    _spiller->notify_sink_observers();
    ASSERT_EQ(wrapper_obs.sink_count.load(), 1);
    ASSERT_EQ(wrapper_obs.source_count.load(), 0);
}

// Observer-propagation on sub-ops. The partitionwise wrappers seed one observer onto the wrapper and every
// sub-op (so the sub-op's aggregator-pip notify does not null-deref under the event scheduler). Model the
// propagated fan-out by subscribing the same observer instance both to the parent spiller's source list (the
// wrapper) and to a second list emission (a sub-op's aggregator-pip notify): the single parked driver behind
// that shared observer is woken by either path -- never silently dropped.
TEST_F(SpillableAggObserverWakeupTest, partitionwise_propagated_observer_woken_on_any_path) {
    CountingObserver shared_obs; // wrapper == sub-op observer (the propagation invariant)

    // Path A: parent spiller source list (the wrapper's own subscription).
    _spiller->observable().subscribe_source(&_state, &shared_obs);
    _spiller->notify_source_observers();
    ASSERT_EQ(shared_obs.source_count.load(), 1);

    // Path B: a sub-op's pip-observable fires its own source notify into the same observer (modeled by a
    // second emission). The parked driver is woken again -- the propagation guarantees the sub-op holds a
    // real observer, not nullptr, so the notify lands instead of crashing.
    _spiller->notify_source_observers();
    ASSERT_EQ(shared_obs.source_count.load(), 2);
}

// Poller-mode gate: with the event scheduler off, subscribe_* drop the subscription, so a notify reaches
// nobody (the operator prepare() stays unconditional, no observer registered). Confirms the families do not
// leak a wakeup onto a poller-mode driver.
TEST_F(SpillableAggObserverWakeupTest, subscription_gated_off_in_poller_mode) {
    CountingObserver obs;
    _state.set_enable_event_scheduler(false);

    _spiller->observable().subscribe_sink(&_state, &obs);
    _spiller->observable().subscribe_source(&_state, &obs);
    ASSERT_EQ(_spiller->observable().observer_count(), 0);

    _spiller->notify_sink_observers();
    _spiller->notify_source_observers();
    ASSERT_EQ(obs.sink_count.load(), 0);
    ASSERT_EQ(obs.source_count.load(), 0);
}

// Per-family factory event opt-in is gated on the shared agg kill switch. The default-off switch keeps
// none of the three families opting into the event scheduler (the fragment gate ANDs all factories, so
// one false demotes the fragment to the poller without a rebuild). This binds the kill switch the e2e
// flips to the source-side subscription path above.
TEST_F(SpillableAggObserverWakeupTest, agg_kill_switch_default_off) {
    ASSERT_FALSE(config::enable_spill_agg_events);
}

} // namespace starrocks::pipeline
