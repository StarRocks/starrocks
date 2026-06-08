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

// Observer-wakeup coverage for the spilled sort source moved onto the event scheduler. Unlike the agg
// families (one source over one spiller), the sort source merges all partitions:
// SortContext::subscribe_source_to_spillers subscribes one source-driver observer to the source list of
// every partition spiller, so a flush-all ("partition ready") or restore completion of any partition wakes
// the single source. In the gathered parallel-merge branch M source drivers share one SortContext and each
// subscribes, so a partition emits M notifies (M observers x N spillers). This N-subscription fan-out is the
// shape this test pins.
//
// Real path vs model. The SortContext loop itself needs a SortContext + N ChunksSorters + a driver (heavy
// fixtures), and the source reaches has_output_data() on a bare spiller (derefs the unallocated reader). So,
// following the convention of this directory (spillable_agg_observer_wakeup_test), this test drives the
// exact subscription + emission path the SortContext loop runs -- subscribe each of N real bare Spillers'
// source lists to the source observer, then emit -- and asserts the parked source is woken by any partition
// and by the right list (source emission, not sink). SortContext::subscribe_source_to_spillers is the
// trivial for-loop over _chunks_sorter_partitions that issues exactly these per-spiller subscribe_source calls.

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <vector>

#include "common/config_exec_flow_fwd.h"
#include "compute_env/spill/options.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/spiller_factory.h"
#include "exec/pipeline/primitives/pipeline_observer.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

namespace {

// Records which trigger woke it (see spillable_agg_observer_wakeup_test). A parked source driver is woken by
// source_trigger(); the counters let a test assert the parked source was woken by the RIGHT trigger and not
// cross-woken by the wrong one.
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

class SpillableSortObserverWakeupTest : public ::testing::Test {
public:
    void SetUp() override {
        _state.set_enable_event_scheduler(true);
        // N bare, unprepared spillers -- one per partition. Enough to drive observable() subscribe/notify and
        // the in-flight IO counters. We never call is_full()/has_output_data() (which would deref the
        // unallocated writer/reader).
        auto factory = spill::make_spilled_factory();
        spill::SpilledOptions opts;
        for (int i = 0; i < kNumPartitions; ++i) {
            _partitions.push_back(factory->create(opts));
        }
    }

    // Replicate SortContext::subscribe_source_to_spillers: subscribe one source observer to every partition
    // spiller's source list.
    void subscribe_source_to_all(PipelineObserver* observer) {
        for (auto& spiller : _partitions) {
            spiller->observable().subscribe_source(&_state, observer);
        }
    }

    static constexpr int kNumPartitions = 4;
    RuntimeState _state;
    std::vector<std::shared_ptr<spill::Spiller>> _partitions;
};

// N-fan-out: one source observer is subscribed to every partition spiller, and a restore/flush completion of
// any partition wakes it. Each partition's source list holds exactly that one observer.
TEST_F(SpillableSortObserverWakeupTest, source_woken_by_any_partition) {
    CountingObserver source_obs;
    subscribe_source_to_all(&source_obs);
    for (auto& spiller : _partitions) {
        ASSERT_EQ(spiller->observable().observer_count(), 1);
    }

    // A restore / flush-all completion on each partition independently wakes the single source.
    for (int i = 0; i < kNumPartitions; ++i) {
        _partitions[i]->notify_source_observers();
        ASSERT_EQ(source_obs.source_count.load(), i + 1) << "partition " << i << " did not wake the source";
    }
    ASSERT_EQ(source_obs.sink_count.load(), 0); // never cross-woken onto the sink axis
}

// Source-list routing: a partition's source emission wakes the source observer and not a sink observer
// subscribed to the same partition (the WAIT_RESTORE source axis is list-disjoint from the sink axis).
TEST_F(SpillableSortObserverWakeupTest, source_emission_does_not_cross_wake_sink) {
    CountingObserver source_obs;
    CountingObserver sink_obs;
    subscribe_source_to_all(&source_obs);
    for (auto& spiller : _partitions) {
        spiller->observable().subscribe_sink(&_state, &sink_obs);
    }

    _partitions[0]->notify_source_observers();
    ASSERT_EQ(source_obs.source_count.load(), 1);
    ASSERT_EQ(sink_obs.sink_count.load(), 0); // sink not cross-woken by a source emission

    _partitions[0]->notify_sink_observers();
    ASSERT_EQ(sink_obs.sink_count.load(), 1);
    ASSERT_EQ(source_obs.source_count.load(), 1); // source not moved by the sink emission
}

// Gathered parallel-merge: M source drivers share one SortContext, each subscribes to all N partitions, so a
// single partition's completion emits M notifies -- one per gathered source observer. None is dropped.
TEST_F(SpillableSortObserverWakeupTest, gathered_m_drivers_each_woken_by_one_partition) {
    constexpr int kM = 3;
    std::vector<std::unique_ptr<CountingObserver>> observers;
    for (int m = 0; m < kM; ++m) {
        observers.push_back(std::make_unique<CountingObserver>());
        subscribe_source_to_all(observers.back().get());
    }
    // M observers x N partitions: every partition spiller's source list now holds M observers.
    for (auto& spiller : _partitions) {
        ASSERT_EQ(spiller->observable().observer_count(), kM);
    }

    // One partition completing wakes ALL M gathered source drivers.
    _partitions[1]->notify_source_observers();
    for (int m = 0; m < kM; ++m) {
        ASSERT_EQ(observers[m]->source_count.load(), 1) << "gathered driver " << m << " was not woken";
    }
}

// Poller-mode gate: with the event scheduler off, subscribe_source drops the subscription (the gate lives
// inside SpillEventObservable::subscribe_source), so the SortContext loop registers nobody and a notify reaches
// no observer. Confirms the fan-out does not leak a wakeup onto a poller-mode source.
TEST_F(SpillableSortObserverWakeupTest, subscription_gated_off_in_poller_mode) {
    CountingObserver source_obs;
    _state.set_enable_event_scheduler(false);

    subscribe_source_to_all(&source_obs);
    for (auto& spiller : _partitions) {
        ASSERT_EQ(spiller->observable().observer_count(), 0);
    }

    for (auto& spiller : _partitions) {
        spiller->notify_source_observers();
    }
    ASSERT_EQ(source_obs.source_count.load(), 0);
}

// The sort fragment's event opt-in is gated on enable_spill_sort_events (default false; the fragment gate ANDs
// the sink factory's support_event_scheduler, so one false demotes the fragment to the poller without a
// rebuild). Binds the kill switch the e2e flips to the subscription path above.
TEST_F(SpillableSortObserverWakeupTest, sort_kill_switch_default_off) {
    ASSERT_FALSE(config::enable_spill_sort_events);
}

} // namespace starrocks::pipeline
