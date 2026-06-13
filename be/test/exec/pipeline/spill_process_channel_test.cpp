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

#include "exec/pipeline/spill_process_channel.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>

#include "base/testutil/assert.h"
#include "compute_env/spill/options.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/spiller_factory.h"
#include "exec/pipeline/primitives/pipeline_observer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

class CountingObserver final : public pipeline::PipelineObserver {
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

// A task that returns a single empty chunk then EOF.
SpillProcessTask make_eof_task() {
    auto produced = std::make_shared<bool>(false);
    return SpillProcessTask([produced]() -> StatusOr<ChunkPtr> { return Status::EndOfFile("eos"); });
}

} // namespace

class SpillProcessChannelTest : public ::testing::Test {
public:
    void SetUp() override {
        _state.set_enable_event_scheduler(true);
        // A bare spiller: never prepared, so we must not call has_output()
        // (which reads the writer's is_full). We only exercise the channel's
        // count/notify/finishing mechanics, which route through the spiller's
        // observable.
        auto factory = spill::make_spilled_factory();
        spill::SpilledOptions opts;
        _spiller = factory->create(opts);
        _spiller->observable().subscribe_source(&_state, &_source_obs);
        _spiller->observable().subscribe_sink(&_state, &_sink_obs);
    }

    RuntimeState _state;
    std::shared_ptr<spill::Spiller> _spiller;
    CountingObserver _source_obs;
    CountingObserver _sink_obs;
};

// Enqueuing a task makes the channel non-empty and wakes the source list.
TEST_F(SpillProcessChannelTest, add_task_increments_count_and_wakes_source) {
    SpillProcessChannel channel;
    channel.set_spiller(_spiller);

    ASSERT_FALSE(channel.has_task());
    // Direct enqueue carries its own source wakeup: production callers
    // (aggregator, sorter, hash joiner) enqueue without going through
    // execute(), and the sleeping spill-process source has no other notifier
    // before the first IO exists. Count is published before the notify.
    channel.add_spill_task(make_eof_task());
    ASSERT_TRUE(channel.has_task());
    ASSERT_EQ(_source_obs.source_count.load(), 1);
}

// execute() on a working channel enqueues all tasks under its single lock via the *_locked mutators
// (calling the public add_* there would re-enter the non-reentrant _mutex and deadlock), then fires exactly
// one source wakeup after the lock is released.
TEST_F(SpillProcessChannelTest, execute_working_enqueues_then_wakes_source) {
    SpillProcessChannel channel;
    channel.set_spiller(_spiller);
    // Mark the channel working so execute() takes the queueing branch.
    channel.add_spill_task(make_eof_task());
    ASSERT_TRUE(channel.is_working());
    ASSERT_EQ(channel.has_task(), true);

    SpillProcessTasksBuilder builder(&_state);
    builder.then([](RuntimeState*) { return Status::OK(); });
    builder.finally([](RuntimeState*) { return Status::OK(); });

    int32_t source_before = _source_obs.source_count.load();
    ASSERT_OK(channel.execute(builder));

    // then(1) + finally(1) queued on top of the priming task.
    ASSERT_TRUE(channel.has_task());
    // execute() notifies the source list exactly once, regardless of how many tasks it queued: the
    // *_locked mutators do not notify, only execute()'s single post-lock notify does.
    ASSERT_EQ(_source_obs.source_count.load(), source_before + 1);
    // add_last_task published the terminal flag.
    ASSERT_TRUE(channel.is_finishing());

    // The priming task + then(1) + finally(1) are all queued: three tasks, drained one by one.
    int drained = 0;
    while (channel.acquire_spill_task()) {
        channel.on_current_task_finished();
        ++drained;
    }
    ASSERT_EQ(drained, 3);
}

// A drained task drops the count and wakes the sink list (the writer blocks
// on has_task()).
TEST_F(SpillProcessChannelTest, task_finished_decrements_count_and_wakes_sink) {
    SpillProcessChannel channel;
    channel.set_spiller(_spiller);

    channel.add_spill_task(make_eof_task());
    ASSERT_TRUE(channel.acquire_spill_task());
    ASSERT_TRUE(channel.current_task());
    ASSERT_TRUE(channel.has_task());

    int32_t sink_before = _sink_obs.sink_count.load();
    channel.on_current_task_finished();

    ASSERT_FALSE(channel.current_task());
    ASSERT_FALSE(channel.has_task());
    ASSERT_GT(_sink_obs.sink_count.load(), sink_before);
}

// acquire on an empty queue is a non-blocking no-op (the gate makes emptiness
// transient).
TEST_F(SpillProcessChannelTest, acquire_on_empty_queue_is_noop) {
    SpillProcessChannel channel;
    channel.set_spiller(_spiller);

    ASSERT_FALSE(channel.acquire_spill_task());
    ASSERT_FALSE(channel.current_task());
}

// Terminal: set_finishing publishes the flag and wakes the source list; once
// every task has drained the channel reports finished.
TEST_F(SpillProcessChannelTest, set_finishing_wakes_source_and_marks_finished) {
    SpillProcessChannel channel;
    channel.set_spiller(_spiller);

    channel.add_spill_task(make_eof_task());
    ASSERT_FALSE(channel.is_finished());

    int32_t source_before = _source_obs.source_count.load();
    channel.set_finishing();
    ASSERT_TRUE(channel.is_finishing());
    ASSERT_GT(_source_obs.source_count.load(), source_before);
    // Still a queued task: not finished yet.
    ASSERT_FALSE(channel.is_finished());

    ASSERT_TRUE(channel.acquire_spill_task());
    channel.on_current_task_finished();
    // Drained + finishing => finished.
    ASSERT_TRUE(channel.is_finished());
}

} // namespace starrocks
