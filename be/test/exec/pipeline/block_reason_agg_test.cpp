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

// The wakeup table for the spillable aggregation operators, made executable. Every false-branch park state
// of the agg blocking sink/source carries a typed BlockReason covered by the operator's declared mask, and
// is woken by exactly one declared event injected onto a real bare Spiller's observable. Driving the real
// operator into each phase needs an Aggregator + prepared Spiller + driver (heavy fixtures), so the
// block_reason() phase transitions themselves are asserted against a predicate copy of the sink switch here
// and end-to-end through the SQL path. The join family's masks/rows live in block_reason_join_test; the
// BlockReason primitive and the spill-process mask in block_reason_test.

#include <gtest/gtest.h>

#include <atomic>
#include <cctype>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "compute_env/spill/options.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/spiller_factory.h"
#include "exec/pipeline/aggregate/spillable_aggregate_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/spillable_aggregate_blocking_source_operator.h"
#include "exec/pipeline/primitives/block_reason.h"
#include "exec/pipeline/primitives/pipeline_observer.h"
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

// How the declared event reaches the parked driver: a spiller observable notification on the matching list
// (flush completion -> both lists, restore completion -> source, channel-drain -> sink).
enum class EventPath { SINK_LIST, SOURCE_LIST, BOTH_LISTS };

struct WakeupRow {
    std::string name;      // operator + phase
    BlockReason reason;    // block_reason() in this park state
    uint32_t covered_mask; // the operator's covered_wakeups()
    EventPath event;       // the event that wakes it
};

// Aliased straight from the operator headers so each row checks against the single source of truth rather
// than a hand-kept copy that could drift.
constexpr uint32_t kAggSinkMask = SpillableAggregateBlockingSinkOperator::kCoveredWakeups;
constexpr uint32_t kAggSourceMask = SpillableAggregateBlockingSourceOperator::kCoveredWakeups;

const std::vector<WakeupRow> kAggWakeupTable = {
        {"agg_sink/writer_full", BlockReason::WAIT_FLUSH, kAggSinkMask, EventPath::SINK_LIST},
        {"agg_sink/channel_has_task", BlockReason::WAIT_CHANNEL, kAggSinkMask, EventPath::SINK_LIST},
        {"agg_source/restore", BlockReason::WAIT_RESTORE, kAggSourceMask, EventPath::SOURCE_LIST},
};

// Predicate copy of the agg/distinct blocking sink block_reason() (channel-first, then writer-full). If the
// operator switch changes, this copy must change with it.
BlockReason sink_block_reason(bool task_ok, bool is_finished, bool channel_has_task, bool is_full) {
    if (!task_ok || is_finished) {
        return BlockReason::NONE;
    }
    if (channel_has_task) {
        return BlockReason::WAIT_CHANNEL;
    }
    if (is_full) {
        return BlockReason::WAIT_FLUSH;
    }
    return BlockReason::NONE;
}

} // namespace

class BlockReasonAggTest : public ::testing::TestWithParam<WakeupRow> {
public:
    void SetUp() override {
        _state.set_enable_event_scheduler(true);
        auto factory = spill::make_spilled_factory();
        spill::SpilledOptions opts;
        _spiller = factory->create(opts);
    }

    RuntimeState _state;
    std::shared_ptr<spill::Spiller> _spiller;
};

// Each row: the reason is a real block (non-NONE) AND covered by the operator's declared mask. A new false
// branch added without a covered bit fails here -- the table is the completeness gate for the agg family.
TEST_P(BlockReasonAggTest, reason_is_covered) {
    const auto& row = GetParam();
    ASSERT_NE(row.reason, BlockReason::NONE) << row.name;
    ASSERT_NE(row.covered_mask & block_reason_bit(row.reason), 0u)
            << row.name << " reason " << block_reason_name(row.reason) << " not covered by mask " << row.covered_mask;
}

// Each row: inject exactly the declared event and assert the parked observer is woken on the matching list.
TEST_P(BlockReasonAggTest, declared_event_wakes_observer) {
    const auto& row = GetParam();
    CountingObserver obs;
    switch (row.event) {
    case EventPath::SINK_LIST:
        _spiller->observable().subscribe_sink(&_state, &obs);
        _spiller->notify_sink_observers();
        ASSERT_EQ(obs.sink_count.load(), 1) << row.name;
        ASSERT_EQ(obs.source_count.load(), 0) << row.name;
        break;
    case EventPath::SOURCE_LIST:
        _spiller->observable().subscribe_source(&_state, &obs);
        _spiller->notify_source_observers();
        ASSERT_EQ(obs.source_count.load(), 1) << row.name;
        ASSERT_EQ(obs.sink_count.load(), 0) << row.name;
        break;
    case EventPath::BOTH_LISTS:
        _spiller->observable().subscribe_sink(&_state, &obs);
        _spiller->observable().subscribe_source(&_state, &obs);
        _spiller->notify_sink_observers();
        _spiller->notify_source_observers();
        ASSERT_EQ(obs.sink_count.load(), 1) << row.name;
        ASSERT_EQ(obs.source_count.load(), 1) << row.name;
        break;
    }
}

// Declarations match subscriptions: the agg sink covers writer-full + channel, the source covers restore.
TEST(BlockReasonAggMaskTest, masks_cover_documented_reasons) {
    ASSERT_NE(kAggSinkMask & block_reason_bit(BlockReason::WAIT_FLUSH), 0u);
    ASSERT_NE(kAggSinkMask & block_reason_bit(BlockReason::WAIT_CHANNEL), 0u);
    ASSERT_EQ(kAggSinkMask & block_reason_bit(BlockReason::WAIT_RESTORE), 0u);
    ASSERT_EQ(kAggSourceMask, block_reason_bit(BlockReason::WAIT_RESTORE));
}

// block_reason() == the declared reason for the sink false branches (channel-held / writer-full).
TEST(BlockReasonAggMaskTest, sink_block_reason_maps_each_branch) {
    ASSERT_EQ(sink_block_reason(/*task_ok=*/true, /*finished=*/false, /*channel=*/true, /*full=*/false),
              BlockReason::WAIT_CHANNEL);
    ASSERT_EQ(sink_block_reason(true, false, false, true), BlockReason::WAIT_FLUSH);
    ASSERT_EQ(sink_block_reason(true, false, false, false), BlockReason::NONE);
    // A failed spiller or a finished sink is not a spill block (the error guard carries the status out).
    ASSERT_EQ(sink_block_reason(false, false, true, true), BlockReason::NONE);
    ASSERT_EQ(sink_block_reason(true, true, true, true), BlockReason::NONE);
}

INSTANTIATE_TEST_SUITE_P(AggWakeupTable, BlockReasonAggTest, ::testing::ValuesIn(kAggWakeupTable),
                         [](const ::testing::TestParamInfo<WakeupRow>& info) {
                             std::string n = info.param.name;
                             for (auto& c : n) {
                                 if (!std::isalnum(static_cast<unsigned char>(c))) {
                                     c = '_';
                                 }
                             }
                             return n;
                         });

} // namespace starrocks::pipeline
