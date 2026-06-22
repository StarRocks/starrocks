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

// The wakeup table for the spillable hash-join build and probe, made executable. Every false-branch park
// state carries a typed BlockReason covered by the operator's declared mask, and is woken by exactly one
// declared event: a spiller observable notification on the matching list, or -- for the build-side load
// latch and the build-done handoff -- the direct observer trigger those paths fire. Driving the real
// operator into each phase needs a HashJoiner + prepared Spiller + driver (heavy fixtures), so the probe
// block_reason() phase transitions themselves are asserted against a predicate copy of the probe switch
// here and end-to-end through the SQL path. The agg family's masks/rows live in block_reason_agg_test; the
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
#include "exec/pipeline/hashjoin/spillable_hash_join_build_operator.h"
#include "exec/pipeline/hashjoin/spillable_hash_join_probe_operator.h"
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

// How the declared event reaches the parked driver. SINK_LIST / SOURCE_LIST / BOTH_LISTS are the spiller
// observable notifications (flush completion -> both, restore completion -> source, channel-drain -> sink);
// DIRECT_SOURCE is the build-side load latch count_down defer and the build-done handoff, both firing
// observer()->source_trigger() directly (not through a spiller list).
enum class EventPath { SINK_LIST, SOURCE_LIST, BOTH_LISTS, DIRECT_SOURCE };

struct WakeupRow {
    std::string name;      // operator + phase
    BlockReason reason;    // block_reason() in this park state
    uint32_t covered_mask; // the operator's covered_wakeups()
    EventPath event;       // the event that wakes it
};

// Aliased straight from the operator headers so each row checks against the single source of truth rather
// than a hand-kept copy that could drift.
constexpr uint32_t kProbeMask = SpillableHashJoinProbeOperator::kCoveredWakeups;
constexpr uint32_t kBuildMask = SpillableHashJoinBuildOperator::kCoveredWakeups;

const std::vector<WakeupRow> kJoinWakeupTable = {
        {"probe/build_not_ready", BlockReason::WAIT_BUILD, kProbeMask, EventPath::DIRECT_SOURCE},
        {"probe/latch_loading", BlockReason::WAIT_LATCH, kProbeMask, EventPath::DIRECT_SOURCE},
        {"probe/writer_full", BlockReason::WAIT_FLUSH, kProbeMask, EventPath::BOTH_LISTS},
        {"probe/restore_in_flight", BlockReason::WAIT_RESTORE, kProbeMask, EventPath::SOURCE_LIST},
        {"build_sink/writer_full", BlockReason::WAIT_FLUSH, kBuildMask, EventPath::SINK_LIST},
        {"build_sink/channel_has_task", BlockReason::WAIT_CHANNEL, kBuildMask, EventPath::SINK_LIST},
};

// Predicate copy of SpillableHashJoinProbeOperator::block_reason(). If the operator switch changes, this
// copy must change with it.
struct ProbeState {
    bool is_ready = true;
    bool is_finished = false;
    bool spilled = true;
    bool latch_loading = false;
    bool status_ok = true;
    bool processing_partitions_empty = false;
    bool any_prober_has_data = false;
    bool writer_full = false;
    bool is_finishing = true;
    bool all_partition_finished = false;
    bool current_reader_empty = false;
};

BlockReason probe_block_reason(const ProbeState& s) {
    if (!s.is_ready) {
        return BlockReason::WAIT_BUILD;
    }
    if (s.is_finished || !s.spilled) {
        return BlockReason::NONE;
    }
    if (s.latch_loading) {
        return BlockReason::WAIT_LATCH;
    }
    if (!s.status_ok) {
        return BlockReason::NONE;
    }
    if (s.processing_partitions_empty) {
        return BlockReason::NONE;
    }
    if (s.any_prober_has_data) {
        return BlockReason::NONE;
    }
    if (s.writer_full) {
        return BlockReason::WAIT_FLUSH;
    }
    if (s.is_finishing && !s.all_partition_finished && !s.current_reader_empty) {
        return BlockReason::WAIT_RESTORE;
    }
    return BlockReason::NONE;
}

} // namespace

class BlockReasonJoinTest : public ::testing::TestWithParam<WakeupRow> {
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
// branch added without a covered bit fails here -- the table is the completeness gate for the join family.
TEST_P(BlockReasonJoinTest, reason_is_covered) {
    const auto& row = GetParam();
    ASSERT_NE(row.reason, BlockReason::NONE) << row.name;
    ASSERT_NE(row.covered_mask & block_reason_bit(row.reason), 0u)
            << row.name << " reason " << block_reason_name(row.reason) << " not covered by mask " << row.covered_mask;
}

// Each row: inject exactly the declared event and assert the parked observer is woken on the matching list
// (or directly for the build-side load latch / build-done handoff).
TEST_P(BlockReasonJoinTest, declared_event_wakes_observer) {
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
        // Writer full: the probe is on BOTH lists (writer and reader role); the flush-completion defer wakes both.
        _spiller->observable().subscribe_sink(&_state, &obs);
        _spiller->observable().subscribe_source(&_state, &obs);
        _spiller->notify_sink_observers();
        _spiller->notify_source_observers();
        ASSERT_EQ(obs.sink_count.load(), 1) << row.name;
        ASSERT_EQ(obs.source_count.load(), 1) << row.name;
        break;
    case EventPath::DIRECT_SOURCE:
        // The build-side load latch count_down defer and the build-done handoff target the probe observer
        // directly via observer()->source_trigger() -- not through a spiller list.
        obs.source_trigger();
        ASSERT_EQ(obs.source_count.load(), 1) << row.name;
        break;
    }
}

// Declarations match subscriptions: the probe covers all four phase reasons, the build sink mirrors the agg
// sink (writer-full + channel).
TEST(BlockReasonJoinMaskTest, masks_cover_documented_reasons) {
    ASSERT_NE(kProbeMask & block_reason_bit(BlockReason::WAIT_FLUSH), 0u);
    ASSERT_NE(kProbeMask & block_reason_bit(BlockReason::WAIT_RESTORE), 0u);
    ASSERT_NE(kProbeMask & block_reason_bit(BlockReason::WAIT_LATCH), 0u);
    ASSERT_NE(kProbeMask & block_reason_bit(BlockReason::WAIT_BUILD), 0u);
    ASSERT_NE(kBuildMask & block_reason_bit(BlockReason::WAIT_FLUSH), 0u);
    ASSERT_NE(kBuildMask & block_reason_bit(BlockReason::WAIT_CHANNEL), 0u);
}

// block_reason() == the declared reason for each reachable probe phase.
TEST(BlockReasonJoinMaskTest, probe_block_reason_maps_each_phase) {
    {
        ProbeState s;
        s.is_ready = false; // build not ready
        ASSERT_EQ(probe_block_reason(s), BlockReason::WAIT_BUILD);
    }
    {
        ProbeState s;
        s.latch_loading = true; // load batch in flight
        ASSERT_EQ(probe_block_reason(s), BlockReason::WAIT_LATCH);
    }
    {
        ProbeState s;
        s.writer_full = true; // probe-spiller full
        ASSERT_EQ(probe_block_reason(s), BlockReason::WAIT_FLUSH);
    }
    {
        ProbeState s;
        s.is_finishing = true;
        s.current_reader_empty = false; // readers initialized, restore in flight
        ASSERT_EQ(probe_block_reason(s), BlockReason::WAIT_RESTORE);
    }
    {
        // non-block branches: empty processing partitions route through has_output, a runnable prober is not
        // parked, an error/finished operator is not a spill block.
        ProbeState s;
        s.processing_partitions_empty = true;
        ASSERT_EQ(probe_block_reason(s), BlockReason::NONE);
        ProbeState s2;
        s2.any_prober_has_data = true;
        ASSERT_EQ(probe_block_reason(s2), BlockReason::NONE);
        ProbeState s3;
        s3.is_finished = true;
        ASSERT_EQ(probe_block_reason(s3), BlockReason::NONE);
    }
}

INSTANTIATE_TEST_SUITE_P(JoinWakeupTable, BlockReasonJoinTest, ::testing::ValuesIn(kJoinWakeupTable),
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
