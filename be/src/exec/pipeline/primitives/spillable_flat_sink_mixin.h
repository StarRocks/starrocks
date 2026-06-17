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

#pragma once

#include <cstdint>
#include <optional>

#include "exec/pipeline/primitives/block_reason.h"

namespace starrocks::pipeline {

// CRTP mixin for the flat spillable sink edge shared by the agg-family blocking/partitionwise sinks and
// the hash-join build. Each of these sinks parks OUTPUT_FULL on the same two reasons, just reached
// through its own spiller/spill-channel pair. Derived supplies two private accessors that return that
// pair for its own path:
//
//   const ...&/Ptr _spiller();       // the operator's spill::Spiller
//   const ...&/Ptr _spill_channel(); // the operator's SpillProcessChannel
//
// and inherits the shared predicates below. The accessors go through static_cast<Derived>, so each
// operator keeps its own pointer path (_aggregator->spiller(), _agg_op->aggregator()->spiller(),
// _join_builder->spiller(), ...) where it already has it.
template <class Derived>
class SpillableFlatSinkMixin {
protected:
    // The single branching predicate that both need_input() and block_reason() read. It returns the reason
    // the sink is parked on while back-pressured (WAIT_CHANNEL for a queued spill-process task, WAIT_FLUSH
    // for a full writer), or nullopt when the sink is not parked on a spill block -- either the spiller
    // failed (the run carries the status out, so the operator is runnable, not blocked) or the sink is
    // finished. The order matters. task_status() is checked first, as the error guard: a failed spill task
    // can leave is_full()/has_task() stuck true while is_finished() never flips, so the OUTPUT_FULL sleeper
    // would miss a wakeup that already fired. is_finished() is checked second (need_input() also gates on
    // !is_finished()). Then a queued channel task back-pressures before a full writer, and the two reasons
    // are kept distinct. Each named branch passes its literal through named<R, kSpillSinkCoveredWakeups>():
    // a reason outside the shared sink-list coverage mask does not compile here, for every flat sink at
    // once, so spill_sink_block_reason() can only ever return a covered reason.
    std::optional<BlockReason> spill_sink_blocked_on() const {
        auto* self = static_cast<const Derived*>(this);
        if (!self->_spiller()->task_status().ok()) {
            return std::nullopt;
        }
        if (self->is_finished()) {
            return std::nullopt;
        }
        if (self->_spill_channel()->has_task()) {
            return named<BlockReason::WAIT_CHANNEL, kSpillSinkCoveredWakeups>();
        }
        if (self->_spiller()->is_full()) {
            return named<BlockReason::WAIT_FLUSH, kSpillSinkCoveredWakeups>();
        }
        return std::nullopt;
    }

    bool spill_sink_need_input() const {
        auto* self = static_cast<const Derived*>(this);
        return !self->is_finished() && !spill_sink_blocked_on().has_value();
    }

    // value_or works on a runtime BlockReason, so named<>() (which guards a compile-time literal R) does
    // not apply here; that guard lives on the literal returns inside spill_sink_blocked_on(), which this
    // only forwards. The runtime NONE default is the always-legal "not parked" answer.
    BlockReason spill_sink_block_reason() const { return spill_sink_blocked_on().value_or(BlockReason::NONE); }

    // Compile-time copy of the wakeup table shared by every flat spill sink: the prepare() sink-list
    // subscription covers exactly WAIT_FLUSH (full writer) and WAIT_CHANNEL (queued spill-process task).
    static constexpr uint32_t kSpillSinkCoveredWakeups =
            block_reason_bit(BlockReason::WAIT_FLUSH) | block_reason_bit(BlockReason::WAIT_CHANNEL);
};

} // namespace starrocks::pipeline
