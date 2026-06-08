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

// CRTP mixin for the two-axis spillable partitionwise source edge shared by the partitionwise-agg and
// partitionwise-distinct sources. Both wrap a non-partitionwise blocking sub-op plus a ConjugateOperator,
// route every transient per-partition restore reader to one parent spiller, and park INPUT_EMPTY on the
// same two axes -- WAIT_FLUSH while the sink drains its final flush, WAIT_RESTORE while a per-partition
// restore task is in flight. Derived supplies five private accessors that return its own wrapped ops:
//
//   const std::shared_ptr<spill::Spiller>& _spiller(); // the parent spiller (non-pw sub-op's aggregator's)
//   bool _sink_complete();                              // non-pw sub-op's aggregator->is_sink_complete()
//   bool _conjugate_finished();                         // the wrapped ConjugateOperator's is_finished()
//   bool _non_pw_has_output();                          // the wrapped non-pw sub-op's has_output()
//   bool _non_pw_finished();                            // the wrapped non-pw sub-op's is_finished()
//
// and declares (friending this mixin) the identically-named state members the predicates read: the
// restore-loop cursor (_partitions, _curr_partition_idx, _curr_partition_reader, _curr_partition_eos) and
// _is_finished. has_output(), its parked-reason counterpart block_reason(), and is_finished() all live
// here, in one copy against one branch order, so the two sources stay in step -- a member renamed on only
// one of them fails to compile here.
template <class Derived>
class SpillablePartitionWiseSourceMixin {
protected:
    // Whether the source has a chunk to emit. It shares the spilled-region branch order with
    // spill_pw_source_blocked_on() below (that function is the parked-reason counterpart), but it also
    // delegates the non-spilled case to the wrapped sub-op and returns false on the terminal "all partitions
    // drained" branch (spill_pw_source_blocked_on() maps those to nullopt, not a park: is_finished() gates
    // them out before block_reason() is read). Keep the two in step.
    bool spill_pw_source_has_output() const {
        auto* self = static_cast<const Derived*>(this);
        const auto& spiller = self->_spiller();
        // Extra safety: a failed spiller is runnable, so the woken driver carries the status out through
        // pull_chunk's task_status() on its next turn instead of looping. (Even without this the error is not
        // lost -- a restore error fires complete_io -> notify_source -> the reader-runnable branch below ->
        // pull_chunk surfaces it; this just moves the check to the top of the predicate.)
        if (!spiller->task_status().ok()) {
            return true;
        }
        if (!spiller->spilled()) {
            return self->_non_pw_has_output();
        }
        // is_sink_complete() true means the sink finished all spilling tasks and the source can process spill
        // partitions one by one safely.
        if (!self->_sink_complete()) {
            return false;
        }
        // First call must pull_chunk to acquire all partitions.
        if (self->_partitions.empty()) {
            return true;
        }
        // All partitions processed: no data to output.
        if (self->_curr_partition_idx >= self->_partitions.size()) {
            return false;
        }
        // Reader not yet created, no async restore task in flight, or output already buffered: pull_chunk must
        // run to obtain a chunk from the current partition reader and push it into the conjugate.
        if (!self->_curr_partition_reader || !self->_curr_partition_reader->has_restore_task() ||
            self->_curr_partition_reader->has_output_data()) {
            return true;
        }
        // Current partition fully drained into the conjugate and the conjugate still has output to emit.
        if (self->_curr_partition_eos && !self->_conjugate_finished()) {
            return true;
        }
        return false;
    }

    // The parking-reason counterpart of has_output(). It is not computed from has_output(): it repeats
    // has_output()'s nested-state branch order by hand, so block_reason() answers exactly when has_output()
    // parks, and does not depend on has_output() having run first. Keep it in step with
    // spill_pw_source_has_output().
    std::optional<BlockReason> spill_pw_source_blocked_on() const {
        auto* self = static_cast<const Derived*>(this);
        const auto& spiller = self->_spiller();

        // A failed spiller is runnable, not parked: the woken driver carries the status out through
        // pull_chunk's task_status() on its next turn.
        if (!spiller->task_status().ok()) {
            return std::nullopt;
        }
        // Not spilled: the non-spilled base output is handled by the non-pw sub-op's has_output(); this is
        // not a spill block. The non-spilled path parks on NONE on purpose (matching the non-spilled
        // blocking source): the wakeup comes from the aggregator-pip source observable, and the named
        // WAIT_FLUSH reason is only declared once spilled() is true.
        if (!spiller->spilled()) {
            return std::nullopt;
        }
        // Sink still draining its final flush. Woken via the aggregator-pip defer_notify_source: the sink
        // and source share one Aggregator (memoized factory), so its pip-observable carries the
        // is_sink_complete wakeup, and prepare() attaches observer() to every sub-op before their prepare,
        // so this source registers on that shared observable.
        if (!self->_sink_complete()) {
            return named<BlockReason::WAIT_FLUSH, kSpillPwSourceCoveredWakeups>();
        }
        // First call still has to pull the partitions; runnable.
        if (self->_partitions.empty()) {
            return std::nullopt;
        }
        // All partitions processed: the source finishes (is_finished() == true here), it does not park.
        if (self->_curr_partition_idx >= self->_partitions.size()) {
            return std::nullopt;
        }
        // Reader absent, no restore task in flight, or output already buffered: runnable.
        if (!self->_curr_partition_reader || !self->_curr_partition_reader->has_restore_task() ||
            self->_curr_partition_reader->has_output_data()) {
            return std::nullopt;
        }
        // Current partition drained into the conjugate and the conjugate still has output to pull: runnable.
        if (self->_curr_partition_eos && !self->_conjugate_finished()) {
            return std::nullopt;
        }
        // Reader present, a restore task is in flight, no data yet: parked on per-partition restore IO. Both
        // this and the WAIT_FLUSH branch pass through named<R, kSpillPwSourceCoveredWakeups>(): a reason
        // outside this source's coverage mask does not compile, so the value block_reason() returns is
        // always a covered reason.
        return named<BlockReason::WAIT_RESTORE, kSpillPwSourceCoveredWakeups>();
    }

    // value_or works on a runtime BlockReason produced above, so named<>() (a compile-time literal R guard)
    // cannot wrap this; that guard lives on spill_pw_source_blocked_on()'s literal returns, which this only
    // forwards. The runtime NONE default is the always-legal "not parked" answer.
    BlockReason spill_pw_source_block_reason() const {
        return spill_pw_source_blocked_on().value_or(BlockReason::NONE);
    }

    // Finished once set_finished landed, or -- on the non-spilled path -- when the wrapped sub-op is finished,
    // or -- on the spilled path -- once every pulled partition has been processed.
    bool spill_pw_source_is_finished() const {
        auto* self = static_cast<const Derived*>(this);
        if (self->_is_finished) {
            return true;
        }
        if (!self->_spiller()->spilled()) {
            return self->_non_pw_finished();
        }
        return !self->_partitions.empty() && self->_curr_partition_idx >= self->_partitions.size();
    }

    // Compile-time copy of the wakeup table shared by every partitionwise source: the prepare() source-list
    // subscription on the parent spiller covers WAIT_RESTORE (restore IO completion), and the aggregator-pip
    // observable the sink shares covers WAIT_FLUSH (the sink's final flush).
    static constexpr uint32_t kSpillPwSourceCoveredWakeups =
            block_reason_bit(BlockReason::WAIT_FLUSH) | block_reason_bit(BlockReason::WAIT_RESTORE);
};

} // namespace starrocks::pipeline
