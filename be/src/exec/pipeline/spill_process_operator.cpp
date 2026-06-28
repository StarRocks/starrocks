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

#include "exec/pipeline/spill_process_operator.h"

#include <memory>

#include "compute_env/spill/mem_tracker_guard.h"
#include "compute_env/spill/spiller.hpp"
#include "exec/pipeline/spill_process_channel.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status SpillProcessOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    // The pump sleeps INPUT_EMPTY on has_output() (has_task && !is_full), so it belongs on the spiller's
    // source list: flush completion notifies both lists, and the channel handshake (enqueue / set_finishing)
    // wakes the source side. The spiller is set on the channel before prepare (create() wired it). The gate
    // for poller mode is inside subscribe_source.
    if (_channel->spiller() != nullptr) {
        _channel->spiller()->observable().subscribe_source(state, observer());
    }
    return Status::OK();
}

bool SpillProcessOperator::has_output() const {
    return !_is_finished && _channel->has_output();
}

bool SpillProcessOperator::is_finished() const {
    return _is_finished || _channel->is_finished();
}

std::optional<BlockReason> SpillProcessOperator::_blocked_on() const {
    // Read only when the pump is parked (has_output() == false). A finished channel is not a block.
    // Otherwise either the channel has no task (waiting on the writer's enqueue, WAIT_CHANNEL), or a task is
    // queued but the spiller writer is full (waiting on flush/restore IO, WAIT_RESTORE). The full-writer
    // case is named WAIT_RESTORE here, not the sink's WAIT_FLUSH for the same is_full(), because this pump
    // is a source woken off the spiller's source list, and WAIT_RESTORE is the reason that list covers.
    // Null-safe on the spiller (close() resets it; when it is absent the writer is not full -> nullopt,
    // matching the channel's own null-spiller park).
    if (_is_finished || _channel->is_finished()) {
        return std::nullopt;
    }
    // Both named branches pass through named<R, kCoveredWakeups>(): a reason outside this pump's coverage
    // mask does not compile, so the value block_reason() returns is always a covered reason.
    if (!_channel->has_task()) {
        return named<BlockReason::WAIT_CHANNEL, kCoveredWakeups>();
    }
    if (_channel->spiller() != nullptr && _channel->spiller()->is_full()) {
        return named<BlockReason::WAIT_RESTORE, kCoveredWakeups>();
    }
    return std::nullopt;
}

BlockReason SpillProcessOperator::block_reason() const {
    return _blocked_on().value_or(BlockReason::NONE);
}

Status SpillProcessOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

void SpillProcessOperator::close(RuntimeState* state) {
    _channel->close();
    SourceOperator::close(state);
}

StatusOr<ChunkPtr> SpillProcessOperator::pull_chunk(RuntimeState* state) {
    if (!_channel->current_task()) {
        // acquire_spill_task is now non-blocking. An empty queue is only transient (has_output gates us,
        // and the writer's enqueue notify reschedules us), so return a no-op instead of an error; a
        // genuinely closed channel shows up through is_finished()/set_finished() upstream.
        bool res = _channel->acquire_spill_task();
        if (!res) {
            return nullptr;
        }
    }
    DCHECK(_channel->current_task());

    auto chunk_st = _channel->current_task()();
    if (chunk_st.status().ok() && !state->is_cancelled()) {
        const auto& chunk = chunk_st.value();
        if (chunk != nullptr && !chunk->is_empty()) {
            auto& spiller = _channel->spiller();
            RETURN_IF_ERROR(spiller->spill(state, chunk_st.value(), TRACKER_WITH_SPILLER_GUARD(state, spiller)));
        }
    } else if (chunk_st.status().is_end_of_file()) {
        // The task drained; drop it from the channel count and wake the writer
        // blocked on has_task().
        _channel->on_current_task_finished();
    } else if (!chunk_st.status().ok()) {
        return chunk_st.status();
    }

    return nullptr;
}

OperatorPtr SpillProcessOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto channel = _process_ctx->get_or_create(driver_sequence);
    auto processor = std::make_shared<SpillProcessOperator>(this, id(), get_raw_name(), plan_node_id(), driver_sequence,
                                                            std::move(channel));
    return processor;
}

} // namespace starrocks::pipeline
