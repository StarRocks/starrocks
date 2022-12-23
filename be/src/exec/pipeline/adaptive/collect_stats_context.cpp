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

#include "exec/pipeline/adaptive/collect_stats_context.h"

#include <utility>

#include "column/chunk.h"
#include "common/statusor.h"
#include "exec/pipeline/adaptive/utils.h"

namespace starrocks::pipeline {

/// BufferState.
bool BufferState::need_input(int32_t driver_seq) const {
    return true;
}

Status BufferState::push_chunk(int32_t driver_seq, ChunkPtr chunk) {
    size_t num_chunk_rows = chunk->num_rows();
    _ctx->_buffer_chunk_queue(driver_seq).emplace(std::move(chunk));
    size_t prev_num_rows = _num_rows.fetch_add(num_chunk_rows);

    if (prev_num_rows < _max_buffer_rows && prev_num_rows + num_chunk_rows >= _max_buffer_rows) {
        _ctx->_transform_state(CollectStatsStateEnum::PASSTHROUGH, _ctx->_sink_dop);
    }
    return Status::OK();
}

bool BufferState::has_output(int32_t driver_seq) const {
    return false;
}

StatusOr<ChunkPtr> BufferState::pull_chunk(int32_t driver_seq) {
    return Status::InternalError("Shouldn't call BufferState::pull_chunk");
}

Status BufferState::set_finishing(int32_t driver_seq) {
    int num_finished_seqs = ++_num_finished_seqs;
    if (num_finished_seqs == _ctx->_sink_dop) {
        int num_partial_rows = _ctx->_runtime_state->chunk_size() * _ctx->MAX_BLOCK_CHUNKS_PER_DRIVER;
        size_t adjusted_dop = _num_rows / num_partial_rows;
        adjusted_dop = compute_max_le_power2(adjusted_dop);
        adjusted_dop = std::max<size_t>(adjusted_dop, 1);
        adjusted_dop = std::min<size_t>(adjusted_dop, _ctx->_sink_dop);

        _ctx->_transform_state(CollectStatsStateEnum::MAPPING_POWER2, adjusted_dop);
    }

    return Status::OK();
}

bool BufferState::is_finished(int32_t driver_seq) const {
    return false;
}

/// PassthroughState.
PassthroughState::PassthroughState(CollectStatsContext* const ctx)
        : CollectStatsState(ctx),
          _in_chunk_queue_per_driver_seq(ctx->_sink_dop),
          _unpluging_per_driver_seq(ctx->_sink_dop) {}

bool PassthroughState::need_input(int32_t driver_seq) const {
    return _in_chunk_queue_per_driver_seq[driver_seq].size_approx() < MAX_PASSTHROUGH_CHUNKS_PER_DRIVER_SEQ;
}

Status PassthroughState::push_chunk(int32_t driver_seq, ChunkPtr chunk) {
    _in_chunk_queue_per_driver_seq[driver_seq].enqueue(std::move(chunk));
    return Status::OK();
}

bool PassthroughState::has_output(int32_t driver_seq) const {
    const auto& buffer_chunk_queue = _ctx->_buffer_chunk_queue(driver_seq);
    if (!buffer_chunk_queue.empty()) {
        return true;
    }

    size_t num_chunks = _in_chunk_queue_per_driver_seq[driver_seq].size_approx();
    auto& unpluging = _unpluging_per_driver_seq[driver_seq];
    if (unpluging) {
        if (num_chunks > 0) {
            return true;
        }
        unpluging = false;
        return false;
    } else if (num_chunks >= UNPLUG_THRESHOLD_PER_DRIVER_SEQ) {
        unpluging = true;
        return true;
    }

    if (_ctx->_is_finishing_per_driver_seq[driver_seq]) {
        return num_chunks > 0;
    }
    return false;
}

StatusOr<ChunkPtr> PassthroughState::pull_chunk(int32_t driver_seq) {
    auto& buffer_chunk_queue = _ctx->_buffer_chunk_queue(driver_seq);
    if (!buffer_chunk_queue.empty()) {
        auto chunk = std::move(buffer_chunk_queue.front());
        buffer_chunk_queue.pop();
        return chunk;
    }

    auto& passthrough_chunk_queue = _in_chunk_queue_per_driver_seq[driver_seq];
    ChunkPtr chunk = nullptr;
    passthrough_chunk_queue.try_dequeue(chunk);
    return chunk;
}

Status PassthroughState::set_finishing(int32_t driver_seq) {
    return Status::OK();
}

bool PassthroughState::is_finished(int32_t driver_seq) const {
    if (!_ctx->_is_finishing_per_driver_seq[driver_seq]) {
        return false;
    }

    const auto& buffer_chunk_queue = _ctx->_buffer_chunk_queue(driver_seq);
    const auto& passthrough_chunk_queue = _in_chunk_queue_per_driver_seq[driver_seq];
    return buffer_chunk_queue.empty() && passthrough_chunk_queue.size_approx() <= 0;
}

/// MappingPower2State.
void MappingPower2State::set_adjusted_dop(size_t adjusted_dop) {
    _adjusted_dop = adjusted_dop;

    _info_per_driver_seq.reserve(_adjusted_dop);
    for (int i = 0; i < _adjusted_dop; i++) {
        _info_per_driver_seq.emplace_back(i, _ctx->_runtime_state->chunk_size());
    }
}

bool MappingPower2State::need_input(int32_t driver_seq) const {
    return false;
}

Status MappingPower2State::push_chunk(int32_t driver_seq, ChunkPtr chunk) {
    return Status::InternalError("Shouldn't call MappingPower2State::push_chunk");
}

bool MappingPower2State::has_output(int32_t driver_seq) const {
    if (driver_seq >= _adjusted_dop) {
        return false;
    }

    const auto& [buffer_idx, accumulator] = _info_per_driver_seq[driver_seq];
    return buffer_idx < _ctx->_sink_dop || !accumulator.empty();
}

StatusOr<ChunkPtr> MappingPower2State::pull_chunk(int32_t driver_seq) {
    auto& [buffer_idx, accumulator] = _info_per_driver_seq[driver_seq];
    if (!accumulator.empty()) {
        return accumulator.pull();
    }

    while (buffer_idx < _ctx->_sink_dop) {
        auto& buffer_chunk_queue = _ctx->_buffer_chunk_queue(buffer_idx);
        while (!buffer_chunk_queue.empty()) {
            accumulator.push(std::move(buffer_chunk_queue.front()));
            buffer_chunk_queue.pop();
            if (!accumulator.empty()) {
                return accumulator.pull();
            }
        }

        buffer_idx += _adjusted_dop;
    }

    accumulator.finalize();
    return accumulator.pull();
}

Status MappingPower2State::set_finishing(int32_t driver_seq) {
    return Status::InternalError("Should already call MappingPower2State::set_finishing before");
}

bool MappingPower2State::is_finished(int32_t driver_seq) const {
    return !has_output(driver_seq);
}

/// CollectStatsContext.
CollectStatsContext::CollectStatsContext(RuntimeState* const runtime_state, size_t dop)
        : _sink_dop(dop),
          _source_dop(dop),
          _buffer_chunk_queue_per_driver_seq(dop),
          _is_finishing_per_driver_seq(dop),
          _runtime_state(runtime_state) {
    int max_buffer_rows = runtime_state->chunk_size() * dop * MAX_BLOCK_CHUNKS_PER_DRIVER;
    _state_payloads[CollectStatsStateEnum::BLOCK] = std::make_unique<BufferState>(this, max_buffer_rows);
    _state_payloads[CollectStatsStateEnum::PASSTHROUGH] = std::make_unique<PassthroughState>(this);
    _state_payloads[CollectStatsStateEnum::MAPPING_POWER2] = std::make_unique<MappingPower2State>(this);
    _set_state(CollectStatsStateEnum::BLOCK);
}

void CollectStatsContext::close(RuntimeState* state) {}

bool CollectStatsContext::need_input(int32_t driver_seq) const {
    return _state_ref()->need_input(driver_seq);
}

Status CollectStatsContext::push_chunk(int32_t driver_seq, ChunkPtr chunk) {
    return _state_ref()->push_chunk(driver_seq, std::move(chunk));
}

bool CollectStatsContext::has_output(int32_t driver_seq) const {
    return _state_ref()->has_output(driver_seq);
}

StatusOr<ChunkPtr> CollectStatsContext::pull_chunk(int32_t driver_seq) {
    return _state_ref()->pull_chunk(driver_seq);
}

// TODO: what about source operator short-circuit?
Status CollectStatsContext::set_finishing(int32_t driver_seq) {
    _is_finishing_per_driver_seq[driver_seq] = true;
    return _state_ref()->set_finishing(driver_seq);
}

bool CollectStatsContext::is_finished(int32_t driver_seq) const {
    return _state_ref()->is_finished(driver_seq);
}

bool CollectStatsContext::is_source_ready() const {
    return _state_ref() != _get_state(CollectStatsStateEnum::BLOCK);
}

CollectStatsStateRawPtr CollectStatsContext::_get_state(CollectStatsStateEnum state) const {
    return _state_payloads.at(state).get();
}
CollectStatsStateRawPtr CollectStatsContext::_state_ref() const {
    return _state.load();
}
void CollectStatsContext::_set_state(CollectStatsStateEnum state_enum) {
    _state = _get_state(state_enum);
}
void CollectStatsContext::_transform_state(CollectStatsStateEnum state_enum, size_t source_dop) {
    auto* next_state = _get_state(state_enum);
    next_state->set_adjusted_dop(source_dop);
    _source_dop = source_dop;
    _state = next_state;
}

CollectStatsContext::BufferChunkQueue& CollectStatsContext::_buffer_chunk_queue(int32_t driver_seq) {
    return _buffer_chunk_queue_per_driver_seq[driver_seq];
}

} // namespace starrocks::pipeline
