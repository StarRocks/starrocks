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

#include "exec/chunks_sorter_full_sort.h"
#include "exec/spill/executor.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "exec/spillable_chunks_sorter_sort.h"

namespace starrocks {
void SpillableChunksSorterFullSort::setup_runtime(RuntimeProfile* profile, MemTracker* parent_mem_tracker) {
    ChunksSorterFullSort::setup_runtime(profile, parent_mem_tracker);
    _spiller->set_metrics(spill::SpillProcessMetrics(profile));
}

Status SpillableChunksSorterFullSort::update(RuntimeState* state, const ChunkPtr& chunk) {
    if (_spill_strategy == spill::SpillStrategy::NO_SPILL) {
        RETURN_IF_ERROR(ChunksSorterFullSort::update(state, chunk));
        _update_revocable_mem_bytes();
        return Status::OK();
    }

    // force spill
    bool first_time_spill = _spiller->spilled_append_rows() == 0;
    CHECK(!_spill_channel->has_task());

    RETURN_IF_ERROR(_spiller->spill(state, chunk, io_executor(), RESOURCE_TLS_MEMTRACER_GUARD(state)));

    if (first_time_spill) {
        auto process_task = _spill_process_task();
        while (!_spiller->is_full()) {
            auto chunk_st = process_task();
            if (chunk_st.ok()) {
                if (!chunk_st.value()->is_empty()) {
                    RETURN_IF_ERROR(_spiller->spill(state, chunk_st.value(), io_executor(),
                                                    RESOURCE_TLS_MEMTRACER_GUARD(state)));
                }
            } else if (chunk_st.status().is_end_of_file()) {
                return Status::OK();
            } else {
                return chunk_st.status();
            }
        }
        _spill_channel->add_spill_task({std::move(process_task)});
    }

    return Status::OK();
}

Status SpillableChunksSorterFullSort::do_done(RuntimeState* state) {
    if (_spill_strategy == spill::SpillStrategy::NO_SPILL) {
        return ChunksSorterFullSort::do_done(state);
    }

    if (_sorted_chunks.empty() && _unsorted_chunk == nullptr) {
        // force flush
        RETURN_IF_ERROR(_spiller->flush(state, io_executor(), RESOURCE_TLS_MEMTRACER_GUARD(state)));
    } else {
        // TODO: avoid sort multi times
        // spill sorted chunks
        auto spill_process_task = _spill_process_task();
        _spill_channel->add_spill_task({std::move(spill_process_task)});
        std::function<StatusOr<ChunkPtr>()> flush_task = [this, state]() -> StatusOr<ChunkPtr> {
            RETURN_IF_ERROR(_spiller->flush(state, io_executor(), RESOURCE_TLS_MEMTRACER_GUARD(state)));
            return Status::EndOfFile("eos");
        };
        _spill_channel->add_spill_task({std::move(flush_task)});
    }

    return Status::OK();
}

void SpillableChunksSorterFullSort::cancel() {
    ChunksSorterFullSort::cancel();
    if (_spill_strategy == spill::SpillStrategy::NO_SPILL) {
        // nothing TODO
    } else {
        if (_spill_channel->has_task()) {
            std::function<StatusOr<ChunkPtr>()> cancel_task = [this]() -> StatusOr<ChunkPtr> {
                _spiller->cancel();
                return Status::EndOfFile("eos");
            };
            _spill_channel->add_spill_task(std::move(cancel_task));
        } else {
            _spiller->cancel();
        }
    }
}

Status SpillableChunksSorterFullSort::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_spiller->spilled()) {
        return ChunksSorterFullSort::get_next(chunk, eos);
    }

    RETURN_IF_ERROR(_get_result_from_spiller(chunk, eos));

    return Status::OK();
}

size_t SpillableChunksSorterFullSort::reserved_bytes(const ChunkPtr& chunk) {
    if (chunk) {
        return chunk->memory_usage() + (_unsorted_chunk != nullptr ? _unsorted_chunk->memory_usage() * 2 : 0);
    }
    return _unsorted_chunk != nullptr ? _unsorted_chunk->memory_usage() * 2 : 0;
}

size_t SpillableChunksSorterFullSort::get_output_rows() const {
    if (!_spiller->spilled()) {
        return ChunksSorterFullSort::get_output_rows();
    }
    return _spiller->spilled_append_rows();
}

void SpillableChunksSorterFullSort::_update_revocable_mem_bytes() {
    size_t revocable_mem_bytes = 0;
    if (auto unsorted_chunk = _unsorted_chunk) {
        revocable_mem_bytes += unsorted_chunk->memory_usage();
    }

    for (const auto& chunk : _sorted_chunks) {
        if (chunk) {
            revocable_mem_bytes += chunk->memory_usage();
        }
    }

    _revocable_mem_bytes = revocable_mem_bytes;
}

std::function<StatusOr<ChunkPtr>()> SpillableChunksSorterFullSort::_spill_process_task() {
    return [this]() -> StatusOr<ChunkPtr> {
        if (_unsorted_chunk != nullptr) {
            return std::move(_unsorted_chunk);
        }

        if (_process_staging_unsorted_chunk_idx != _staging_unsorted_chunks.size()) {
            return std::move(_staging_unsorted_chunks[_process_staging_unsorted_chunk_idx++]);
        }

        if (_process_sorted_chunk_idx != _sorted_chunks.size()) {
            return std::move(_sorted_chunks[_process_sorted_chunk_idx++]);
        }
        return Status::EndOfFile("eos");
    };
}

Status SpillableChunksSorterFullSort::_get_result_from_spiller(ChunkPtr* chunk, bool* eos) {
    auto chunk_st = _spiller->restore(_state, io_executor(), RESOURCE_TLS_MEMTRACER_GUARD(_state));
    if (chunk_st.status().is_end_of_file()) {
        *eos = true;
    }
    RETURN_IF_ERROR(chunk_st.status());
    *chunk = std::move(chunk_st.value());
    return Status::OK();
}

} // namespace starrocks