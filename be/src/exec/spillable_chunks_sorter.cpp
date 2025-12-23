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
DEFINE_FAIL_POINT(chunk_sorter_spill_on_set_finishing);

template <DerivedFromChunksSorter TChunksSorter>
void SpillableChunksSorter<TChunksSorter>::setup_runtime(RuntimeState* state, RuntimeProfile* profile,
                                                         MemTracker* parent_mem_tracker) {
    TChunksSorter::setup_runtime(state, profile, parent_mem_tracker);
    _spiller->set_metrics(spill::SpillProcessMetrics(profile, state->mutable_total_spill_bytes()));
}

template <DerivedFromChunksSorter TChunksSorter>
Status SpillableChunksSorter<TChunksSorter>::update(RuntimeState* state, const ChunkPtr& chunk) {
    if (_spill_strategy == spill::SpillStrategy::NO_SPILL) {
        RETURN_IF_ERROR(TChunksSorter::update(state, chunk));
        _update_revocable_mem_bytes();
        return Status::OK();
    }
    // force spill
    auto guard = TRACKER_WITH_SPILLER_GUARD(state, _spiller);
    bool first_time_spill = _spiller->spilled_append_rows() == 0;
    CHECK(!_spill_channel->has_task());
    RETURN_IF_ERROR(_spiller->spill(state, chunk, guard));

    // first time spill, we need to spill the existing data in sorter
    if (first_time_spill) {
        auto process_task = TChunksSorter::_get_chunk_iterator();
        while (!_spiller->is_full()) {
            auto chunk_st = process_task();
            if (chunk_st.ok()) {
                if (!chunk_st.value()->is_empty()) {
                    RETURN_IF_ERROR(_spiller->spill(state, chunk_st.value(), guard));
                }
            } else if (chunk_st.status().is_end_of_file()) {
                return Status::OK();
            } else {
                return chunk_st.status();
            }
        }
        // spill buffer is full.
        // add spill task to spill channel
        _spill_channel->add_spill_task({std::move(process_task)});
    }

    return Status::OK();
}

template <DerivedFromChunksSorter TChunksSorter>
Status SpillableChunksSorter<TChunksSorter>::do_done(RuntimeState* state) {
    FAIL_POINT_TRIGGER_EXECUTE(chunk_sorter_spill_on_set_finishing,
                               { _spill_strategy = spill::SpillStrategy::SPILL_ALL; });

    if (_spill_strategy == spill::SpillStrategy::NO_SPILL) {
        return TChunksSorter::do_done(state);
    }

    // When spilling is enabled, the behavior at do_done() depends on whether
    // the underlying sorter still has staging (in-memory) data.
    if (TChunksSorter::_have_no_staging_data()) {
        // Case 1: no staging data remains.
        // Trigger a final flush to ensure all buffered spill data is written out.
        RETURN_IF_ERROR(_spiller->flush(state, TRACKER_WITH_SPILLER_GUARD(state, _spiller)));
    } else {
        // TODO: avoid sort multi times
        // spill sorted chunks
        auto spill_process_task = TChunksSorter::_get_chunk_iterator();
        _spill_channel->add_spill_task({std::move(spill_process_task)});
        std::function<StatusOr<ChunkPtr>()> flush_task = [this, state]() -> StatusOr<ChunkPtr> {
            RETURN_IF_ERROR(_spiller->flush(state, TRACKER_WITH_SPILLER_GUARD(state, _spiller)));
            return Status::EndOfFile("eos");
        };
        _spill_channel->add_spill_task({std::move(flush_task)});
    }

    return Status::OK();
}
template <DerivedFromChunksSorter TChunksSorter>
void SpillableChunksSorter<TChunksSorter>::cancel() {
    if (_spiller->spilled()) {
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

template <DerivedFromChunksSorter TChunksSorter>
Status SpillableChunksSorter<TChunksSorter>::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_spiller->spilled()) {
        return TChunksSorter::get_next(chunk, eos);
    }

    RETURN_IF_ERROR(_get_result_from_spiller(chunk, eos));

    return Status::OK();
}

template <DerivedFromChunksSorter TChunksSorter>
size_t SpillableChunksSorter<TChunksSorter>::reserved_bytes(const ChunkPtr& chunk) {
    size_t reserved = TChunksSorter::_reserved_bytes(chunk);
    if (chunk) {
        return chunk->memory_usage() + reserved;
    }
    return reserved;
}

template <DerivedFromChunksSorter TChunksSorter>
size_t SpillableChunksSorter<TChunksSorter>::get_output_rows() const {
    if (!_spiller->spilled()) {
        return this->TChunksSorter::get_output_rows();
    }
    return _spiller->spilled_append_rows();
}

template <DerivedFromChunksSorter TChunksSorter>
void SpillableChunksSorter<TChunksSorter>::_update_revocable_mem_bytes() {
    _revocable_mem_bytes = TChunksSorter::_get_revocable_mem_bytes();
}

template <DerivedFromChunksSorter TChunksSorter>
Status SpillableChunksSorter<TChunksSorter>::_get_result_from_spiller(ChunkPtr* chunk, bool* eos) {
    auto chunk_st = _spiller->restore(_state, TRACKER_WITH_SPILLER_GUARD(_state, _spiller));
    if (chunk_st.status().is_end_of_file()) {
        *eos = true;
    }
    RETURN_IF_ERROR(chunk_st.status());
    *chunk = std::move(chunk_st.value());
    return Status::OK();
}

template class SpillableChunksSorter<ChunksSorterFullSort>;
template class SpillableChunksSorter<ChunksSorterTopn>;

} // namespace starrocks