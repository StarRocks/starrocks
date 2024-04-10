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

#include <utility>

#include "exec/chunks_sorter.h"
#include "exec/chunks_sorter_full_sort.h"
namespace starrocks {
class SpillableChunksSorterFullSort : public ChunksSorterFullSort {
public:
    template <class... Args>
    SpillableChunksSorterFullSort(Args&&... args) : ChunksSorterFullSort(std::forward<Args>(args)...) {}
    ~SpillableChunksSorterFullSort() noexcept override = default;

    bool is_full() override { return (_spiller != nullptr && _spiller->is_full()) || _spill_channel->has_task(); }

    bool has_pending_data() override { return _spiller != nullptr && _spiller->has_pending_data(); }
    void setup_runtime(RuntimeState* state, RuntimeProfile* profile, MemTracker* parent_mem_tracker) override;

    Status update(RuntimeState* state, const ChunkPtr& chunk) override;
    Status do_done(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;
    size_t reserved_bytes(const ChunkPtr& chunk) override;

    void cancel() override;

    size_t get_output_rows() const override;

private:
    void _update_revocable_mem_bytes();

    std::function<StatusOr<ChunkPtr>()> _spill_process_task();

    Status _get_result_from_spiller(ChunkPtr* chunk, bool* eos);

    // used in spill
    // index for _staging_unsorted_chunk_idx
    size_t _process_staging_unsorted_chunk_idx = 0;
    // index for _sorted_chunk_idx
    size_t _process_sorted_chunk_idx = 0;
    // index for _early_materialized_chunks
    size_t _process_early_materialized_chunks_idx = 0;
};
} // namespace starrocks