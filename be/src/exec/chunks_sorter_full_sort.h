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

#include "column/vectorized_fwd.h"
#include "exec/chunks_sorter.h"
#include "exec/sorting/merge.h"

namespace starrocks {
class ExprContext;
struct ChunksSorterFullSortProfiler {
    ChunksSorterFullSortProfiler(RuntimeProfile* runtime_profile, MemTracker* parent_mem_tracker)
            : profile(runtime_profile) {
        input_required_memory = ADD_COUNTER(profile, "InputRequiredMemory", TUnit::BYTES);
        num_sorted_runs = ADD_COUNTER(profile, "NumSortedRuns", TUnit::UNIT);
    }

    RuntimeProfile* profile{};
    RuntimeProfile::Counter* input_required_memory = nullptr;
    RuntimeProfile::Counter* num_sorted_runs = nullptr;
};
class ChunksSorterFullSort : public ChunksSorter {
public:
    /**
     * Constructor.
     * @param sort_exprs     The order-by columns or columns with expression. This sorter will use but not own the object.
     * @param is_asc         Orders on each column.
     * @param is_null_first  NULL values should at the head or tail.
     * @param size_of_chunk_batch  In the case of a positive limit, this parameter limits the size of the batch in Chunk unit.
     */
    ChunksSorterFullSort(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                         const std::vector<bool>* is_asc_order, const std::vector<bool>* is_null_first,
                         const std::string& sort_keys);
    ~ChunksSorterFullSort() override;

    // Append a Chunk for sort.
    Status update(RuntimeState* state, const ChunkPtr& chunk) override;
    Status do_done(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

    size_t get_output_rows() const override;

    int64_t mem_usage() const override;

    void setup_runtime(RuntimeState* state, starrocks::RuntimeProfile* profile,
                       MemTracker* parent_mem_tracker) override;

private:
    Status _partial_sort(RuntimeState* state);

protected:
    size_t _total_rows = 0; // Total rows of sorting data
    size_t _staging_unsorted_rows = 0;
    size_t _staging_unsorted_bytes = 0;
    std::vector<ChunkPtr> _staging_unsorted_chunks;
    ChunkPtr _unsorted_chunk;                   // Unsorted chunk, accumulate it to a larger chunk
    std::vector<ChunkUniquePtr> _sorted_chunks; // Partial sorted, but not merged
    RuntimeProfile* _runtime_profile = nullptr;
    MemTracker* _parent_mem_tracker = nullptr;
    std::unique_ptr<ObjectPool> _object_pool = nullptr;
    ChunksSorterFullSortProfiler* _profiler = nullptr;
};

} // namespace starrocks
