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
#include "gtest/gtest_prod.h"

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
                         const std::string& sort_keys, int64_t max_buffered_rows, int64_t max_buffered_bytes,
                         const std::vector<SlotId>& early_materialized_slots);
    ~ChunksSorterFullSort() override;

    // Append a Chunk for sort.
    [[nodiscard]] Status update(RuntimeState* state, const ChunkPtr& chunk) override;
    [[nodiscard]] Status do_done(RuntimeState* state) override;
    [[nodiscard]] Status get_next(ChunkPtr* chunk, bool* eos) override;

    size_t get_output_rows() const override;

    int64_t mem_usage() const override;

    void setup_runtime(RuntimeState* state, starrocks::RuntimeProfile* profile,
                       MemTracker* parent_mem_tracker) override;

private:
    // Three stages of sorting procedure:
    // 1. Accumulate input chunks into a big chunk(but not exceed the kMaxBufferedChunkSize), to reduce the memory copy during merge
    // 2. Sort the accumulated big chunk partially
    // 3. Merge all big-chunks into global sorted
    [[nodiscard]] Status _merge_unsorted(RuntimeState* state, const ChunkPtr& chunk);
    [[nodiscard]] Status _partial_sort(RuntimeState* state, bool done);
    [[nodiscard]] Status _merge_sorted(RuntimeState* state);
    void _split_late_and_early_chunks();
    static constexpr SlotId ORDINAL_COLUMN_SLOT_ID = -2;
    void _assign_ordinals();
    template <typename T>
    void _assign_ordinals_tmpl();
    template <typename T>
    ChunkPtr _late_materialize_tmpl(const ChunkPtr& chunk);

protected:
    ChunkPtr _late_materialize(const ChunkPtr& chunk);

    size_t _total_rows = 0;        // Total rows of sorting data
    Permutation _sort_permutation; // Temp permutation for sorting
    size_t _staging_unsorted_rows = 0;
    size_t _staging_unsorted_bytes = 0;
    std::vector<ChunkPtr> _staging_unsorted_chunks;
    ChunkPtr _unsorted_chunk;                   // Unsorted chunk, accumulate it to a larger chunk
    std::vector<ChunkUniquePtr> _sorted_chunks; // Partial sorted, but not merged
    SortedRuns _merged_runs;                    // After merge
    RuntimeProfile* _runtime_profile = nullptr;
    MemTracker* _parent_mem_tracker = nullptr;
    std::unique_ptr<ObjectPool> _object_pool = nullptr;
    ChunksSorterFullSortProfiler* _profiler = nullptr;

    // TODO: further tunning the buffer parameter
    const size_t max_buffered_rows;  // Max buffer 1024000 rows
    const size_t max_buffered_bytes; // Max buffer 16MB bytes

    // only when order-by columns(_sort_exprs) are all ColumnRefs and the cost of eager-materialization of
    // other columns is large than ordinal column, then we materialize order-by columns and ordinal columns eagerly,
    // and other columns are lazy-materialized.
    std::unordered_set<SlotId> _early_materialized_slots;
    int _max_num_rows = 0;
    int _offset_in_chunk_bits = 0;
    int _chunk_idx_bits = 0;
    std::vector<SlotId> _column_id_to_slot_id;
    std::vector<ChunkUniquePtr> _early_materialized_chunks;
    std::vector<ChunkUniquePtr> _late_materialized_chunks;
};

} // namespace starrocks
