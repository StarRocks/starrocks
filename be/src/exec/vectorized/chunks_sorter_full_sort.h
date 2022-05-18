// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/sorting/merge.h"
#include "gtest/gtest_prod.h"

namespace starrocks {
class ExprContext;

namespace vectorized {

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
    Status done(RuntimeState* state) override;
    void get_next(ChunkPtr* chunk, bool* eos) override;
    bool pull_chunk(ChunkPtr* chunk) override;

    SortedRuns get_sorted_runs() override;
    size_t get_output_rows() const override;

    int64_t mem_usage() const override;

private:
    // Three stages of sorting procedure:
    // 1. Accumulate input chunks into a big chunk(but not exceed the kMaxBufferedChunkSize), to reduce the memory copy during merge
    // 2. Sort the accumulated big chunk partially
    // 3. Merge all big-chunks into global sorted
    Status _merge_unsorted(RuntimeState* state, const ChunkPtr& chunk);
    Status _partial_sort(RuntimeState* state, bool done);
    Status _merge_sorted(RuntimeState* state);

    size_t _total_rows = 0;               // Total rows of sorting data
    Permutation _sort_permutation;        // Temp permutation for sorting
    ChunkPtr _unsorted_chunk;             // Unsorted chunk, accumulate it to a larger chunk
    std::vector<ChunkPtr> _sorted_chunks; // Partial sorted, but not merged
    SortedRuns _merged_runs;              // After merge

    // TODO: further tunning the buffer parameter
    static constexpr size_t kMaxBufferedChunkSize = 1024000;
};

} // namespace vectorized
} // namespace starrocks
