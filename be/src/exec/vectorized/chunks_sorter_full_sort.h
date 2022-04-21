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
                         const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first,
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
    Status _sort_chunks(RuntimeState* state);

    size_t _total_rows = 0;
    std::vector<ChunkPtr> _sorted_chunks; // Before merge
    SortedRuns _merged_runs;              // After merge
};

} // namespace vectorized
} // namespace starrocks
