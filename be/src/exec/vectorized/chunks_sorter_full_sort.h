// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exprs/expr_context.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {
class ChunksSorterFullSort : public ChunksSorter {
public:
    /**
     * Constructor.
     * @param sort_exprs     The order-by columns or columns with expresion. This sorter will use but not own the object.
     * @param is_asc         Orders on each column.
     * @param is_null_first  NULL values should at the head or tail.
     * @param size_of_chunk_batch  In the case of a positive limit, this parameter limits the size of the batch in Chunk unit.
     */
    ChunksSorterFullSort(const std::vector<ExprContext*>* sort_exprs, const std::vector<bool>* is_asc,
                         const std::vector<bool>* is_null_first, size_t size_of_chunk_batch);
    ~ChunksSorterFullSort() override;

    // Append a Chunk for sort.
    Status update(RuntimeState* state, const ChunkPtr& chunk) override;
    Status done(RuntimeState* state) override;
    void get_next(ChunkPtr* chunk, bool* eos) override;
    bool pull_chunk(ChunkPtr* chunk) override;

    int64_t mem_usage() const override {
        int64_t usage = 0;
        if (_big_chunk != nullptr) {
            usage += _big_chunk->memory_usage();
        }
        if (_sorted_segment != nullptr) {
            usage += _sorted_segment->mem_usage();
        }
        usage += _sorted_permutation.capacity() * sizeof(Permutation);
        usage += _selective_values.capacity() * sizeof(uint32_t);
        return usage;
    }

    friend class SortHelper;

private:
    Status _sort_chunks(RuntimeState* state);
    Status _build_sorting_data(RuntimeState* state);

    void _sort_by_row_cmp();
    void _sort_by_columns();

    void _append_rows_to_chunk(Chunk* dest, Chunk* src, const Permutation& permutation, size_t offset, size_t count);

    ChunkUniquePtr _big_chunk;
    std::unique_ptr<DataSegment> _sorted_segment;
    Permutation _sorted_permutation;
    std::vector<uint32_t> _selective_values; // for appending selective values to sorted rows
};

} // namespace starrocks::vectorized
