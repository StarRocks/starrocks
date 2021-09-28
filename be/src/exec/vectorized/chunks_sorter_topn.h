// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exprs/expr_context.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {
// Sort Chunks in memory with specified order by rules.
class ChunksSorterTopn : public ChunksSorter {
public:
    /**
     * Constructor.
     * @param sort_exprs     The order-by columns or columns with expresion. This sorter will use but not own the object.
     * @param is_asc         Orders on each column.
     * @param is_null_first  NULL values should at the head or tail.
     * @param offset         Number of top rows to skip.
     * @param limit          Number of top rows after those skipped to extract. Zero means no limit.
     * @param size_of_chunk_batch  In the case of a positive limit, this parameter limits the size of the batch in Chunk unit.
     */
    ChunksSorterTopn(const std::vector<ExprContext*>* sort_exprs, const std::vector<bool>* is_asc,
                     const std::vector<bool>* is_null_first, size_t offset = 0, size_t limit = 0,
                     size_t size_of_chunk_batch = 1000);
    ~ChunksSorterTopn() override;

    // Append a Chunk for sort.
    Status update(RuntimeState* state, const ChunkPtr& chunk) override;
    // Finish seeding Chunk, and get sorted data with top OFFSET rows have been skipped.
    Status done(RuntimeState* state) override;
    // get_next only works after done().
    void get_next(ChunkPtr* chunk, bool* eos) override;
    // pull_chunk for pipeline.
    bool pull_chunk(ChunkPtr* chunk) override;

private:
    inline size_t _get_number_of_rows_to_sort() const { return _offset + _limit; }

    Status _sort_chunks(RuntimeState* state);

    // build data for top-n
    Status _build_sorting_data(RuntimeState* state, Permutation& permutation_second, DataSegments& segments);

    Status _hybrid_sort_first_time(RuntimeState* state, Permutation& new_permutation, DataSegments& segments,
                                   size_t sort_row_number);

    Status _hybrid_sort_common(RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation,
                               DataSegments& segments, size_t sort_row_number);

    void _merge_sort_common(ChunkPtr& big_chunk, DataSegments& segments, size_t sort_row_number, size_t sorted_size,
                            size_t permutation_size, Permutation& new_permutation);

    static void _sort_data_by_row_cmp(
            Permutation& permutation, size_t rows_to_sort, size_t rows_size,
            const std::function<bool(const PermutationItem& l, const PermutationItem& r)>& cmp_fn);

    static void _set_permutation_before(Permutation&, size_t size, std::vector<std::vector<uint8_t>>& filter_array);

    static void _set_permutation_complete(std::pair<Permutation, Permutation>&, size_t size,
                                          std::vector<std::vector<uint8_t>>& filter_array);

    Status _filter_and_sort_data_by_row_cmp(RuntimeState* state, std::pair<Permutation, Permutation>& permutation,
                                            DataSegments& segments, size_t batch_size);

    Status _merge_sort_data_as_merged_segment(RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation,
                                              DataSegments& segments);

    // buffer

    struct RawChunks {
        std::vector<ChunkPtr> chunks;
        size_t size_of_rows = 0;

        void clear() {
            chunks.clear();
            size_of_rows = 0;
        }
    };

    const size_t _offset;
    const size_t _limit;

    RawChunks _raw_chunks;

    bool _init_merged_segment;
    DataSegment _merged_segment;
};

} // namespace starrocks::vectorized
