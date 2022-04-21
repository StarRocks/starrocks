// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exprs/expr_context.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {
// Sort Chunks in memory with specified order by rules.
class ChunksSorterTopn : public ChunksSorter {
public:
    static constexpr size_t kMaxBufferedChunks = 512;
    static constexpr size_t kMinBufferedChunks = 16;
    static constexpr size_t kDefaultBufferedChunks = 64;

    // Tunning the max_buffer_chunks according to requested limit
    // The experiment could refer to https://github.com/StarRocks/starrocks/pull/4694.
    //
    // This parameter has at least two effects:
    // If smaller, the partial-merge-sort procedure would become more frequent, thus reduce the memory usage and
    // generate a baseline to filter input data.
    // If larger, the partial-merge-sort would become in-frequent, and cost lower, but the downside is the merge-sort
    // stage is expensive compared to the filter stage.
    //
    // As a result, we need to tunning this parameter to achieve better performance.
    // The followering heuristic is based on experiment of current algorithm implementation, which needs
    // further improvement if the algorithm changed.
    static constexpr size_t tunning_buffered_chunks(size_t limit) {
        if (limit <= 1024) {
            return 16;
        }
        if (limit <= 65536) {
            return 64;
        }
        return 256;
    }

    /**
     * Constructor.
     * @param sort_exprs     The order-by columns or columns with expression. This sorter will use but not own the object.
     * @param is_asc         Orders on each column.
     * @param is_null_first  NULL values should at the head or tail.
     * @param offset         Number of top rows to skip.
     * @param limit          Number of top rows after those skipped to extract. Zero means no limit.
     * @param max_buffered_chunks  In the case of a positive limit, this parameter limits the size of the batch in Chunk unit.
     */
    ChunksSorterTopn(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs, const std::vector<bool>* is_asc,
                     const std::vector<bool>* is_null_first, const std::string& sort_keys, size_t offset = 0,
                     size_t limit = 0, size_t max_buffered_chunks = kDefaultBufferedChunks);
    ~ChunksSorterTopn() override;

    // Append a Chunk for sort.
    Status update(RuntimeState* state, const ChunkPtr& chunk) override;
    // Finish seeding Chunk, and get sorted data with top OFFSET rows have been skipped.
    Status done(RuntimeState* state) override;
    // get_next only works after done().
    void get_next(ChunkPtr* chunk, bool* eos) override;
    DataSegment* get_result_data_segment() override;

    uint64_t get_partition_rows() const override;
    Permutation* get_permutation() const override;

    // pull_chunk for pipeline.
    bool pull_chunk(ChunkPtr* chunk) override;

    int64_t mem_usage() const override { return _raw_chunks.mem_usage() + _merged_segment.mem_usage(); }

private:
    size_t _get_number_of_rows_to_sort() const { return _offset + _limit; }

    Status _sort_chunks(RuntimeState* state);

    // build data for top-n
    Status _build_sorting_data(RuntimeState* state, Permutation& permutation_second, DataSegments& segments);

    Status _hybrid_sort_first_time(RuntimeState* state, Permutation& new_permutation, DataSegments& segments,
                                   size_t sort_row_number);

    Status _hybrid_sort_common(RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation,
                               DataSegments& segments, size_t sort_row_number);

    Status _merge_sort_common(ChunkPtr& big_chunk, DataSegments& segments, size_t sort_row_number, size_t sorted_size,
                              size_t permutation_size, Permutation& new_permutation);

    static void _set_permutation_before(Permutation&, size_t size, std::vector<std::vector<uint8_t>>& filter_array);

    static void _set_permutation_complete(std::pair<Permutation, Permutation>&, size_t size,
                                          std::vector<std::vector<uint8_t>>& filter_array);

    Status _filter_and_sort_data(RuntimeState* state, std::pair<Permutation, Permutation>& permutation,
                                 DataSegments& segments, size_t chunk_size);

    Status _merge_sort_data_as_merged_segment(RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation,
                                              DataSegments& segments);

    Status _partial_sort_col_wise(RuntimeState* state, std::pair<Permutation, Permutation>& permutations,
                                  DataSegments& segments, const size_t chunk_size, size_t number_of_rows_to_sort);

    // buffer

    struct RawChunks {
        std::vector<ChunkPtr> chunks;
        size_t size_of_rows = 0;

        int64_t mem_usage() const {
            int64_t usage = 0;
            for (auto& chunk : chunks) {
                usage += chunk->memory_usage();
            }
            return usage;
        }

        void clear() {
            chunks.clear();
            size_of_rows = 0;
        }
    };

    const size_t _offset;
    const size_t _limit;
    const size_t _max_buffered_chunks;

    RawChunks _raw_chunks;
    bool _init_merged_segment;
    DataSegment _merged_segment;
};

} // namespace starrocks::vectorized
