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
#include "exprs/expr_context.h"
#include "util/runtime_profile.h"

namespace starrocks {
// Sort Chunks in memory with specified order by rules.
class ChunksSorterTopn : public ChunksSorter {
public:
    static constexpr size_t kDefaultBufferedChunks = 64;
    static constexpr size_t kDefaultMaxBufferRows =
            1 << 30; // 1 billion rows, the number of rows has little impact on performance
    static constexpr size_t kDefaultMaxBufferBytes =
            256 << 20; // 256MB, a larger limit may improve performance but is not memory allocator friendly
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

    static constexpr size_t max_buffered_chunks(size_t rows_to_sort) {
        return std::max<size_t>(tunning_buffered_chunks(rows_to_sort), rows_to_sort / 4069);
    }

    /**
     * Constructor.
     * @param sort_exprs     The order-by columns or columns with expression. This sorter will use but not own the object.
     * @param is_asc_order         Orders on each column.
     * @param is_null_first  NULL values should at the head or tail.
     * @param offset         Number of top rows to skip.
     * @param limit          Number of top rows after those skipped to extract. Zero means no limit.
     * @param max_buffered_chunks  In the case of a positive limit, this parameter limits the size of the batch in Chunk unit.
     */
    ChunksSorterTopn(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                     const std::vector<bool>* is_asc_order, const std::vector<bool>* is_null_first,
                     const std::string& sort_keys, size_t offset = 0, size_t limit = 0,
                     const TTopNType::type topn_type = TTopNType::ROW_NUMBER,
                     size_t max_buffered_rows = kDefaultMaxBufferRows,
                     size_t max_buffered_bytes = kDefaultMaxBufferBytes,
                     size_t max_buffered_chunks = kDefaultBufferedChunks);
    ~ChunksSorterTopn() override;

    // Append a Chunk for sort.
    Status update(RuntimeState* state, const ChunkPtr& chunk) override;
    // Finish seeding Chunk, and get sorted data with top OFFSET rows have been skipped.
    Status do_done(RuntimeState* state) override;
    // get_next only works after done().
    Status get_next(ChunkPtr* chunk, bool* eos) override;

    size_t get_output_rows() const override;

    int64_t mem_usage() const override { return _raw_chunks.mem_usage + _merged_runs.mem_usage(); }

    void setup_runtime(RuntimeState* state, RuntimeProfile* profile, MemTracker* parent_mem_tracker) override;

    std::vector<RuntimeFilter*>* runtime_filters(ObjectPool* pool) override;

private:
    size_t _get_number_of_rows_to_sort() const { return _offset + _limit; }

    Status _sort_chunks(RuntimeState* state);

    // build data for top-n
    Status _build_sorting_data(RuntimeState* state, Permutation& permutation_second, DataSegments& segments);

    Status _hybrid_sort_first_time(RuntimeState* state, Permutation& new_permutation, DataSegments& segments);

    Status _hybrid_sort_common(RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation,
                               DataSegments& segments);

    Status _merge_sort_common(MergedRuns* dst, DataSegments& segments, const size_t rows_to_keep,
                              Permutation& new_permutation);

    static void _set_permutation_before(Permutation&, size_t size, std::vector<std::vector<uint8_t>>& filter_array);

    static void _set_permutation_complete(std::pair<Permutation, Permutation>&, size_t size,
                                          std::vector<std::vector<uint8_t>>& filter_array);

    Status _filter_and_sort_data(RuntimeState* state, std::pair<Permutation, Permutation>& permutations,
                                 DataSegments& segments);

    Status _merge_sort_data_as_merged_segment(RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation,
                                              DataSegments& segments);

    Status _partial_sort_col_wise(RuntimeState* state, std::pair<Permutation, Permutation>& permutations,
                                  DataSegments& segments);

    // 1. compare each row in the segment with the highest element in merged sort runs, and assign INCLUDE_IN_SEGMENT to LE elements.
    // 2. compare each row in the segment with the lowest element in merged sort runs, and assign SMALLER_THAN_MIN_OF_SEGMENT to LT elements.
    Status _build_filter_from_high_low_comparison(const DataSegments& segments,
                                                  std::vector<std::vector<uint8_t>>& filter_array,
                                                  const SortDescs& sort_descs, uint32_t& least_num,
                                                  uint32_t& middle_num);
    // compare each row in the segment with the lowest element in merged sort runs, and assign SMALLER_THAN_MIN_OF_SEGMENT to LT elements.
    Status _build_filter_from_low_comparison(const DataSegments& segments,
                                             std::vector<std::vector<uint8_t>>& filter_array,
                                             const SortDescs& sort_descs, uint32_t& least_num, uint32_t& middle_num);
    // For rank type topn, it may keep more data than we need during processing,
    // therefor, pruning should be performed when processing is finished
    // For example, given the sorted set [1, 2, 3, 3, 3, 4, 5] with limit = 3,
    // the last two element [4, 5] should be pruned
    void _rank_pruning();

    const MergedRun& _lowest_merged_run() const { return _merged_runs.front(); }

    std::pair<const MergedRun*, int> _get_run_by_row_id(size_t rid) const {
        size_t index = 0;
        size_t skip_offset = rid;
        while (index < _merged_runs.num_chunks() && skip_offset >= 0) {
            const auto& run = _merged_runs.at(index);
            if (skip_offset >= run.num_rows()) {
                skip_offset -= run.num_rows();
            } else {
                return {&run, skip_offset};
            }
            index++;
        }
        return {nullptr, 0};
    }

    // buffer
    struct RawChunks {
        std::vector<ChunkPtr> chunks;
        size_t size_of_rows = 0;
        size_t mem_usage = 0;

        void update_mem_usage(size_t delta) { mem_usage += delta; }

        void clear() {
            chunks.clear();
            size_of_rows = 0;
            mem_usage = 0;
        }
    };
    const size_t _max_buffered_rows;
    const size_t _max_buffered_bytes;
    const size_t _max_buffered_chunks;
    size_t _init_buffered_chunks;
    RawChunks _raw_chunks;
    bool _init_merged_segment;
    MergedRuns _merged_runs;

    const size_t _limit;
    const size_t _offset;
    const TTopNType::type _topn_type;

    int _highest_nozero_pos(size_t val) {
        if (val == 0) {
            return 0;
        }
        return (sizeof(size_t) * 8) - __builtin_clzll(val) - 1;
    }

    void _adjust_chunks_capacity(bool inc) {
        if (inc) {
            size_t shift = (_highest_nozero_pos(_max_buffered_chunks) - _highest_nozero_pos(_init_buffered_chunks)) / 4;
            shift = std::max<size_t>(shift, 1);
            _buffered_chunks_capacity = _buffered_chunks_capacity << shift;
            _buffered_chunks_capacity = std::min(_buffered_chunks_capacity, _max_buffered_chunks);
        }
    }

    size_t _buffered_chunks_capacity;

    std::vector<RuntimeFilter*> _runtime_filter;

    RuntimeProfile::Counter* _sort_filter_rows = nullptr;
    RuntimeProfile::Counter* _sort_filter_timer = nullptr;
};

} // namespace starrocks
