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

#include "exec/chunks_sorter_distinct_topn.h"

namespace starrocks {
ChunksSorterDistinctTopn::~ChunksSorterDistinctTopn() = default;

Status ChunksSorterDistinctTopn::_sort_chunks(RuntimeState* state) {
    // Chunks for this batch.
    DataSegments segments;

    // Sermutations for this batch:
    // if _init_merged_segment == false, means this is first batch:
    //     permutations.first is empty, and permutations.second is contains this batch.
    // else if _init_merged_segment == true, means this is not first batch, _merged_segment is not empty:
    //     permutations.first is the `SMALLER_THAN_MIN_OF_SEGMENT` part
    //     permutations.second is the `INCLUDE_IN_SEGMENT` part
    //
    std::pair<Permutation, Permutation> permutations;
    size_t distinct_top_n_of_first_size = 0;
    size_t distinct_top_n_of_second_size = 0;

    // Step 1: extract datas from _raw_chunks into segments,
    // and initialize permutations.second when _init_merged_segment == false.
    RETURN_IF_ERROR(_build_sorting_data(state, permutations.second, segments));

    // Step 2: filter batch-chunks as permutations.first and permutations.second when _init_merged_segment == true.
    // sort part chunks in permutations.first and permutations.second, if _init_merged_segment == false means permutations.first is empty.
    RETURN_IF_ERROR(_filter_and_sort_data_for_dense_rank(state, permutations, segments, distinct_top_n_of_first_size,
                                                         distinct_top_n_of_second_size));

    // Step 3: merge sort of two ordered groups
    // the first ordered group only contains permutations.first
    // the second ordered group contains both permutations.second and _merged_segment
    RETURN_IF_ERROR(_merge_sort_data_as_merged_segment_for_dense_rank(
            state, permutations, segments, distinct_top_n_of_first_size, distinct_top_n_of_second_size));
}

Status ChunksSorterDistinctTopn::_filter_and_sort_data_for_dense_rank(RuntimeState* state,
                                                                      std::pair<Permutation, Permutation>& permutations,
                                                                      DataSegments& segments,
                                                                      size_t& distinct_top_n_of_first_size,
                                                                      size_t& distinct_top_n_of_second_size) {
    const size_t rows_to_sort = _get_number_of_rows_to_sort();

    if (_init_merged_segment) {
        std::vector<std::vector<uint8_t>> filter_array;
        uint32_t smaller_num, include_num;
        if (_current_distinct_top_n >= rows_to_sort) {
            // in this case, we use the frist row of _merged_segment and the last row of _merged_segment to split segments into three parts:
            // 1.SMALLER_THAN_MIN_OF_SEGMENT(< min): which is smaller than the first row of _merged_segment
            // 2.INCLUDE_IN_SEGMENT( >= min && <= max): which is larger than or eqaul to the first row of _merged_segment but is smaller than or equal to the last row of _merged_segment
            // 3.LARGER_THAN_MAX_OF_SEGMENT( > max ): which is larger than the last row of _merged_segment, this part is useless
            SCOPED_TIMER(_sort_filter_timer);
            RETURN_IF_ERROR(_merged_segment.get_filter_array(segments, rows_to_sort, filter_array, _sort_desc,
                                                             smaller_num, include_num));
        } else {
            // in this case, we use the frist row of _merged_segment to split segments into two parts:
            // 1.SMALLER_THAN_MIN_OF_SEGMENT(< min): which is smaller than the first row of _merged_segment
            // 2.INCLUDE_IN_SEGMENT( >= min ): which is larger than or eqaul to the first row of _merged_segment
            // we don't have LARGER_THAN_MAX_OF_SEGMENT part, because we don't have enough distinct top n right now
            SCOPED_TIMER(_sort_filter_timer);
            RETURN_IF_ERROR(
                    _merged_segment.get_filter_array(segments, 1, filter_array, _sort_desc, smaller_num, include_num));
        }

        size_t filtered_rows = 0;
        for (auto& segment : segments) {
            filtered_rows += segment.chunk->num_rows();
        }

        {
            SCOPED_TIMER(_build_timer);
            // SMALLER_THAN_MIN_OF_SEGMENT
            permutations.first.resize(smaller_num);
            // INCLUDE_IN_SEGMENT
            permutations.second.resize(include_num);
            // Use filter_array to set permutations.first and permutations.second.
            _set_permutation_complete(permutations, segments.size(), filter_array);
        }

        // get top n of SMALLER_THAN_MIN_OF_SEGMENT
        size_t row_to_sort = _get_number_of_rows_to_sort();
        RETURN_IF_ERROR(
                _partial_sort_col_wise(state, permutations.first, segments, row_to_sort, distinct_top_n_of_first_size));
        // if we can get enough distinct top n from SMALLER_THAN_MIN_OF_SEGMENT, we already get what we want
        if (distinct_top_n_of_first_size >= rows_to_sort) {
            // clear permutations.second which is useless
            Permutation().swap(permutations.second);
            return Status::OK();
        } else {
            // otherwise we need to get more distinct top n from INCLUDE_IN_SEGMENT
            RETURN_IF_ERROR(_partial_sort_col_wise(state, permutations.second, segments,
                                                   distinct_top_n_of_first_size - rows_to_sort,
                                                   distinct_top_n_of_second_size));
            return Status::OK();
        }
    }
}

Status ChunksSorterDistinctTopn::_merge_sort_data_as_merged_segment_for_dense_rank(
        RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation, DataSegments& segments,
        size_t distinct_top_n_of_first_size, size_t distinct_top_n_of_second_size) {
    SCOPED_TIMER(_merge_timer);

    if (_init_merged_segment) {
        RETURN_IF_ERROR(_hybrid_sort_common_for_dense_rank(
                state, new_permutation, segments, distinct_top_n_of_first_size, distinct_top_n_of_second_size));
    } else {
        // The first batch chunks, just new_permutation.second.
        RETURN_IF_ERROR(_hybrid_sort_first_time(state, new_permutation.second, segments));
        _init_merged_segment = true;
    }

    // Include release memory's time in _merge_timer.
    Permutation().swap(new_permutation.first);
    Permutation().swap(new_permutation.second);

    DataSegments().swap(segments);
    return Status::OK();
}

Status ChunksSorterDistinctTopn::_hybrid_sort_common_for_dense_rank(
        RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation, DataSegments& segments,
        size_t first_distinct_num, size_t second_distinct_num) {
    const size_t rows_to_sort = _get_number_of_rows_to_sort();

    if (first_distinct_num == 0 && second_distinct_num == 0) {
        return Status::OK();
    }

    size_t distinct_rows_to_keep = rows_to_sort;
    ChunkPtr big_chunk;
    std::vector<ChunkPtr> chunks;
    for (auto& segment : segments) {
        chunks.push_back(segment.chunk);
    }

    // There are three parts of data
    // _merged_segment, the previously sorted one
    // the `SMALLER_THAN_MIN_OF_SEGMENT` part
    // the `INCLUDE_IN_SEGMENT` part
    size_t new_distinct_top_n = 0;

    // First, we find elements from `SMALLER_THAN_MIN_OF_SEGMENT`
    if (first_distinct_num > 0) {
        big_chunk.reset(
                segments[new_permutation.first[0].chunk_index].chunk->clone_empty(first_distinct_num).release());
        materialize_by_permutation(big_chunk.get(), chunks, new_permutation.first);
        distinct_rows_to_keep -= first_distinct_num;
        new_distinct_top_n += first_distinct_num;
    }

    // Seoncd, there are two cases:
    // case1: rows_to_keep == 0, which measn `SMALLER_THAN_MIN_OF_SEGMENT` part itself suffice, we can ignore both
    // `INCLUDE_IN_SEGMENT` part and _merged_segment
    // case2: rows_to_keep > 0, which means `SMALLER_THAN_MIN_OF_SEGMENT` part itself not suffice, we need to get more elements
    // from both `INCLUDE_IN_SEGMENT` part and _merged_segment. And notice that `INCLUDE_IN_SEGMENT` part may be empty
    if (distinct_rows_to_keep > 0) {
        if (big_chunk == nullptr) {
            big_chunk.reset(segments[new_permutation.second[0].chunk_index]
                                    .chunk->clone_empty(distinct_rows_to_keep)
                                    .release());
        }

        distinct_rows_to_keep = std::min(distinct_rows_to_keep, _current_distinct_top_n + second_distinct_num);

        // we need to merge INCLUDE_IN_SEGMENT and _merged_segment, get 'distinct_rows_to_keep' top elements from merged result and append them to big_chunk
        size_t merged_distinct_num;
        RETURN_IF_ERROR(_merge_sort_common(big_chunk, segments, distinct_rows_to_keep, new_permutation.second,
                                           second_distinct_num),
                        &merged_distinct_num);
        new_distinct_top_n += merged_distinct_num;
    }
    RETURN_IF_ERROR(big_chunk->upgrade_if_overflow());

    DataSegment merged_segment;
    merged_segment.init(_sort_exprs, big_chunk);
    _merged_segment = std::move(merged_segment);
    _current_distinct_top_n = new_distinct_top_n;

    return Status::OK();
}

Status ChunksSorterDistinctTopn::_partial_sort_col_wise(RuntimeState* state, Permutation& permutation,
                                                        DataSegments& segments, size_t limit, size_t& distinct_top_n) {
    SCOPED_TIMER(_sort_timer);

    std::vector<Columns> vertical_chunks;
    for (auto& segment : segments) {
        vertical_chunks.push_back(segment.order_by_columns);
    }

    // size_t first_size = std::min(permutation.size(), limit);

    if (limit > 0) {
        RETURN_IF_CANCELLED(state);
        return sort_vertical_chunks(state->cancelled_ref(), vertical_chunks, _sort_desc, permutation, limit, _topn_type,
                                    &distinct_top_n);
    }
}

} // namespace starrocks
