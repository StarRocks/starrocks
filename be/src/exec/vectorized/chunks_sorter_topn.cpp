// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "chunks_sorter_topn.h"

#include "column/type_traits.h"
#include "exprs/expr.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "util/orlp/pdqsort.h"
#include "util/stopwatch.hpp"

namespace starrocks::vectorized {

ChunksSorterTopn::ChunksSorterTopn(const std::vector<ExprContext*>* sort_exprs, const std::vector<bool>* is_asc,
                                   const std::vector<bool>* is_null_first, size_t offset, size_t limit,
                                   size_t size_of_chunk_batch)
        : ChunksSorter(sort_exprs, is_asc, is_null_first, size_of_chunk_batch),
          _offset(offset),
          _limit(limit),
          _init_merged_segment(false) {
    auto& raw_chunks = _raw_chunks.chunks;
    raw_chunks.reserve(size_of_chunk_batch);
}

ChunksSorterTopn::~ChunksSorterTopn() = default;

// Cumulative chunks into _raw_chunks for sorting.
Status ChunksSorterTopn::update(RuntimeState* state, const ChunkPtr& chunk) {
    auto& raw_chunks = _raw_chunks.chunks;
    size_t chunk_number = raw_chunks.size();
    if (chunk_number <= 0) {
        raw_chunks.push_back(chunk);
        chunk_number++;
    } else if (raw_chunks[chunk_number - 1]->num_rows() + chunk->num_rows() > config::vector_chunk_size) {
        raw_chunks.push_back(chunk);
        chunk_number++;
    } else {
        // Old planner will not remove duplicated sort column.
        // columns in chunk may have same column ptr
        // append_safe will check size of all columns in dest chunk
        // to ensure same column will not apppend repeatedly.
        raw_chunks[chunk_number - 1]->append_safe(*chunk);
    }
    _raw_chunks.size_of_rows += chunk->num_rows();

    // when number of Chunks exceeds _limit or _size_of_chunk_batch, run sort and then part of
    // cached chunks can be dropped, so it can reduce the memory usage.
    // TopN caches _limit or _size_of_chunk_batch primitive chunks,
    // performs sorting once, and discards extra rows

    if (_limit > 0 && (chunk_number >= _limit || chunk_number >= _size_of_chunk_batch)) {
        RETURN_IF_ERROR(_sort_chunks(state));
    }

    return Status::OK();
}

Status ChunksSorterTopn::done(RuntimeState* state) {
    auto& raw_chunks = _raw_chunks.chunks;
    if (!raw_chunks.empty()) {
        RETURN_IF_ERROR(_sort_chunks(state));
    }

    // skip top OFFSET rows
    if (_offset > 0) {
        if (_offset > _merged_segment.chunk->num_rows()) {
            _merged_segment.clear();
            _next_output_row = 0;
        } else {
            _next_output_row += _offset;
        }
    } else {
        _next_output_row = 0;
    }

    return Status::OK();
}

void ChunksSorterTopn::get_next(ChunkPtr* chunk, bool* eos) {
    ScopedTimer<MonotonicStopWatch> timer(_output_timer);
    if (_next_output_row >= _merged_segment.chunk->num_rows()) {
        *chunk = nullptr;
        *eos = true;
        return;
    }
    *eos = false;
    size_t count = std::min(size_t(config::vector_chunk_size), _merged_segment.chunk->num_rows() - _next_output_row);
    chunk->reset(_merged_segment.chunk->clone_empty(count).release());
    (*chunk)->append_safe(*_merged_segment.chunk, _next_output_row, count);
    _next_output_row += count;
}

/*
 * _next_output_row index the next row we need to get, 
 * In this case, The actual data is _merged_segment.chunk, 
 * so we use _next_output_row to get datas from _merged_segment.chunk, 
 * and copy it in chunk as output.
 */
bool ChunksSorterTopn::pull_chunk(ChunkPtr* chunk) {
    if (_next_output_row >= _merged_segment.chunk->num_rows()) {
        *chunk = nullptr;
        return true;
    }
    size_t count = std::min(size_t(config::vector_chunk_size), _merged_segment.chunk->num_rows() - _next_output_row);
    chunk->reset(_merged_segment.chunk->clone_empty(count).release());
    (*chunk)->append_safe(*_merged_segment.chunk, _next_output_row, count);
    _next_output_row += count;

    if (_next_output_row >= _merged_segment.chunk->num_rows()) {
        return true;
    }
    return false;
}

Status ChunksSorterTopn::_sort_chunks(RuntimeState* state) {
    const size_t batch_size = _raw_chunks.size_of_rows;

    // chunks for this batch.
    DataSegments segments;

    // permutations for this batch:
    // if _init_merged_segment == false, means this is first batch:
    //     permutations.first is empty, and permutations.second is contains this batch.
    // else if _init_merged_segment == false, means this is not first batch, _merged_segment[low_value, high_value] is not empty:
    //     permutations.first < low_value and low_value <= permutations.second < high_value(in the case of asc).
    //
    std::pair<Permutation, Permutation> permutations;

    // step 1: extract datas from _raw_chunks into segments,
    // and initialize permutations.second when _init_merged_segment == false.
    RETURN_IF_ERROR(_build_sorting_data(state, permutations.second, segments));

    // step 2: filter batch-chunks as permutations.first and permutations.second when _init_merged_segment == true.
    // sort part chunks in permutations.first and permutations.second, if _init_merged_segment == false means permutations.first is empty.
    RETURN_IF_ERROR(_filter_and_sort_data_by_row_cmp(state, permutations, segments, batch_size));

    // step 3:
    // (1) take permutations.first as BEFORE.
    // (2) take permutations.second merge-sort with _merged_segment as IN.
    // the result is _merged_segment as [BEFORE, IN].
    RETURN_IF_ERROR(_merge_sort_data_as_merged_segment(state, permutations, segments));

    return Status::OK();
}

Status ChunksSorterTopn::_build_sorting_data(RuntimeState* state, Permutation& permutation_second,
                                             DataSegments& segments) {
    ScopedTimer<MonotonicStopWatch> timer(_build_timer);

    size_t row_count = _raw_chunks.size_of_rows;
    auto& raw_chunks = _raw_chunks.chunks;

    // Build one chunk as one DataSegment
    // The timer says that: in top-n case, _build_sorting_data may be called multiple times.
    // If merging small chunks into one chunk every time this method called, the accumulated
    // time consumption would significantly more than the pdqsort or partial_sort routine.
    // So here just put one chunk into one DataSegment for a less total time consumption.
    size_t raw_chunks_size = raw_chunks.size();
    segments.reserve(raw_chunks_size);
    for (const auto& cnk : raw_chunks) {
        // merge segments into segments for the convenience of merging sorted result.
        segments.emplace_back(_sort_exprs, cnk);
    }
    _raw_chunks.clear();

    // this time, because we will filter chunks before initialize permutations, so we just check memory usage.
    if (!_init_merged_segment) {
        // this time, Just initialized permutations.second.
        permutation_second.resize(row_count);

        uint32_t perm_index = 0;
        for (uint32_t i = 0; i < segments.size(); ++i) {
            uint32_t num = segments[i].chunk->num_rows();
            for (uint32_t j = 0; j < num; ++j) {
                permutation_second[perm_index] = {i, j, perm_index};
                ++perm_index;
            }
        }
    }

    return Status::OK();
}

void ChunksSorterTopn::_sort_data_by_row_cmp(
        Permutation& permutation, size_t rows_to_sort, size_t rows_size,
        const std::function<bool(const PermutationItem& l, const PermutationItem& r)>& cmp_fn) {
    if (rows_to_sort > 0 && rows_to_sort < rows_size / 5) {
        // when Limit >= 1/5 of all data, a full sort will be faster than partial sort.
        // partial sort
        std::partial_sort(permutation.begin(), permutation.begin() + rows_to_sort, permutation.end(), cmp_fn);
        // for topn, We don't need the data after [0, number_of_rows_to_sort).
        permutation.resize(rows_to_sort);
    } else {
        // full sort
        pdqsort(permutation.begin(), permutation.end(), cmp_fn);
        if (rows_size > rows_to_sort) {
            // for topn, We don't need the data after [0, number_of_rows_to_sort).
            permutation.resize(rows_to_sort);
        }
    }
}

void ChunksSorterTopn::_set_permutation_before(Permutation& permutation, size_t size,
                                               std::vector<std::vector<uint8_t>>& filter_array) {
    uint32_t first_size = 0;
    for (uint32_t i = 0; i < size; ++i) {
        size_t nums = filter_array[i].size();
        for (uint32_t j = 0; j < nums; ++j) {
            if (filter_array[i][j] == DataSegment::BEFORE_LAST_RESULT) {
                permutation[first_size] = {i, j, first_size};
                ++first_size;
            }
        }
    }
}

void ChunksSorterTopn::_set_permutation_complete(std::pair<Permutation, Permutation>& permutations, size_t size,
                                                 std::vector<std::vector<uint8_t>>& filter_array) {
    uint32_t first_size, second_size;
    first_size = second_size = 0;

    for (uint32_t i = 0; i < size; ++i) {
        size_t nums = filter_array[i].size();
        for (uint32_t j = 0; j < nums; ++j) {
            if (filter_array[i][j] == DataSegment::BEFORE_LAST_RESULT) {
                permutations.first[first_size] = {i, j, first_size};
                ++first_size;
            } else if (filter_array[i][j] == DataSegment::IN_LAST_RESULT) {
                permutations.second[second_size] = {i, j, second_size};
                ++second_size;
            }
        }
    }
}

// In general, we take the first and last row from _merged_segment:
// step 1: use last row to filter batch_size rows in segments as two parts(rows < lastRow and rows >= lastRow),
// step 2: use first row to filter all rows < lastRow, result in two parts, the BEFORE is (rows < firstRow), the IN is (rows >= firstRow and rwos < lastRows),
// step 3: set this result in filter_array, BEOFRE(filter_array's value is 2), IN(filter_array's value is 1), others is give up.
// all this is done in get_filter_array.
//
// and maybe _merged_segment'size is not enough as < number_of_rows_to_sort,
// this case we just use first row to filter all rows to get BEFORE.
// this is done in get_filter_array
//
// then will obtain BEFORE and IN as permutations.first and permutations.second use filter_array.
// at last we sort parts datas in permutations.first and permutations.second.
Status ChunksSorterTopn::_filter_and_sort_data_by_row_cmp(RuntimeState* state,
                                                          std::pair<Permutation, Permutation>& permutations,
                                                          DataSegments& segments, const size_t batch_size) {
    ScopedTimer<MonotonicStopWatch> timer(_sort_timer);

    DCHECK(_get_number_of_order_by_columns() > 0) << "order by columns can't be empty";

    const int64_t number_of_rows_to_sort = _get_number_of_rows_to_sort();
    DCHECK(number_of_rows_to_sort > 0) << "output rows can't be empty";

    if (_init_merged_segment) {
        std::vector<std::vector<uint8_t>> filter_array;
        uint32_t least_num, middle_num;

        // bytes_for_filter is use to check memory usage,
        // because memory increase in get_filter_array and get_filter_array
        // and that maybe return ERROR Status, so we use bytes_for_filter for memory that release after filter.
        // bytes_for_filter's use have 2 cases:
        // in get_filter_array:
        // part 1: std::vector<std::vector<uint64_t>> rows_to_compare_array;
        // part 2: std::vector<std::vector<uint8_t>>& filter_array, std::vector<size_t> first_size_array and std::vector<std::vector<uint64_t>> rows_to_compare_array(second compare).
        //
        // part 1 and part 2 is not coexistence, and we use bytes_for_filter to get the larger of them,
        // subtract filter_array's memory in bytes_for_filter at end. because filter_array is used remainly.

        if (number_of_rows_to_sort > 1 && _merged_segment.chunk->num_rows() >= number_of_rows_to_sort) {
            RETURN_IF_ERROR(_merged_segment.get_filter_array(segments, number_of_rows_to_sort, filter_array,
                                                             _sort_order_flag, _null_first_flag, least_num,
                                                             middle_num));
        } else {
            RETURN_IF_ERROR(_merged_segment.get_filter_array(segments, 1, filter_array, _sort_order_flag,
                                                             _null_first_flag, least_num, middle_num));
        }

        timer.stop();
        {
            ScopedTimer<MonotonicStopWatch> timer(_build_timer);
            permutations.first.resize(least_num);
            // BEFORE's size is enough, so we ignore IN.
            if (least_num >= number_of_rows_to_sort) {
                // use filter_array to set permutations.first.
                _set_permutation_before(permutations.first, segments.size(), filter_array);
            } else if (number_of_rows_to_sort > 1) {
                // if number_of_rows_to_sort == 1, first row and last row is the same identity. so we do nothing.
                // BEFORE's size < number_of_rows_to_sort, we need set permutations.first and permutations.second.
                permutations.second.resize(middle_num);

                // use filter_array to set permutations.first and permutations.second.
                _set_permutation_complete(permutations, segments.size(), filter_array);
            }
        }
        timer.start();
    }

    const DataSegments& data = segments;
    const std::vector<int>& sort_order_flag = _sort_order_flag;
    const std::vector<int>& null_first_flag = _null_first_flag;

    auto cmp_fn = [&data, &sort_order_flag, &null_first_flag](const PermutationItem& l, const PermutationItem& r) {
        int c = data[l.chunk_index].compare_at(l.index_in_chunk, data[r.chunk_index], r.index_in_chunk, sort_order_flag,
                                               null_first_flag);
        if (c == 0) {
            return l.permutation_index < r.permutation_index;
        } else {
            return c < 0;
        }
    };

    // take topn in [permutations.first, permutations.second].
    // if permutations.first.size() >= number_of_rows_to_sort, we just need number_of_rows_to_sort rows in permutations.first.
    // else we need all permutations.first as BEFORE and take (number_of_rows_to_sort - permutations.first.size()) rows from
    // permutations.second.
    size_t first_size = permutations.first.size();
    if (first_size >= number_of_rows_to_sort) {
        _sort_data_by_row_cmp(permutations.first, number_of_rows_to_sort, first_size, cmp_fn);
    } else {
        if (first_size > 0) {
            pdqsort(permutations.first.begin(), permutations.first.end(), cmp_fn);
        }

        _sort_data_by_row_cmp(permutations.second, number_of_rows_to_sort - first_size, permutations.second.size(),
                              cmp_fn);
    }

    return Status::OK();
}

Status ChunksSorterTopn::_merge_sort_data_as_merged_segment(RuntimeState* state,
                                                            std::pair<Permutation, Permutation>& new_permutation,
                                                            DataSegments& segments) {
    ScopedTimer<MonotonicStopWatch> timer(_merge_timer);

    size_t sort_row_number = _get_number_of_rows_to_sort();
    DCHECK(sort_row_number > 0) << "output rows can't be empty";

    if (_init_merged_segment) {
        RETURN_IF_ERROR(_hybrid_sort_common(state, new_permutation, segments, sort_row_number));
    } else {
        // the first batch chunks, just new_permutation.second.
        RETURN_IF_ERROR(_hybrid_sort_first_time(state, new_permutation.second, segments, sort_row_number));
        _init_merged_segment = true;
    }

    // include release memory'time in _merge_timer.
    Permutation().swap(new_permutation.first);
    Permutation().swap(new_permutation.second);

    DataSegments().swap(segments);
    return Status::OK();
}

// take sort_row_number rows from permutation_second merge-sort with _merged_segment.
// And take result datas into big_chunk.
void ChunksSorterTopn::_merge_sort_common(ChunkPtr& big_chunk, DataSegments& segments, size_t sort_row_number,
                                          size_t sorted_size, size_t permutation_size,
                                          Permutation& permutation_second) {
    size_t index_of_merging = 0, index_of_left = 0, index_of_right = 0;
    while ((index_of_merging < sort_row_number) && (index_of_left < sorted_size) &&
           (index_of_right < permutation_size)) {
        const auto& right = permutation_second[index_of_right];
        int cmp = _merged_segment.compare_at(index_of_left, segments[right.chunk_index], right.index_in_chunk,
                                             _sort_order_flag, _null_first_flag);

        if (cmp <= 0) {
            // pickup from left
            big_chunk->append_safe(*_merged_segment.chunk, index_of_left, 1);
            ++index_of_left;
        } else {
            // pickup from right
            big_chunk->append_safe(*segments[right.chunk_index].chunk, right.index_in_chunk, 1);
            ++index_of_right;
        }
        ++index_of_merging;
    }

    if (index_of_left < sorted_size && index_of_merging < sort_row_number) {
        size_t min = std::min(sorted_size - index_of_left, sort_row_number - index_of_merging);
        size_t index_of_left_end = index_of_left + min;
        for (; index_of_left < index_of_left_end; ++index_of_left) {
            big_chunk->append_safe(*_merged_segment.chunk, index_of_left, 1);
        }
    }

    if (index_of_right < permutation_size && index_of_merging < sort_row_number) {
        size_t min = std::min(permutation_size - index_of_right, sort_row_number - index_of_merging);
        size_t index_of_right_end = index_of_right + min;
        for (; index_of_right < index_of_right_end; ++index_of_right) {
            const auto& right = permutation_second[index_of_right];
            big_chunk->append_safe(*segments[right.chunk_index].chunk, right.index_in_chunk, 1);
        }
    }
}

Status ChunksSorterTopn::_hybrid_sort_common(RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation,
                                             DataSegments& segments, size_t sort_row_number) {
    ChunkPtr big_chunk;
    size_t first_size = new_permutation.first.size();

    // this means we just need number_of_rows_to_sort rows in permutations.first.
    if (first_size >= sort_row_number) {
        big_chunk.reset(segments[new_permutation.first[0].chunk_index].chunk->clone_empty(sort_row_number).release());

        for (size_t index = 0; index < sort_row_number; ++index) {
            auto& right = new_permutation.first[index];
            big_chunk->append_safe(*segments[right.chunk_index].chunk, right.index_in_chunk, 1);
        }

        DataSegment merged_segment;
        merged_segment.init(_sort_exprs, big_chunk);

        _merged_segment = std::move(merged_segment);
    } else {
        size_t permutation_size = new_permutation.second.size();
        if (first_size == 0 && permutation_size == 0) {
            return Status::OK();
        }

        // this means we need all permutations.first as BEFORE and take (number_of_rows_to_sort - permutations.first.size()) rows from
        // permutations.second merge-sort with _merged_segment.
        sort_row_number = sort_row_number - first_size;
        size_t sorted_size = _merged_segment.chunk->num_rows();
        if (sort_row_number > sorted_size + permutation_size) {
            sort_row_number = sorted_size + permutation_size;
        }

        if (first_size > 0) {
            big_chunk.reset(segments[new_permutation.first[0].chunk_index]
                                    .chunk->clone_empty(sort_row_number + first_size)
                                    .release());

            for (size_t index = 0; index < first_size; ++index) {
                auto& right = new_permutation.first[index];
                big_chunk->append_safe(*segments[right.chunk_index].chunk, right.index_in_chunk, 1);
            }
        } else {
            big_chunk.reset(
                    segments[new_permutation.second[0].chunk_index].chunk->clone_empty(sort_row_number).release());
        }

        // take sort_row_number rows from permutations.second merge-sort with _merged_segment.
        _merge_sort_common(big_chunk, segments, sort_row_number, sorted_size, permutation_size, new_permutation.second);

        DataSegment merged_segment;
        merged_segment.init(_sort_exprs, big_chunk);

        _merged_segment = std::move(merged_segment);
    }

    return Status::OK();
}

Status ChunksSorterTopn::_hybrid_sort_first_time(RuntimeState* state, Permutation& new_permutation,
                                                 DataSegments& segments, size_t sort_row_number) {
    size_t permutation_size = new_permutation.size();
    if (sort_row_number > permutation_size) {
        sort_row_number = permutation_size;
    }

    if (sort_row_number > std::numeric_limits<uint32_t>::max()) {
        LOG(WARNING) << "full sort row is " << sort_row_number;
        return Status::InternalError("Full sort in single query instance only support at most 4294967295 rows");
    }

    ChunkPtr big_chunk;
    size_t index_of_merging = 0;

    // Initial this big chunk.
    big_chunk.reset(segments[new_permutation[0].chunk_index].chunk->clone_empty(sort_row_number).release());
    while (index_of_merging < sort_row_number) {
        auto& permutation = new_permutation[index_of_merging];
        big_chunk->append_safe(*segments[permutation.chunk_index].chunk, permutation.index_in_chunk, 1);
        ++index_of_merging;
    }

    _merged_segment.init(_sort_exprs, big_chunk);

    return Status::OK();
}

} // namespace starrocks::vectorized
