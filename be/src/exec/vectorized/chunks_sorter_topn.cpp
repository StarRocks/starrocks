// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "chunks_sorter_topn.h"

#include "column/type_traits.h"
#include "exec/vectorized/sorting/merge.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"
#include "exprs/expr.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "util/orlp/pdqsort.h"
#include "util/stopwatch.hpp"

namespace starrocks::vectorized {

ChunksSorterTopn::ChunksSorterTopn(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                                   const std::vector<bool>* is_asc_order, const std::vector<bool>* is_null_first,
                                   const std::string& sort_keys, size_t offset, size_t limit,
                                   const TTopNType::type topn_type, size_t max_buffered_chunks)
        : ChunksSorter(state, sort_exprs, is_asc_order, is_null_first, sort_keys, true),
          _max_buffered_chunks(max_buffered_chunks),
          _init_merged_segment(false),
          _limit(limit),
          _topn_type(topn_type),
          _offset(offset) {
    DCHECK_GT(_get_number_of_rows_to_sort(), 0) << "output rows can't be empty";
    DCHECK(_topn_type == TTopNType::ROW_NUMBER || _offset == 0);
    auto& raw_chunks = _raw_chunks.chunks;
    raw_chunks.reserve(max_buffered_chunks);
}

ChunksSorterTopn::~ChunksSorterTopn() = default;

// Cumulative chunks into _raw_chunks for sorting.
Status ChunksSorterTopn::update(RuntimeState* state, const ChunkPtr& chunk) {
    auto& raw_chunks = _raw_chunks.chunks;
    size_t chunk_number = raw_chunks.size();
    if (chunk_number <= 0) {
        raw_chunks.push_back(chunk);
        chunk_number++;
    } else if (raw_chunks[chunk_number - 1]->num_rows() + chunk->num_rows() > _state->chunk_size()) {
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

    if (_limit > 0 && (chunk_number >= _limit || chunk_number >= _max_buffered_chunks)) {
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

Status ChunksSorterTopn::get_next(ChunkPtr* chunk, bool* eos) {
    ScopedTimer<MonotonicStopWatch> timer(_output_timer);
    if (_next_output_row >= _merged_segment.chunk->num_rows()) {
        *chunk = nullptr;
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    size_t count = std::min(size_t(_state->chunk_size()), _merged_segment.chunk->num_rows() - _next_output_row);
    chunk->reset(_merged_segment.chunk->clone_empty(count).release());
    (*chunk)->append_safe(*_merged_segment.chunk, _next_output_row, count);
    (*chunk)->downgrade();
    _next_output_row += count;
    return Status::OK();
}

SortedRuns ChunksSorterTopn::get_sorted_runs() {
    return {SortedRun(_merged_segment.chunk, _merged_segment.order_by_columns)};
}

size_t ChunksSorterTopn::get_output_rows() const {
    return _merged_segment.chunk->num_rows();
}

Status ChunksSorterTopn::_sort_chunks(RuntimeState* state) {
    const size_t chunk_size = _raw_chunks.size_of_rows;

    // chunks for this batch.
    DataSegments segments;

    // permutations for this batch:
    // if _init_merged_segment == false, means this is first batch:
    //     permutations.first is empty, and permutations.second is contains this batch.
    // else if _init_merged_segment == true, means this is not first batch, _merged_segment[low_value, high_value] is not empty:
    //     permutations.first < low_value and low_value <= permutations.second < high_value(in the case of asc).
    //
    std::pair<Permutation, Permutation> permutations;

    // step 1: extract datas from _raw_chunks into segments,
    // and initialize permutations.second when _init_merged_segment == false.
    RETURN_IF_ERROR(_build_sorting_data(state, permutations.second, segments));

    // step 2: filter batch-chunks as permutations.first and permutations.second when _init_merged_segment == true.
    // sort part chunks in permutations.first and permutations.second, if _init_merged_segment == false means permutations.first is empty.
    RETURN_IF_ERROR(_filter_and_sort_data(state, permutations, segments, chunk_size));

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
                permutation_second[perm_index] = {i, j};
                ++perm_index;
            }
        }
    }

    return Status::OK();
}

void ChunksSorterTopn::_set_permutation_before(Permutation& permutation, size_t size,
                                               std::vector<std::vector<uint8_t>>& filter_array) {
    for (uint32_t i = 0; i < size; ++i) {
        size_t nums = filter_array[i].size();
        for (uint32_t j = 0; j < nums; ++j) {
            if (filter_array[i][j] == DataSegment::BEFORE_LAST_RESULT) {
                PermutationItem item{i, j};
                permutation.emplace_back(item);
            }
        }
    }
}

void ChunksSorterTopn::_set_permutation_complete(std::pair<Permutation, Permutation>& permutations, size_t size,
                                                 std::vector<std::vector<uint8_t>>& filter_array) {
    size_t first_size = permutations.first.capacity();
    size_t second_size = permutations.second.capacity();

    for (uint32_t i = 0; i < size; ++i) {
        size_t nums = filter_array[i].size();
        for (uint32_t j = 0; j < nums; ++j) {
            if (filter_array[i][j] == DataSegment::BEFORE_LAST_RESULT) {
                PermutationItem tmp{i, j};
                permutations.first.emplace_back(tmp);
            } else if (filter_array[i][j] == DataSegment::IN_LAST_RESULT) {
                PermutationItem tmp{i, j};
                permutations.second.emplace_back(tmp);
            }
        }
    }

    if (permutations.second.size() != second_size) {
        std::cout << "INVALID_SIZE:" << first_size << "," << permutations.first.size() << "," << second_size << ","
                  << permutations.second.size() << std::endl;
    }
}

// In general, we take the first and last row from _merged_segment:
// step 1: use last row to filter chunk_size rows in segments as two parts(rows < lastRow and rows >= lastRow),
// step 2: use first row to filter all rows < lastRow, result in two parts, the BEFORE is (rows < firstRow), the IN is (rows >= firstRow and rwos < lastRows),
// step 3: set this result in filter_array, BEOFRE(filter_array's value is 2), IN(filter_array's value is 1), others is give up.
// all this is done in get_filter_array.
//
// and maybe _merged_segment'size is not enough as < rows_to_sort,
// this case we just use first row to filter all rows to get BEFORE.
// this is done in get_filter_array
//
// then will obtain BEFORE and IN as permutations.first and permutations.second use filter_array.
// at last we sort parts datas in permutations.first and permutations.second.
Status ChunksSorterTopn::_filter_and_sort_data(RuntimeState* state, std::pair<Permutation, Permutation>& permutations,
                                               DataSegments& segments, const size_t chunk_size) {
    ScopedTimer<MonotonicStopWatch> timer(_sort_timer);

    DCHECK(_get_number_of_order_by_columns() > 0) << "order by columns can't be empty";

    const size_t rows_to_sort = _get_number_of_rows_to_sort();

    if (_init_merged_segment) {
        std::vector<std::vector<uint8_t>> filter_array;
        uint32_t least_num, middle_num;

        // Here are 2 cases:
        // case 1: _merged_segment.chunk->num_rows() >= rows_to_sort, which means we already have enough rows,
        // so we can use both index of `0` and `rows_to_sort - 1` as the left and right boundary to filter the coming input chunks
        // into three parts, BEFORE_LAST_RESULT, IN_LAST_RESULT and the part to be dropped
        // case 2: _merged_segment.chunk->num_rows() < rows_to_sort, which means we haven't have enough rows,
        // so we can only use the index of `0` as the left boundary to filter the coming input chunks into two parts, BEFORE_LAST_RESULT and IN_LAST_RESULT

        if (_merged_segment.chunk->num_rows() >= rows_to_sort) {
            RETURN_IF_ERROR(_merged_segment.get_filter_array(segments, rows_to_sort, filter_array, _sort_order_flag,
                                                             _null_first_flag, least_num, middle_num));
        } else {
            RETURN_IF_ERROR(_merged_segment.get_filter_array(segments, 1, filter_array, _sort_order_flag,
                                                             _null_first_flag, least_num, middle_num));
        }

        timer.stop();
        {
            ScopedTimer<MonotonicStopWatch> timer(_build_timer);
            permutations.first.reserve(least_num);
            // BEFORE's size is enough, so we ignore IN.
            if (least_num >= rows_to_sort) {
                // use filter_array to set permutations.first.
                _set_permutation_before(permutations.first, segments.size(), filter_array);
            } else if (rows_to_sort > 1 || _topn_type == TTopNType::RANK) {
                // If rows_to_sort == 1, here are two cases:
                // case 1: _topn_type is TTopNType::ROW_NUMBER, first row and last row is the same identity. so we do nothing.
                // case 2: _topn_type is TTopNType::RANK, we need to contain all the rows equal with the last row of merged segment,
                // and these equals row maybe exist in the second part

                // BEFORE's size < rows_to_sort, we need set permutations.first and permutations.second.
                permutations.second.reserve(middle_num);

                // use filter_array to set permutations.first and permutations.second.
                _set_permutation_complete(permutations, segments.size(), filter_array);
            }
        }
        timer.start();
    }

    return _partial_sort_col_wise(state, permutations, segments, chunk_size, rows_to_sort);
}

Status ChunksSorterTopn::_partial_sort_col_wise(RuntimeState* state, std::pair<Permutation, Permutation>& permutations,
                                                DataSegments& segments, const size_t chunk_size,
                                                const size_t rows_to_sort) {
    std::vector<Columns> vertical_chunks;
    for (auto& segment : segments) {
        vertical_chunks.push_back(segment.order_by_columns);
    }
    auto do_sort = [&](Permutation& perm, size_t limit) {
        return sort_vertical_chunks(state->cancelled_ref(), vertical_chunks, _sort_order_flag, _null_first_flag, perm,
                                    limit, _topn_type == TTopNType::RANK);
    };

    size_t first_size = std::min(permutations.first.size(), rows_to_sort);

    // Sort the first, then the second
    if (first_size > 0) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(do_sort(permutations.first, first_size));
    }

    if (rows_to_sort > first_size) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(do_sort(permutations.second, rows_to_sort - first_size));
    } else if (_topn_type == TTopNType::RANK) {
        // if _topn_type is TTopNType::RANK, we need to contain all the rows equal with the last row of merged segment,
        // and these equals row maybe exist in the second part, we can fetch these part by set rank limit number to 1
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(do_sort(permutations.second, 1));
    }

    return Status::OK();
}

Status ChunksSorterTopn::_merge_sort_data_as_merged_segment(RuntimeState* state,
                                                            std::pair<Permutation, Permutation>& new_permutation,
                                                            DataSegments& segments) {
    ScopedTimer<MonotonicStopWatch> timer(_merge_timer);

    if (_init_merged_segment) {
        RETURN_IF_ERROR(_hybrid_sort_common(state, new_permutation, segments));
    } else {
        // the first batch chunks, just new_permutation.second.
        RETURN_IF_ERROR(_hybrid_sort_first_time(state, new_permutation.second, segments));
        _init_merged_segment = true;
    }

    // include release memory'time in _merge_timer.
    Permutation().swap(new_permutation.first);
    Permutation().swap(new_permutation.second);

    DataSegments().swap(segments);
    return Status::OK();
}

// take rows_to_sort rows from permutation_second merge-sort with _merged_segment.
// And take result datas into big_chunk.
Status ChunksSorterTopn::_merge_sort_common(ChunkPtr& big_chunk, DataSegments& segments, const size_t rows_to_keep,
                                            size_t sorted_size, size_t permutation_size,
                                            Permutation& permutation_second) {
    // Assemble the permutated segments into a chunk
    std::vector<ChunkPtr> right_chunks;
    for (auto& segment : segments) {
        right_chunks.push_back(segment.chunk);
    }
    ChunkPtr right_chunk = big_chunk->clone_empty(permutation_second.size());
    append_by_permutation(right_chunk.get(), right_chunks, permutation_second);
    Columns right_columns;
    for (auto expr : *_sort_exprs) {
        auto maybe_column = expr->evaluate(right_chunk.get());
        RETURN_IF_ERROR(maybe_column);
        right_columns.push_back(maybe_column.value());
    }

    ChunkPtr left_chunk = _merged_segment.chunk;
    Columns left_columns = _merged_segment.order_by_columns;

    Permutation merged_perm;
    merged_perm.reserve(rows_to_keep);
    SortDescs sort_desc(_sort_order_flag, _null_first_flag);

    RETURN_IF_ERROR(merge_sorted_chunks_two_way(sort_desc, {left_chunk, left_columns}, {right_chunk, right_columns},
                                                &merged_perm));
    CHECK_GE(merged_perm.size(), rows_to_keep);
    merged_perm.resize(rows_to_keep);

    std::vector<ChunkPtr> chunks{left_chunk, right_chunk};
    append_by_permutation(big_chunk.get(), chunks, merged_perm);
    return Status::OK();
}

Status ChunksSorterTopn::_hybrid_sort_common(RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation,
                                             DataSegments& segments) {
    const size_t rows_to_sort = _get_number_of_rows_to_sort();

    size_t first_size = new_permutation.first.size();
    size_t second_size = new_permutation.second.size();

    if (first_size == 0 && second_size == 0) {
        return Status::OK();
    }

    size_t rows_to_keep = rows_to_sort;
    if (_topn_type == TTopNType::RANK && first_size > rows_to_keep) {
        // For rank type, the number of rows may be greater than the specified limit rank number
        // For example, given set [1, 1, 1, 1, 1] and limit rank number=2, all the element's rank is 1,
        // so the size of BEFORE_LAST_RESULT part may greater than the specified limit rank number(2)
        rows_to_keep = first_size;
    }

    ChunkPtr big_chunk;
    std::vector<ChunkPtr> chunks;
    for (auto& segment : segments) {
        chunks.push_back(segment.chunk);
    }

    if (first_size > 0) {
        big_chunk.reset(segments[new_permutation.first[0].chunk_index].chunk->clone_empty(first_size).release());
        append_by_permutation(big_chunk.get(), chunks, new_permutation.first);
        rows_to_keep -= first_size;
    }

    if (rows_to_keep > 0 || _topn_type == TTopNType::RANK) {
        // In case of rows_to_keep == 0, through _merged_segment.get_filter_array,
        // all the rows that equal to the last value of merged segment are stored in the IN_LAST_RESULT,
        // so we also need to consider this part in type rank
        if (big_chunk == nullptr) {
            big_chunk.reset(segments[new_permutation.second[0].chunk_index].chunk->clone_empty(rows_to_keep).release());
        }
        size_t sorted_size = _merged_segment.chunk->num_rows();
        rows_to_keep = std::min(rows_to_keep, sorted_size + second_size);
        if (_topn_type == TTopNType::RANK && sorted_size + second_size > rows_to_keep) {
            rows_to_keep = sorted_size + second_size;
        }

        RETURN_IF_ERROR(_merge_sort_common(big_chunk, segments, rows_to_keep, sorted_size, second_size,
                                           new_permutation.second));
    }
    RETURN_IF_ERROR(big_chunk->upgrade_if_overflow());

    DataSegment merged_segment;
    merged_segment.init(_sort_exprs, big_chunk);
    _merged_segment = std::move(merged_segment);

    return Status::OK();
}

Status ChunksSorterTopn::_hybrid_sort_first_time(RuntimeState* state, Permutation& new_permutation,
                                                 DataSegments& segments) {
    const size_t rows_to_sort = _get_number_of_rows_to_sort();

    size_t rows_to_keep = rows_to_sort;

    rows_to_keep = std::min(new_permutation.size(), rows_to_keep);
    if (_topn_type == TTopNType::RANK && new_permutation.size() > rows_to_keep) {
        // For rank type, the number of rows may be greater than the specified limit rank number
        // For example, given set [1, 1, 1, 1, 1], all the element's rank is 1, so we must keep
        // all the element if rank limit number is 1
        rows_to_keep = new_permutation.size();
    }

    if (rows_to_keep > Column::MAX_CAPACITY_LIMIT) {
        LOG(WARNING) << "topn sort row exceed rows limit " << rows_to_keep;
        return Status::InternalError(fmt::format("TopN sort exceed rows limit {}", rows_to_keep));
    }

    ChunkPtr big_chunk;
    big_chunk.reset(segments[new_permutation[0].chunk_index].chunk->clone_empty(rows_to_keep).release());

    // Initial this big chunk.
    std::vector<ChunkPtr> chunks;
    for (auto& segment : segments) {
        chunks.push_back(segment.chunk);
    }
    new_permutation.resize(rows_to_keep);
    append_by_permutation(big_chunk.get(), chunks, new_permutation);

    RETURN_IF_ERROR(big_chunk->upgrade_if_overflow());
    _merged_segment.init(_sort_exprs, big_chunk);

    return Status::OK();
}

} // namespace starrocks::vectorized
