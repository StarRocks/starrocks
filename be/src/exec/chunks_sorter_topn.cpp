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

#include "chunks_sorter_topn.h"

#include "column/column_helper.h"
#include "column/datum.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "types/logical_type_infra.h"
#include "util/orlp/pdqsort.h"
#include "util/stopwatch.hpp"

namespace starrocks {

void get_compare_results_colwise(size_t rows_to_sort, const Columns& order_by_columns,
                                 std::vector<CompareVector>& compare_results_array,
                                 const std::vector<DataSegment>& data_segments, const SortDescs& sort_desc) {
    size_t dats_segment_size = data_segments.size();

    for (size_t i = 0; i < dats_segment_size; ++i) {
        size_t rows = data_segments[i].chunk->num_rows();
        compare_results_array[i].resize(rows, 0);
    }

    size_t order_by_column_size = order_by_columns.size();

    for (size_t i = 0; i < dats_segment_size; i++) {
        Buffer<Datum> rhs_values;
        auto& segment = data_segments[i];
        for (size_t col_idx = 0; col_idx < order_by_column_size; col_idx++) {
            rhs_values.push_back(order_by_columns[col_idx]->get(rows_to_sort));
        }
        compare_columns(segment.order_by_columns, compare_results_array[i], rhs_values, sort_desc);
    }
}

ChunksSorterTopn::ChunksSorterTopn(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                                   const std::vector<bool>* is_asc_order, const std::vector<bool>* is_null_first,
                                   const std::string& sort_keys, size_t offset, size_t limit,
                                   const TTopNType::type topn_type, size_t max_buffered_rows, size_t max_buffered_bytes,
                                   size_t max_buffered_chunks)
        : ChunksSorter(state, sort_exprs, is_asc_order, is_null_first, sort_keys, true),
          _max_buffered_rows(max_buffered_rows),
          _max_buffered_bytes(max_buffered_bytes),
          _max_buffered_chunks(max_buffered_chunks),
          _init_merged_segment(false),
          _limit(limit),
          _offset(offset),
          _topn_type(topn_type) {
    DCHECK_GT(_get_number_of_rows_to_sort(), 0) << "output rows can't be empty";
    DCHECK(_topn_type == TTopNType::ROW_NUMBER || _offset == 0);
    auto& raw_chunks = _raw_chunks.chunks;
    // avoid too large buffer chunks
    raw_chunks.reserve(std::min<size_t>(max_buffered_chunks, 256));
}

ChunksSorterTopn::~ChunksSorterTopn() = default;

void ChunksSorterTopn::setup_runtime(RuntimeState* state, RuntimeProfile* profile, MemTracker* parent_mem_tracker) {
    ChunksSorter::setup_runtime(state, profile, parent_mem_tracker);
    _sort_filter_timer = ADD_TIMER(profile, "SortFilterTime");
    _sort_filter_rows = ADD_COUNTER(profile, "SortFilterRows", TUnit::UNIT);
}

// Cumulative chunks into _raw_chunks for sorting.
Status ChunksSorterTopn::update(RuntimeState* state, const ChunkPtr& chunk) {
    if (_limit == 0) {
        return Status::OK();
    }
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

    // Avoid TOPN from using too much memory.
    bool exceed_mem_limit = _raw_chunks.mem_usage() > _max_buffered_bytes;
    if (exceed_mem_limit) {
        return _sort_chunks(state);
    }

    // Try to accumulate more chunks.
    size_t rows_to_sort = _get_number_of_rows_to_sort();
    if (_merged_runs.num_rows() + _raw_chunks.size_of_rows < rows_to_sort) {
        return Status::OK();
    }

    // We have accumulated rows_to_sort rows to build merged runs.
    if (_merged_runs.num_rows() <= rows_to_sort) {
        return _sort_chunks(state);
    }

    // When number of Chunks exceeds _limit or _max_buffered_chunks, run sort and then part of
    // cached chunks can be dropped, so it can reduce the memory usage.
    // TopN caches _limit or _max_buffered_chunks primitive chunks,
    // performs sorting once, and discards extra rows
    if (chunk_number >= _max_buffered_chunks || _raw_chunks.size_of_rows > _max_buffered_rows) {
        return _sort_chunks(state);
    }

    return Status::OK();
}

Status ChunksSorterTopn::do_done(RuntimeState* state) {
    auto& raw_chunks = _raw_chunks.chunks;
    if (!raw_chunks.empty()) {
        RETURN_IF_ERROR(_sort_chunks(state));
    }

    _rank_pruning();

    // Skip top OFFSET rows
    size_t skip_offset = _offset;
    while (_merged_runs.num_chunks() != 0 && skip_offset > 0) {
        auto& run = _merged_runs.front();
        if (skip_offset >= run.num_rows()) {
            skip_offset -= run.num_rows();
            _merged_runs.pop_front();
        } else {
            _merged_runs.front().set_range(skip_offset, run.end_index());
            skip_offset = 0;
        }
    }

    return Status::OK();
}

std::vector<RuntimeFilter*>* ChunksSorterTopn::runtime_filters(ObjectPool* pool) {
    if (!_init_merged_segment) {
        return nullptr;
    }

    const size_t max_value_row_id = _get_number_of_rows_to_sort() - 1;

    // if we want build runtime filter,
    // we should reserve at least "rows_to_sort" rows
    if (max_value_row_id >= _merged_runs.num_rows()) {
        return nullptr;
    }

    size_t current_max_value_row_id = 0;
    const ColumnPtr* order_by_column_ptr = nullptr;
    if (_topn_type == TTopNType::RANK) {
        const auto& run = _merged_runs.back();
        order_by_column_ptr = &run.orderby[0];
        current_max_value_row_id = run.chunk->num_rows() - 1;
    } else {
        const auto& [run, max_rid] = _get_run_by_row_id(max_value_row_id);
        order_by_column_ptr = &run->orderby[0];
        current_max_value_row_id = max_rid;
    }
    const auto& order_by_column = *order_by_column_ptr;

    // _topn_type != TTopNType::RANK means we need reserve the max_value
    bool is_close_interval = _topn_type == TTopNType::RANK || _sort_desc.num_columns() != 1;
    bool asc = _sort_desc.descs[0].asc_order();
    bool null_first = _sort_desc.descs[0].is_null_first();

    if (_runtime_filter.empty()) {
        auto* rf = type_dispatch_predicate<RuntimeFilter*>(
                (*_sort_exprs)[0]->root()->type().type, false, detail::SortRuntimeFilterBuilder(), pool,
                order_by_column, current_max_value_row_id, asc, null_first, is_close_interval);
        if (rf == nullptr) {
            return nullptr;
        } else {
            _runtime_filter.emplace_back(rf);
        }
    } else {
        type_dispatch_predicate<std::nullptr_t>(
                (*_sort_exprs)[0]->root()->type().type, false, detail::SortRuntimeFilterUpdater(),
                _runtime_filter.back(), order_by_column, current_max_value_row_id, asc, null_first, is_close_interval);
    }

    return &_runtime_filter;
}

Status ChunksSorterTopn::get_next(ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_output_timer);
    if (_merged_runs.num_chunks() == 0) {
        *chunk = nullptr;
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    size_t chunk_size = _state->chunk_size();
    MergedRun& run = _merged_runs.front();
    *chunk = run.steal_chunk(chunk_size);
    if (*chunk != nullptr) {
        RETURN_IF_ERROR((*chunk)->downgrade());
    }
    if (run.empty()) {
        _merged_runs.pop_front();
    }
    *eos = false;
    return Status::OK();
}

size_t ChunksSorterTopn::get_output_rows() const {
    return _merged_runs.num_rows();
}

Status ChunksSorterTopn::_sort_chunks(RuntimeState* state) {
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

    // Step 1: extract datas from _raw_chunks into segments,
    // and initialize permutations.second when _init_merged_segment == false.
    RETURN_IF_ERROR(_build_sorting_data(state, permutations.second, segments));

    // Step 2: filter batch-chunks as permutations.first and permutations.second when _init_merged_segment == true.
    // sort part chunks in permutations.first and permutations.second, if _init_merged_segment == false means permutations.first is empty.
    RETURN_IF_ERROR(_filter_and_sort_data(state, permutations, segments));

    // Step 3: merge sort of two ordered groups
    // the first ordered group only contains permutations.first
    // the second ordered group contains both permutations.second and _merged_segment
    RETURN_IF_ERROR(_merge_sort_data_as_merged_segment(state, permutations, segments));

    return Status::OK();
}

Status ChunksSorterTopn::_build_sorting_data(RuntimeState* state, Permutation& permutation_second,
                                             DataSegments& segments) {
    SCOPED_TIMER(_build_timer);

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
        // Merge segments into segments for the convenience of merging sorted result.
        segments.emplace_back(_sort_exprs, cnk);
    }
    _raw_chunks.clear();

    // This time, because we will filter chunks before initialize permutations, so we just check memory usage.
    if (!_init_merged_segment) {
        // This time, just initialized permutations.second.
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
    uint32_t first_size = 0;
    for (uint32_t i = 0; i < size; ++i) {
        size_t nums = filter_array[i].size();
        for (uint32_t j = 0; j < nums; ++j) {
            if (filter_array[i][j] == DataSegment::SMALLER_THAN_MIN_OF_SEGMENT) {
                permutation[first_size] = {i, j};
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
            if (filter_array[i][j] == DataSegment::SMALLER_THAN_MIN_OF_SEGMENT) {
                permutations.first[first_size] = {i, j};
                ++first_size;
            } else if (filter_array[i][j] == DataSegment::INCLUDE_IN_SEGMENT) {
                permutations.second[second_size] = {i, j};
                ++second_size;
            }
        }
    }
}

// When _merged_segment's size is equal or larger than rows_to_sort, we take the first and last row from _merged_segment:
// step 1: use last row to filter chunk_size rows in segments into two parts, i.e. `INCLUDE_IN_SEGMENT` part and `LARGER_THAN_MAX_OF_SEGMENT` part.
// step 2: use first row to filter the `INCLUDE_IN_SEGMENT` part, further results into two parts, i.e. `SMALLER_THAN_MIN_OF_SEGMENT` part and `INCLUDE_IN_SEGMENT` part.
// step 3: set this result in filter_array of `SMALLER_THAN_MIN_OF_SEGMENT` part and `INCLUDE_IN_SEGMENT` part, and the `LARGER_THAN_MAX_OF_SEGMENT` part is simply dropped.
//
// When _merged_segment's size is smaller than rows_to_sort,
// we just use first row to filter all rows to get `SMALLER_THAN_MIN_OF_SEGMENT` part and `INCLUDE_IN_SEGMENT` part.
//
// The `SMALLER_THAN_MIN_OF_SEGMENT` part is stored in permutations.first while
// the `INCLUDE_IN_SEGMENT` part is stored in permutations.second. And these two parts will be sorted separately
Status ChunksSorterTopn::_filter_and_sort_data(RuntimeState* state, std::pair<Permutation, Permutation>& permutations,
                                               DataSegments& segments) {
    DCHECK(_get_number_of_order_by_columns() > 0) << "order by columns can't be empty";
    const size_t rows_to_sort = _get_number_of_rows_to_sort();

    if (_init_merged_segment) {
        std::vector<std::vector<uint8_t>> filter_array;
        uint32_t smaller_num, include_num;

        // Here are 2 cases:
        // case 1: _merged_runs.num_rows() >= rows_to_sort, which means we already have enough rows,
        // so we can use both index of `0` and `rows_to_sort - 1` as the left and right boundary to filter the coming input chunks
        // into three parts, `SMALLER_THAN_MIN_OF_SEGMENT`, `INCLUDE_IN_SEGMENT` and `LARGER_THAN_MAX_OF_SEGMENT`, and the
        // `LARGER_THAN_MAX_OF_SEGMENT` part is simply dropped
        // case 2: _merged_runs.num_rows() < rows_to_sort, which means we haven't have enough rows,
        // so we can only use the index of `0` as the left boundary to filter the coming input chunks into two parts, `SMALLER_THAN_MIN_OF_SEGMENT` and `INCLUDE_IN_SEGMENT`

        if (_merged_runs.num_rows() >= rows_to_sort) {
            SCOPED_TIMER(_sort_filter_timer);
            RETURN_IF_ERROR(_build_filter_from_high_low_comparison(segments, filter_array, _sort_desc, smaller_num,
                                                                   include_num));
        } else {
            SCOPED_TIMER(_sort_filter_timer);
            RETURN_IF_ERROR(
                    _build_filter_from_low_comparison(segments, filter_array, _sort_desc, smaller_num, include_num));
        }

        size_t filtered_rows = 0;
        for (auto& segment : segments) {
            filtered_rows += segment.chunk->num_rows();
        }

        {
            SCOPED_TIMER(_build_timer);
            permutations.first.resize(smaller_num);
            // `SMALLER_THAN_MIN_OF_SEGMENT` part is enough, so we ignore the `INCLUDE_IN_SEGMENT` part.
            if (smaller_num >= rows_to_sort) {
                // Use filter_array to set permutations.first.
                _set_permutation_before(permutations.first, segments.size(), filter_array);
                filtered_rows -= smaller_num;
            } else if (rows_to_sort > 1 || _topn_type == TTopNType::RANK) {
                // If rows_to_sort == 1, here are two cases:
                // case 1: _topn_type is TTopNType::ROW_NUMBER, first row and last row is the same identity. so we do nothing.
                // case 2: _topn_type is TTopNType::RANK, we need to contain all the rows equal with the last row of merged segment,
                // and these equals row maybe exist in the `INCLUDE_IN_SEGMENT` part

                // `SMALLER_THAN_MIN_OF_SEGMENT` part's size < rows_to_sort, we need set permutations.first and permutations.second.
                permutations.second.resize(include_num);

                // Use filter_array to set permutations.first and permutations.second.
                _set_permutation_complete(permutations, segments.size(), filter_array);
                filtered_rows -= smaller_num;
                filtered_rows -= include_num;
            }
        }
        if (_sort_filter_rows) {
            COUNTER_UPDATE(_sort_filter_rows, filtered_rows);
        }
    }

    return _partial_sort_col_wise(state, permutations, segments);
}

Status ChunksSorterTopn::_partial_sort_col_wise(RuntimeState* state, std::pair<Permutation, Permutation>& permutations,
                                                DataSegments& segments) {
    const size_t rows_to_sort = _get_number_of_rows_to_sort();
    SCOPED_TIMER(_sort_timer);

    std::vector<Columns> vertical_chunks;
    for (auto& segment : segments) {
        vertical_chunks.push_back(segment.order_by_columns);
    }
    auto do_sort = [&](Permutation& perm, size_t limit) {
        return sort_vertical_chunks(state->cancelled_ref(), vertical_chunks, _sort_desc, perm, limit,
                                    _topn_type == TTopNType::RANK);
    };

    size_t first_size = std::min(permutations.first.size(), rows_to_sort);

    // Sort the first
    if (first_size > 0) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(do_sort(permutations.first, first_size));
    }

    // Sort the second
    if (rows_to_sort > first_size) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(do_sort(permutations.second, rows_to_sort - first_size));
    }

    return Status::OK();
}

// TODO: process current_max_row_id
Status ChunksSorterTopn::_build_filter_from_high_low_comparison(const DataSegments& data_segments,
                                                                std::vector<std::vector<uint8_t>>& filter_array,
                                                                const SortDescs& sort_descs, uint32_t& smaller_num,
                                                                uint32_t& include_num) {
    DCHECK(_merged_runs.num_rows() > 0);
    size_t data_segment_size = data_segments.size();

    std::vector<CompareVector> compare_results_array(data_segment_size);
    // First compare the chunk with last row of this segment.
    const size_t max_value_row_id = _get_number_of_rows_to_sort() - 1;
    const auto& [run, max_rid] = _get_run_by_row_id(max_value_row_id);
    get_compare_results_colwise(max_rid, run->orderby, compare_results_array, data_segments, sort_descs);

    include_num = 0;
    filter_array.resize(data_segment_size);
    for (size_t i = 0; i < data_segment_size; ++i) {
        const DataSegment& segment = data_segments[i];
        size_t rows = segment.chunk->num_rows();
        filter_array[i].resize(rows);

        for (size_t j = 0; j < rows; ++j) {
            if (compare_results_array[i][j] <= 0) {
                filter_array[i][j] = DataSegment::INCLUDE_IN_SEGMENT;
                ++include_num;
            }
        }
    }

    // Second compare with first row of this chunk, use rows from first compare.
    {
        for (size_t i = 0; i < data_segment_size; i++) {
            for (auto& cmp : compare_results_array[i]) {
                if (cmp < 0) {
                    cmp = 0;
                }
            }
        }
        get_compare_results_colwise(0, _lowest_merged_run().orderby, compare_results_array, data_segments, sort_descs);
    }

    smaller_num = 0;
    for (size_t i = 0; i < data_segment_size; ++i) {
        const DataSegment& segment = data_segments[i];
        size_t rows = segment.chunk->num_rows();

        for (size_t j = 0; j < rows; ++j) {
            if (compare_results_array[i][j] < 0) {
                filter_array[i][j] = DataSegment::SMALLER_THAN_MIN_OF_SEGMENT;
                ++smaller_num;
            }
        }
    }
    include_num -= smaller_num;
    return Status::OK();
}

Status ChunksSorterTopn::_build_filter_from_low_comparison(const DataSegments& data_segments,
                                                           std::vector<std::vector<uint8_t>>& filter_array,
                                                           const SortDescs& sort_descs, uint32_t& smaller_num,
                                                           uint32_t& include_num) {
    DCHECK(_merged_runs.num_rows() > 0);
    size_t data_segment_size = data_segments.size();
    std::vector<CompareVector> compare_results_array(data_segment_size);

    get_compare_results_colwise(0, _lowest_merged_run().orderby, compare_results_array, data_segments, sort_descs);

    smaller_num = 0, include_num = 0;
    filter_array.resize(data_segment_size);
    for (size_t i = 0; i < data_segment_size; ++i) {
        size_t rows = data_segments[i].chunk->num_rows();
        filter_array[i].resize(rows);

        for (size_t j = 0; j < rows; ++j) {
            if (compare_results_array[i][j] < 0) {
                filter_array[i][j] = DataSegment::SMALLER_THAN_MIN_OF_SEGMENT;
                ++smaller_num;
            } else {
                filter_array[i][j] = DataSegment::INCLUDE_IN_SEGMENT;
                ++include_num;
            }
        }
    }

    return Status::OK();
}

Status ChunksSorterTopn::_merge_sort_data_as_merged_segment(RuntimeState* state,
                                                            std::pair<Permutation, Permutation>& new_permutation,
                                                            DataSegments& segments) {
    SCOPED_TIMER(_merge_timer);

    if (_init_merged_segment) {
        RETURN_IF_ERROR(_hybrid_sort_common(state, new_permutation, segments));
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

// Take rows_to_sort rows from permutation_second merge-sort with _merged_segment.
// And take result datas into big_chunk.
Status ChunksSorterTopn::_merge_sort_common(MergedRuns* dst, DataSegments& segments, const size_t rows_to_keep,
                                            Permutation& permutation_second) {
    // Assemble the permutated segments into a chunk
    size_t right_chunk_size = permutation_second.size();
    ChunkUniquePtr right_unique_chunk =
            dst->empty() ? segments[permutation_second[0].chunk_index].chunk->clone_empty(right_chunk_size)
                         : dst->front().chunk->clone_empty(right_chunk_size);
    {
        std::vector<ChunkPtr> right_chunks;
        for (auto& segment : segments) {
            right_chunks.push_back(segment.chunk);
        }
        materialize_by_permutation(right_unique_chunk.get(), right_chunks, permutation_second);
        permutation_second = {};
    }

    Columns right_columns;
    // ExprContext::evaluate may report error if input chunk is empty
    if (right_unique_chunk->is_empty()) {
        right_columns.assign(_sort_exprs->size(), nullptr);
    } else {
        for (auto expr : *_sort_exprs) {
            ASSIGN_OR_RETURN(auto column, expr->evaluate(right_unique_chunk.get()));
            right_columns.push_back(std::move(column));
        }
    }

    if (_merged_runs.num_chunks() > 1 || _merged_runs.mem_usage() > _max_buffered_bytes) {
        // merge to multi sorted chunks
        RETURN_IF_ERROR(merge_sorted_chunks(_sort_desc, _sort_exprs, _merged_runs, std::move(right_unique_chunk),
                                            rows_to_keep, dst));
    } else {
        // merge to big chunk
        // prepare left chunk
        MergedRun merged_run = std::move(_merged_runs.front());
        _merged_runs.pop_front();
        ChunkPtr left_chunk = std::move(merged_run.chunk);
        Columns left_columns = std::move(merged_run.orderby);

        // prepare right chunk
        ChunkPtr right_chunk = std::move(right_unique_chunk);

        Permutation merged_perm;
        merged_perm.reserve(left_chunk->num_rows() + right_chunk->num_rows());

        RETURN_IF_ERROR(merge_sorted_chunks_two_way(_sort_desc, {left_chunk, left_columns},
                                                    {right_chunk, right_columns}, &merged_perm));
        CHECK_GE(merged_perm.size(), rows_to_keep);
        merged_perm.resize(rows_to_keep);

        // materialize into the dst runs
        std::vector<ChunkPtr> chunks{left_chunk, right_chunk};
        ChunkUniquePtr big_chunk;
        if (dst->num_chunks() == 0) {
            big_chunk = segments[permutation_second[0].chunk_index].chunk->clone_empty(rows_to_keep);
        } else {
            big_chunk = std::move(dst->front().chunk);
            dst->pop_front();
        }
        materialize_by_permutation(big_chunk.get(), chunks, merged_perm);
        RETURN_IF_ERROR(big_chunk->upgrade_if_overflow());
        ASSIGN_OR_RETURN(auto run, MergedRun::build(std::move(big_chunk), *_sort_exprs));
        dst->push_back(std::move(run));
    }

    return Status::OK();
}

Status ChunksSorterTopn::_hybrid_sort_common(RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation,
                                             DataSegments& segments) {
    const size_t rows_to_sort = _get_number_of_rows_to_sort();

    const size_t first_size = new_permutation.first.size();
    const size_t second_size = new_permutation.second.size();

    if (first_size == 0 && second_size == 0) {
        return Status::OK();
    }

    size_t rows_to_keep = rows_to_sort;
    if (_topn_type == TTopNType::RANK && first_size > rows_to_keep) {
        // For rank type, the number of rows may be greater than the specified limit rank number
        // For example, given set [1, 1, 1, 1, 1] and limit rank number=2, all the element's rank is 1,
        // so the size of `SMALLER_THAN_MIN_OF_SEGMENT` part may greater than the specified limit rank number(2)
        rows_to_keep = first_size;
    }

    // There are three parts of data
    // _merged_runs, the previously sorted one
    // the `SMALLER_THAN_MIN_OF_SEGMENT` part
    // the `INCLUDE_IN_SEGMENT` part

    // First, we find elements from `SMALLER_THAN_MIN_OF_SEGMENT`
    MergedRuns merged_runs;
    if (first_size > 0) {
        ChunkUniquePtr big_chunk;
        std::vector<ChunkPtr> chunks;
        for (auto& segment : segments) {
            chunks.push_back(segment.chunk);
        }
        big_chunk = segments[new_permutation.first[0].chunk_index].chunk->clone_empty(first_size);
        materialize_by_permutation(big_chunk.get(), chunks, new_permutation.first);
        rows_to_keep -= first_size;
        ASSIGN_OR_RETURN(auto run, MergedRun::build(std::move(big_chunk), *_sort_exprs));
        merged_runs.push_back(std::move(run));
    }

    // Seoncd, there are two cases:
    // case1: rows_to_keep == 0, which measn `SMALLER_THAN_MIN_OF_SEGMENT` part itself suffice, we can ignore both
    // `INCLUDE_IN_SEGMENT` part and _merged_segment
    // case2: rows_to_keep > 0, which means `SMALLER_THAN_MIN_OF_SEGMENT` part itself not suffice, we need to get more elements
    // from both `INCLUDE_IN_SEGMENT` part and _merged_segment. And notice that `INCLUDE_IN_SEGMENT` part may be empty
    if (rows_to_keep > 0) {
        const size_t sorted_size = _merged_runs.num_rows();
        rows_to_keep = std::min(rows_to_keep, sorted_size + second_size);
        if (_topn_type == TTopNType::RANK && sorted_size + second_size > rows_to_keep) {
            // For rank type, there may exist a wide equal range, so we need to keep all elements of part2 and part3
            rows_to_keep = sorted_size + second_size;
        }
        RETURN_IF_ERROR(_merge_sort_common(&merged_runs, segments, rows_to_keep, new_permutation.second));
    }

    _merged_runs = std::move(merged_runs);

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

    ChunkUniquePtr big_chunk = segments[new_permutation[0].chunk_index].chunk->clone_empty(rows_to_keep);

    // Initial this big chunk.
    std::vector<ChunkPtr> chunks;
    for (auto& segment : segments) {
        chunks.push_back(segment.chunk);
    }
    new_permutation.resize(rows_to_keep);
    materialize_by_permutation(big_chunk.get(), chunks, new_permutation);

    RETURN_IF_ERROR(big_chunk->upgrade_if_overflow());
    ASSIGN_OR_RETURN(auto run, MergedRun::build(std::move(big_chunk), *_sort_exprs));
    _merged_runs.push_back(std::move(run));

    return Status::OK();
}

void ChunksSorterTopn::_rank_pruning() {
    if (_topn_type != TTopNType::RANK) {
        return;
    }
    if (!_init_merged_segment) {
        return;
    }
    if (_merged_runs.num_rows() <= _get_number_of_rows_to_sort()) {
        return;
    }

    const auto peer_group_start = _get_number_of_rows_to_sort() - 1;
    bool found = false;

    // value at position peer_group_start + 1
    size_t target_index = peer_group_start;
    size_t index_in_runs = 0;
    size_t index_in_chunk = 0;
    std::vector<Datum> datums;

    const auto& merged_runs = _merged_runs;
    for (int i = 0; i < merged_runs.num_chunks(); ++i) {
        if (target_index > merged_runs.at(i).num_rows()) {
            target_index -= merged_runs.at(i).num_rows();
        } else {
            index_in_runs = i;
            index_in_chunk = target_index;
            break;
        }
    }

    size_t peer_group_end_index_in_chunk = 0;
    size_t peer_group_end_index_in_runs = 0;

    auto found_peer_group_end = [&merged_runs, index_in_runs, index_in_chunk](const MergedRun& run, size_t begin,
                                                                              size_t end) -> std::pair<int, bool> {
        const auto& target_run = merged_runs.at(index_in_runs);
        for (int j = begin; j < end; ++j) {
            for (size_t k = 0; k < run.orderby.size(); ++k) {
                if (run.orderby[k]->compare_at(j, index_in_chunk, *target_run.orderby[k], 1) != 0) {
                    return {j, true};
                }
            }
        }
        return {0, false};
    };

    for (int i = index_in_runs; !found && i < merged_runs.num_chunks(); ++i) {
        const auto& run = merged_runs.at(i);
        if (run.empty()) continue;
        if (i == index_in_runs) {
            std::tie(peer_group_end_index_in_chunk, found) = found_peer_group_end(run, index_in_chunk, run.end_index());
        } else {
            std::tie(peer_group_end_index_in_chunk, found) =
                    found_peer_group_end(run, run.start_index(), run.end_index());
        }
        if (found) {
            peer_group_end_index_in_runs = i;
            break;
        }
    }

    if (found) {
        _merged_runs.at(peer_group_end_index_in_runs).set_range(0, peer_group_end_index_in_chunk);
        for (int i = peer_group_end_index_in_runs + 1; i < _merged_runs.num_chunks(); ++i) {
            _merged_runs.pop_back();
        }
    }
}

} // namespace starrocks
