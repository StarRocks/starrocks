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
#include "column/type_traits.h"
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

ChunksSorterTopn::ChunksSorterTopn(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                                   const std::vector<bool>* is_asc_order, const std::vector<bool>* is_null_first,
                                   const std::string& sort_keys, size_t offset, size_t limit,
                                   const TTopNType::type topn_type, size_t max_buffered_chunks)
        : ChunksSorter(state, sort_exprs, is_asc_order, is_null_first, sort_keys, true),
          _max_buffered_chunks(max_buffered_chunks),
          _init_merged_segment(false),
          _limit(limit),
          _offset(offset),
          _topn_type(topn_type) {
    DCHECK_GT(_get_number_of_rows_to_sort(), 0) << "output rows can't be empty";
    DCHECK(_topn_type == TTopNType::ROW_NUMBER || _offset == 0);
    auto& raw_chunks = _raw_chunks.chunks;
    raw_chunks.reserve(max_buffered_chunks);
}

ChunksSorterTopn::~ChunksSorterTopn() = default;

void ChunksSorterTopn::setup_runtime(RuntimeState* state, RuntimeProfile* profile, MemTracker* parent_mem_tracker) {
    ChunksSorter::setup_runtime(state, profile, parent_mem_tracker);
    _sort_filter_timer = ADD_TIMER(profile, "SortFilterTime");
    _sort_filter_rows = ADD_COUNTER(profile, "SortFilterRows", TUnit::UNIT);
}

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

    // When number of Chunks exceeds _limit or _size_of_chunk_batch, run sort and then part of
    // cached chunks can be dropped, so it can reduce the memory usage.
    // TopN caches _limit or _size_of_chunk_batch primitive chunks,
    // performs sorting once, and discards extra rows

    if (_limit > 0 && (chunk_number >= _limit || chunk_number >= _max_buffered_chunks)) {
        RETURN_IF_ERROR(_sort_chunks(state));
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

std::vector<RuntimeFilter*>* ChunksSorterTopn::runtime_filters(ObjectPool* pool) {
    if (!_init_merged_segment) {
        return nullptr;
    }

    const size_t max_value_row_id = _get_number_of_rows_to_sort() - 1;
    const auto& order_by_column = _merged_segment.order_by_columns[0];

    // if we want build runtime filter,
    // we should reserve at least "rows_to_sort" rows
    if (max_value_row_id >= order_by_column->size()) {
        return nullptr;
    }
    size_t current_max_value_row_id = _topn_type == TTopNType::RANK ? order_by_column->size() - 1 : max_value_row_id;
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
    if (_next_output_row >= _merged_segment.chunk->num_rows()) {
        *chunk = nullptr;
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    size_t count = std::min(size_t(_state->chunk_size()), _merged_segment.chunk->num_rows() - _next_output_row);
    chunk->reset(_merged_segment.chunk->clone_empty(count).release());
    (*chunk)->append_safe(*_merged_segment.chunk, _next_output_row, count);
    RETURN_IF_ERROR((*chunk)->downgrade());
    _next_output_row += count;
    return Status::OK();
}

size_t ChunksSorterTopn::get_output_rows() const {
    return _merged_segment.chunk->num_rows();
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
        // case 1: _merged_segment.chunk->num_rows() >= rows_to_sort, which means we already have enough rows,
        // so we can use both index of `0` and `rows_to_sort - 1` as the left and right boundary to filter the coming input chunks
        // into three parts, `SMALLER_THAN_MIN_OF_SEGMENT`, `INCLUDE_IN_SEGMENT` and `LARGER_THAN_MAX_OF_SEGMENT`, and the
        // `LARGER_THAN_MAX_OF_SEGMENT` part is simply dropped
        // case 2: _merged_segment.chunk->num_rows() < rows_to_sort, which means we haven't have enough rows,
        // so we can only use the index of `0` as the left boundary to filter the coming input chunks into two parts, `SMALLER_THAN_MIN_OF_SEGMENT` and `INCLUDE_IN_SEGMENT`

        if (_merged_segment.chunk->num_rows() >= rows_to_sort) {
            SCOPED_TIMER(_sort_filter_timer);
            RETURN_IF_ERROR(_merged_segment.get_filter_array(segments, rows_to_sort, filter_array, _sort_desc,
                                                             smaller_num, include_num));
        } else {
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
Status ChunksSorterTopn::_merge_sort_common(ChunkPtr& big_chunk, DataSegments& segments, const size_t rows_to_keep,
                                            size_t sorted_size, Permutation& permutation_second) {
    // Assemble the permutated segments into a chunk
    std::vector<ChunkPtr> right_chunks;
    for (auto& segment : segments) {
        right_chunks.push_back(segment.chunk);
    }
    ChunkPtr right_chunk = big_chunk->clone_empty(permutation_second.size());
    materialize_by_permutation(right_chunk.get(), right_chunks, permutation_second);
    Columns right_columns;
    // ExprContext::evaluate may report error if input chunk is empty
    if (right_chunk->is_empty()) {
        right_columns.assign(_sort_exprs->size(), nullptr);
    } else {
        for (auto expr : *_sort_exprs) {
            auto maybe_column = expr->evaluate(right_chunk.get());
            RETURN_IF_ERROR(maybe_column);
            right_columns.push_back(maybe_column.value());
        }
    }

    ChunkPtr left_chunk = _merged_segment.chunk;
    Columns left_columns = _merged_segment.order_by_columns;

    Permutation merged_perm;
    // avoid exaggerated limit + offset, for an example select * from t order by col limit 9223372036854775800,1
    merged_perm.reserve(std::min<size_t>(rows_to_keep, 10'000'000ul));

    RETURN_IF_ERROR(merge_sorted_chunks_two_way(_sort_desc, {left_chunk, left_columns}, {right_chunk, right_columns},
                                                &merged_perm));
    CHECK_GE(merged_perm.size(), rows_to_keep);
    merged_perm.resize(rows_to_keep);

    std::vector<ChunkPtr> chunks{left_chunk, right_chunk};
    materialize_by_permutation(big_chunk.get(), chunks, merged_perm);
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

    ChunkPtr big_chunk;
    std::vector<ChunkPtr> chunks;
    for (auto& segment : segments) {
        chunks.push_back(segment.chunk);
    }

    // There are three parts of data
    // _merged_segment, the previously sorted one
    // the `SMALLER_THAN_MIN_OF_SEGMENT` part
    // the `INCLUDE_IN_SEGMENT` part

    // First, we find elements from `SMALLER_THAN_MIN_OF_SEGMENT`
    if (first_size > 0) {
        big_chunk.reset(segments[new_permutation.first[0].chunk_index].chunk->clone_empty(first_size).release());
        materialize_by_permutation(big_chunk.get(), chunks, new_permutation.first);
        rows_to_keep -= first_size;
    }

    // Seoncd, there are two cases:
    // case1: rows_to_keep == 0, which measn `SMALLER_THAN_MIN_OF_SEGMENT` part itself suffice, we can ignore both
    // `INCLUDE_IN_SEGMENT` part and _merged_segment
    // case2: rows_to_keep > 0, which means `SMALLER_THAN_MIN_OF_SEGMENT` part itself not suffice, we need to get more elements
    // from both `INCLUDE_IN_SEGMENT` part and _merged_segment. And notice that `INCLUDE_IN_SEGMENT` part may be empty
    if (rows_to_keep > 0) {
        const size_t sorted_size = _merged_segment.chunk->num_rows();
        rows_to_keep = std::min(rows_to_keep, sorted_size + second_size);
        if (big_chunk == nullptr) {
            big_chunk.reset(segments[new_permutation.second[0].chunk_index].chunk->clone_empty(rows_to_keep).release());
        }
        if (_topn_type == TTopNType::RANK && sorted_size + second_size > rows_to_keep) {
            // For rank type, there may exist a wide equal range, so we need to keep all elements of part2 and part3
            rows_to_keep = sorted_size + second_size;
        }
        RETURN_IF_ERROR(_merge_sort_common(big_chunk, segments, rows_to_keep, sorted_size, new_permutation.second));
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
    materialize_by_permutation(big_chunk.get(), chunks, new_permutation);

    RETURN_IF_ERROR(big_chunk->upgrade_if_overflow());
    _merged_segment.init(_sort_exprs, big_chunk);

    return Status::OK();
}

void ChunksSorterTopn::_rank_pruning() {
    if (_topn_type != TTopNType::RANK) {
        return;
    }
    if (!_init_merged_segment) {
        return;
    }
    if (_merged_segment.chunk->num_rows() <= _get_number_of_rows_to_sort()) {
        return;
    }
    DCHECK(!_merged_segment.order_by_columns.empty());

    const auto size = _merged_segment.chunk->num_rows();
    const auto peer_group_start = _get_number_of_rows_to_sort() - 1;
    size_t peer_group_end = size;
    bool found = false;

    for (int i = peer_group_start + 1; !found && i < size; ++i) {
        for (auto& column : _merged_segment.order_by_columns) {
            if (column->compare_at(i, i - 1, *column, 1) != 0) {
                peer_group_end = i;
                found = true;
                break;
            }
        }
    }

    if (found) {
        _merged_segment.chunk->set_num_rows(peer_group_end);
        for (auto& column : _merged_segment.order_by_columns) {
            column->resize(peer_group_end);
        }
    }
}

} // namespace starrocks
