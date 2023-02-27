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

#include <algorithm>
#include <memory>
#include <utility>

#include "column/chunk.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "runtime/chunk_cursor.h"
#include "util/array_view.hpp"

namespace starrocks {

// Some search algorithms with cursor
struct CursorAlgo {
    static int compare_tail(const SortDescs& desc, const SortedRun& left, const SortedRun& right) {
        size_t lhs_tail = left.range.second - 1;
        size_t rhs_tail = right.range.second - 1;
        return left.compare_row(desc, right, lhs_tail, rhs_tail);
    }

    static void trim_permutation(const SortedRun& left, const SortedRun& right, Permutation& perm) {
        if (perm.size() <= left.num_rows() + right.num_rows()) {
            return;
        }
        size_t start = left.range.first + right.range.first;
        size_t end = left.range.second + right.range.second;
        std::copy(perm.begin() + start, perm.begin() + end, perm.begin());
        perm.resize(end - start);
    }

    // TODO(Murphy): optimize with column-wise binary-search
    // Find the first row in `run` greater than last row in cut. AKA upper-bound
    // @return row index of upper bound
    static size_t chunk_upper_bound(const SortDescs& sort_descs, const SortedRun& run, const SortedRun& cut) {
        size_t first = run.start_index();
        size_t count = run.num_rows();
        while (count > 0) {
            size_t mid = first + count / 2;
            int x = run.compare_row(sort_descs, cut, mid, cut.end_index() - 1);
            if (x <= 0) {
                first = mid + 1;
                count -= count / 2 + 1;
            } else {
                count /= 2;
            }
        }
        return first;
    }
};

MergeTwoCursor::MergeTwoCursor(const SortDescs& sort_desc, std::unique_ptr<SimpleChunkSortCursor>&& left_cursor,
                               std::unique_ptr<SimpleChunkSortCursor>&& right_cursor)
        : _sort_desc(sort_desc), _left_cursor(std::move(left_cursor)), _right_cursor(std::move(right_cursor)) {
    _chunk_provider = [&](ChunkUniquePtr* output, bool* eos) -> bool {
        if (output == nullptr || eos == nullptr) {
            return is_data_ready();
        }
        auto chunk = next();
        *eos = is_eos();
        if (!chunk.ok() || !chunk.value()) {
            return false;
        } else {
            *output = std::move(chunk.value());
            return true;
        }
    };
}

// Consume all inputs and produce output through the callback function
Status MergeTwoCursor::consume_all(const ChunkConsumer& output) {
    for (auto chunk = next(); chunk.ok() && !is_eos(); chunk = next()) {
        if (chunk.value()) {
            output(std::move(chunk.value()));
        }
    }

    return Status::OK();
}

// use this as cursor
std::unique_ptr<SimpleChunkSortCursor> MergeTwoCursor::as_chunk_cursor() {
    return std::make_unique<SimpleChunkSortCursor>(as_provider(), _left_cursor->get_sort_exprs());
}
bool MergeTwoCursor::is_data_ready() {
    return _left_cursor->is_data_ready() && _right_cursor->is_data_ready();
}

bool MergeTwoCursor::is_eos() {
    return _left_run.empty() && _left_cursor->is_eos() && _right_run.empty() && _right_cursor->is_eos();
}

StatusOr<ChunkUniquePtr> MergeTwoCursor::next() {
    if (!is_data_ready() || is_eos()) {
        return ChunkUniquePtr();
    }

    if (move_cursor()) {
        return ChunkUniquePtr();
    }
    return merge_sorted_cursor_two_way();
}

bool MergeTwoCursor::move_cursor() {
    DCHECK(is_data_ready());
    DCHECK(!is_eos());

    bool eos = _left_run.empty() || _right_run.empty();

    if (_left_run.empty() && !_left_cursor->is_eos()) {
        auto chunk = _left_cursor->try_get_next();
        if (chunk.first) {
            _left_run = SortedRun(ChunkPtr(chunk.first.release()), chunk.second);
        }
    }
    if (_right_run.empty() && !_right_cursor->is_eos()) {
        auto chunk = _right_cursor->try_get_next();
        if (chunk.first) {
            _right_run = SortedRun(ChunkPtr(chunk.first.release()), chunk.second);
        }
    }

    if (!_left_run.empty() && !_right_run.empty()) {
        eos = false;
    }

    // one is eos but the other has data stream
    // we will passthrough the other data stream
    if ((_left_cursor->is_eos() && !_right_run.empty()) || (_right_cursor->is_eos() && !_left_run.empty())) {
        eos = false;
    }

    return eos;
}

// 1. Find smaller tail
// 2. Cutoff the chunk based on tail
// 3. Merge two chunks and output
// 4. Move to next
StatusOr<ChunkUniquePtr> MergeTwoCursor::merge_sorted_cursor_two_way() {
    DCHECK(!(_left_run.empty() && _right_run.empty()));
    const SortDescs& sort_desc = _sort_desc;
    ChunkUniquePtr result;

    //debug scope
#ifndef NDEBUG
    DCHECK(!(_left_is_empty && !_left_run.empty()));
    DCHECK(!(_right_is_empty && !_right_run.empty()));

    _left_is_empty |= _left_run.empty();
    _right_is_empty |= _right_run.empty();
#endif

    int intersect = _left_run.intersect(sort_desc, _right_run);
    if (intersect < 0) {
        result = _left_run.clone_slice();
        _left_run.reset();
        VLOG_ROW << "merge_sorted_cursor_two_way output left run";
    } else if (intersect > 0) {
        result = _right_run.clone_slice();
        _right_run.reset();
        VLOG_ROW << "merge_sorted_cursor_two_way output right run";
    } else {
        ASSIGN_OR_RETURN(auto merged, merge_sorted_intersected_cursor(_left_run, _right_run));
        result = std::move(merged);
    }

    return result;
}

StatusOr<ChunkUniquePtr> MergeTwoCursor::merge_sorted_intersected_cursor(SortedRun& run1, SortedRun& run2) {
    const auto& sort_desc = _sort_desc;

    int tail_cmp = CursorAlgo::compare_tail(sort_desc, run1, run2);

    Permutation permutation;

    // Merge partial chunk
    RETURN_IF_ERROR(merge_sorted_chunks_two_way(sort_desc, run1, run2, &permutation));
    DCHECK_EQ(run1.num_rows() + run2.num_rows(), permutation.size());

    size_t merged_rows = std::min(run1.num_rows(), run2.num_rows());

    ChunkUniquePtr merged = run2.chunk->clone_empty(merged_rows);

    auto perm_view = PermutationView(permutation.data(), merged_rows);

    // TODO: avoid copy the whole chunk, but copy orderby columns only
    materialize_by_permutation(merged.get(), {run1.chunk, run2.chunk}, perm_view);

    auto left_rows = permutation.size() - merged_rows;
    perm_view = PermutationView(permutation.data() + merged_rows, left_rows);
    ChunkUniquePtr left_merged = run2.chunk->clone_empty(left_rows);
    materialize_by_permutation(left_merged.get(), {run1.chunk, run2.chunk}, perm_view);

    if (tail_cmp <= 0) {
        run1.reset();
        run2 = SortedRun(std::move(left_merged), _left_cursor->get_sort_exprs());
    } else {
        run1 = SortedRun(std::move(left_merged), _left_cursor->get_sort_exprs());
        run2.reset();
    }
    DCHECK_EQ(merged_rows + left_rows, permutation.size());

    return merged;
}

// TODO: avoid copy the whole chunk in cascade merge, but copy order-by column only
// In the scenario that chunk has many columns but order by a few column, that could save a lot of cpu cycles
Status MergeCursorsCascade::init(const SortDescs& sort_desc,
                                 std::vector<std::unique_ptr<SimpleChunkSortCursor>>&& cursors) {
    std::vector<std::unique_ptr<SimpleChunkSortCursor>> current_level = std::move(cursors);

    while (current_level.size() > 1) {
        std::vector<std::unique_ptr<SimpleChunkSortCursor>> next_level;
        next_level.reserve(current_level.size() / 2);

        int level_size = current_level.size() & ~1;
        for (int i = 0; i < level_size; i += 2) {
            auto& left = current_level[i];
            auto& right = current_level[i + 1];
            _mergers.push_back(std::make_unique<MergeTwoCursor>(sort_desc, std::move(left), std::move(right)));
            next_level.push_back(_mergers.back()->as_chunk_cursor());
        }
        if (current_level.size() % 2 == 1) {
            next_level.push_back(std::move(current_level.back()));
        }

        std::swap(next_level, current_level);
    }
    DCHECK_EQ(1, current_level.size());
    _root_cursor = std::move(current_level.front());

    return Status::OK();
}

bool MergeCursorsCascade::is_data_ready() {
    return _root_cursor->is_data_ready();
}

bool MergeCursorsCascade::is_eos() {
    return _root_cursor->is_eos();
}

ChunkUniquePtr MergeCursorsCascade::try_get_next() {
    return _root_cursor->try_get_next().first;
}

Status MergeCursorsCascade::consume_all(const ChunkConsumer& consumer) {
    while (!is_eos()) {
        ChunkUniquePtr chunk = try_get_next();
        if (!!chunk) {
            consumer(std::move(chunk));
        }
    }
    return Status::OK();
}

Status merge_sorted_cursor_two_way(const SortDescs& sort_desc, std::unique_ptr<SimpleChunkSortCursor> left_cursor,
                                   std::unique_ptr<SimpleChunkSortCursor> right_cursor, const ChunkConsumer& output) {
    MergeTwoCursor merger(sort_desc, std::move(left_cursor), std::move(right_cursor));
    return merger.consume_all(std::move(output));
}

Status merge_sorted_cursor_cascade(const SortDescs& sort_desc,
                                   std::vector<std::unique_ptr<SimpleChunkSortCursor>>&& cursors,
                                   const ChunkConsumer& consumer) {
    MergeCursorsCascade merger;
    RETURN_IF_ERROR(merger.init(sort_desc, std::move(cursors)));
    CHECK(merger.is_data_ready());
    merger.consume_all(std::move(consumer));
    return Status::OK();
}

} // namespace starrocks
