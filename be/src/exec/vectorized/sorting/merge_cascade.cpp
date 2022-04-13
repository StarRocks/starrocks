// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sorting.h"
#include "runtime/vectorized/chunk_cursor.h"

namespace starrocks::vectorized {

// Some search algorithms with cursor
struct CursorAlgo {
    static int compare_tail(const SortDescs& desc, const SortedRun& left, const SortedRun& right) {
        size_t lhs_tail = left.num_rows() - 1;
        size_t rhs_tail = right.num_rows() - 1;
        return compare_chunk_row(desc, *(left.chunk), *(right.chunk), lhs_tail, rhs_tail);
    }

    static void trim_permutation(SortedRun left, SortedRun right, Permutation& perm) {
        size_t start = left.range.first + right.range.first;
        size_t end = left.range.second + right.range.second;
        std::copy(perm.begin() + start, perm.begin() + end, perm.begin());
        perm.resize(end - start);
    }

    // Find upper_bound in left column based on right row
    static size_t upper_bound(SortDesc desc, const Column& column, std::pair<size_t, size_t> range,
                              const Column& rhs_column, size_t rhs_row) {
        size_t first = range.first;
        size_t count = range.second - range.first;
        while (count > 0) {
            size_t mid = first + count / 2;
            int x = column.compare_at(mid, rhs_row, rhs_column, -1) * desc.sort_order;
            if (x <= 0) {
                first = mid + 1;
                count -= count / 2 + 1;
            } else {
                count /= 2;
            }
        }
        return first;
    }

    static size_t lower_bound(SortDesc desc, const Column& column, std::pair<size_t, size_t> range,
                              const Column& rhs_column, size_t rhs_row) {
        size_t first = range.first;
        size_t count = range.second - range.first;
        while (count > 0) {
            size_t mid = first + count / 2;
            int x = column.compare_at(mid, rhs_row, rhs_column, desc.null_first) * desc.sort_order;
            if (x < 0) {
                first = mid + 1;
                count -= count / 2 + 1;
            } else {
                count /= 2;
            }
        }
        return first;
    }

    // Cutoff by upper_bound
    // @return last row index of upper bound
    static size_t cutoff_run(const SortDescs& sort_descs, SortedRun run, std::pair<SortedRun, size_t> cut) {
        size_t res = 0;
        std::pair<size_t, size_t> search_range = run.range;

        for (int i = 0; i < run.num_columns(); i++) {
            auto& lhs_col = *run.get_column(i);
            auto& rhs_col = *cut.first.get_column(i);
            SortDesc desc = sort_descs.get_column_desc(i);
            size_t lower = lower_bound(desc, lhs_col, search_range, rhs_col, cut.second);
            size_t upper = upper_bound(desc, lhs_col, search_range, rhs_col, cut.second);
            res = upper;
            if (upper - lower <= 1) {
                break;
            }
            search_range = {lower, upper};
        }
        return res;
    }
};

MergeTwoCursor::MergeTwoCursor(const SortDescs& sort_desc, std::unique_ptr<SimpleChunkSortCursor>&& left_cursor,
                               std::unique_ptr<SimpleChunkSortCursor>&& right_cursor)
        : _sort_desc(sort_desc), _left_cursor(std::move(left_cursor)), _right_cursor(std::move(right_cursor)) {
    _chunk_provider = [&](Chunk** output, bool* eos) -> bool {
        auto chunk = next();
        if (!chunk.ok() || !chunk.value()) return false;
        *output = chunk.value().release();
        *eos = is_eos();
        return true;
    };
}

bool MergeTwoCursor::is_data_ready() {
    return _left_cursor->is_data_ready() && _right_cursor->is_data_ready();
}

bool MergeTwoCursor::is_eos() {
    return _left_cursor->is_eos() && _right_cursor->is_eos();
}

StatusOr<ChunkUniquePtr> MergeTwoCursor::next() {
    if (_left_run.empty() && _right_run.empty()) {
        return ChunkUniquePtr();
    }
    if (!is_data_ready() || is_eos()) {
        return ChunkUniquePtr();
    }
    if (!move_cursor()) {
        return ChunkUniquePtr();
    }
    return merge_sorted_cursor_two_way();
}

// Consume all inputs and produce output through the callback function
Status MergeTwoCursor::consume_all(ChunkConsumer output) {
    auto chunk = next();
    while (chunk.ok() && !!chunk.value()) {
        output(std::move(chunk.value()));
        chunk = next();
    }
    return Status::OK();
}

// use this as cursor
std::unique_ptr<SimpleChunkSortCursor> MergeTwoCursor::as_chunk_cursor() {
    if (!_output_cursor) {
        _output_cursor.reset(new SimpleChunkSortCursor(as_provider(), _left_cursor->get_sort_exprs()));
    }
    return std::move(_output_cursor);
}

// 1. Find smaller tail
// 2. Cutoff the chunk based on tail
// 3. Merge two chunks and output
// 4. Move to next
StatusOr<ChunkUniquePtr> MergeTwoCursor::merge_sorted_cursor_two_way() {
    DCHECK(!(_left_run.empty() && _right_run.empty()));
    const SortDescs& sort_desc = _sort_desc;
    ChunkUniquePtr result;

    if (_left_run.empty()) {
        // TODO: avoid copy
        result = _right_run.clone_chunk();
        _right_run.reset();
    } else if (_right_run.empty()) {
        result = _left_run.clone_chunk();
        _left_run.reset();
    } else {
        int tail_cmp = CursorAlgo::compare_tail(sort_desc, _left_run, _right_run);
        if (tail_cmp <= 0) {
            // Cutoff right by left tail
            size_t right_cut =
                    CursorAlgo::cutoff_run(sort_desc, _right_run, std::make_pair(_left_run, _left_run.num_rows() - 1));
            SortedRun right_1(_right_run.chunk, 0, right_cut);
            SortedRun right_2(_right_run.chunk, right_cut, _right_run.num_rows());

            // Merge partial chunk
            Permutation perm;
            RETURN_IF_ERROR(merge_sorted_chunks_two_way(sort_desc, _left_run, right_1, &perm));
            CursorAlgo::trim_permutation(_left_run, right_1, perm);
            DCHECK_EQ(_left_run.num_rows() + right_1.num_rows(), perm.size());
            ChunkUniquePtr merged = _left_run.chunk->clone_empty(perm.size());
            append_by_permutation(merged.get(), {_left_run.chunk, right_1.chunk}, perm);

            _left_run.reset();
            _right_run = right_2;
            result = std::move(merged);
        } else {
            // Cutoff left by right tail
            size_t left_cut =
                    CursorAlgo::cutoff_run(sort_desc, _left_run, std::make_pair(_right_run, _right_run.num_rows() - 1));
            SortedRun left_1(_left_run.chunk, 0, left_cut);
            SortedRun left_2(_left_run.chunk, left_cut, _left_run.num_rows());

            // Merge partial chunk
            Permutation perm;
            RETURN_IF_ERROR(merge_sorted_chunks_two_way(sort_desc, _right_run, left_1, &perm));
            CursorAlgo::trim_permutation(left_1, _right_run, perm);
            DCHECK_EQ(_right_run.num_rows() + left_1.num_rows(), perm.size());
            std::unique_ptr<Chunk> merged = _left_run.chunk->clone_empty(perm.size());
            append_by_permutation(merged.get(), {_right_run.chunk, left_1.chunk}, perm);

            _left_run = left_2;
            _right_run.reset();
            result = std::move(merged);
        }

#ifndef NDEBUG
        for (int i = 0; i < result->num_rows(); i++) {
            fmt::print("merge row: {}\n", result->debug_row(i));
        }
#endif
    }

    // Move cursor
    move_cursor();
    DCHECK(!!result);

    return result;
}

// @return true if data ready, else return false
bool MergeTwoCursor::move_cursor() {
    DCHECK(is_data_ready());
    DCHECK(!is_eos());

    if (_left_run.empty() && !_left_cursor->is_eos()) {
        auto chunk = _left_cursor->try_get_next();
        // TODO: orderby columns
        if (!chunk.first) {
            return false;
        }
        _left_run = SortedRun(ChunkPtr(chunk.first.release()));
    }
    if (_right_run.empty() && !_right_cursor->is_eos()) {
        auto chunk = _right_cursor->try_get_next();
        if (!!chunk.first) {
            return false;
        }
        _right_run = SortedRun(ChunkPtr(chunk.first.release()));
    }

    return true;
}

Status MergeCursorsCascade::init(const SortDescs& sort_desc,
                                 std::vector<std::unique_ptr<SimpleChunkSortCursor>>&& cursors) {
    int level_size = cursors.size();
    _level_cursors.push_back(std::move(cursors));

    while (level_size > 1) {
        std::vector<std::unique_ptr<SimpleChunkSortCursor>> next_level;
        auto& current_level = _level_cursors.back();
        next_level.reserve(current_level.size() / 2);

        for (int i = 0; i < current_level.size(); i += 2) {
            auto& left = current_level[i];
            auto& right = current_level[i + 1];
            _mergers.emplace_back(std::make_unique<MergeTwoCursor>(sort_desc, std::move(left), std::move(right)));
            next_level.push_back(_mergers.back()->as_chunk_cursor());
        }
        if (current_level.size() % 2 == 1) {
            next_level.push_back(std::move(current_level.back()));
        }

        level_size = next_level.size();
        _level_cursors.push_back(std::move(next_level));
    }
    CHECK_EQ(1, _level_cursors.back().size());

    VLOG(2) << "init MergerCursorsCascade";
    return Status::OK();
}

bool MergeCursorsCascade::is_data_ready() {
    for (auto& cursor : _level_cursors.front()) {
        if (!cursor->is_data_ready()) {
            return false;
        }
    }
    return true;
}

bool MergeCursorsCascade::is_eos() {
    return true;
}

ChunkUniquePtr MergeCursorsCascade::try_get_next() {
    /*
    auto& root_cursor = _level_cursors.back()[0];
    if (root_cursor->has_next()) {
        root_cursor->next_chunk_for_pipeline();
        ChunkPtr chunk = root_cursor->get_current_chunk();
        // TODO: avoid copy
        return chunk->clone_unique();
    }
    */
    return {};
}

Status MergeCursorsCascade::consume_all(ChunkConsumer consumer) {
    while (!is_eos()) {
        ChunkUniquePtr chunk = try_get_next();
        if (!!chunk) {
            consumer(std::move(chunk));
        }
    }
    return Status::OK();
}

Status merge_sorted_cursor_two_way(const SortDescs& sort_desc, std::unique_ptr<SimpleChunkSortCursor> left_cursor,
                                   std::unique_ptr<SimpleChunkSortCursor> right_cursor, ChunkConsumer output) {
    MergeTwoCursor merger(sort_desc, std::move(left_cursor), std::move(right_cursor));
    return merger.consume_all(output);
}

Status merge_sorted_cursor_cascade(const SortDescs& sort_desc,
                                   std::vector<std::unique_ptr<SimpleChunkSortCursor>>& cursors,
                                   ChunkConsumer consumer) {
    /*
    MergeCursorsCascade merger;
    RETURN_IF_ERROR(merger.init(sort_desc, cursors));
    merger.consume_all(consumer);
    */
    return Status::OK();
}

} // namespace starrocks::vectorized