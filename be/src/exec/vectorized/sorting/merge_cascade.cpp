// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized/sorting/merge.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sorting.h"
#include "runtime/chunk_cursor.h"

namespace starrocks::vectorized {

// Some search algorithms with cursor
struct CursorAlgo {
    static int compare_tail(const SortDescs& desc, const SortedRun& left, const SortedRun& right) {
        size_t lhs_tail = left.range.second - 1;
        size_t rhs_tail = right.range.second - 1;
        return left.compare_row(desc, right, lhs_tail, rhs_tail);
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
            int x = column.compare_at(mid, rhs_row, rhs_column, desc.null_first) * desc.sort_order;
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
        std::pair<size_t, size_t> search_range = run.range;
        size_t res = 0;

        for (int i = 0; i < sort_descs.num_columns(); i++) {
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
        if (output == nullptr || eos == nullptr) {
            return is_data_ready();
        }
        auto chunk = next();
        *eos = is_eos();
        if (!chunk.ok() || !chunk.value()) {
            return false;
        } else {
            *output = chunk.value().release();
            return true;
        }
    };
}

// Consume all inputs and produce output through the callback function
Status MergeTwoCursor::consume_all(ChunkConsumer output) {
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
    return _left_cursor->is_eos() && _right_cursor->is_eos();
}

StatusOr<ChunkUniquePtr> MergeTwoCursor::next() {
    if (!is_data_ready() || is_eos()) {
        return ChunkUniquePtr();
    }
    if (!move_cursor()) {
        return ChunkUniquePtr();
    }
    return merge_sorted_cursor_two_way();
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
        result = _right_run.clone_slice();
        _right_run.reset();
    } else if (_right_run.empty()) {
        result = _left_run.clone_slice();
        _left_run.reset();
    } else {
        int tail_cmp = CursorAlgo::compare_tail(sort_desc, _left_run, _right_run);
        if (tail_cmp <= 0) {
            // Cutoff right by left tail
            size_t right_cut =
                    CursorAlgo::cutoff_run(sort_desc, _right_run, std::make_pair(_left_run, _left_run.num_rows() - 1));
            SortedRun right_1(_right_run, 0, right_cut);
            SortedRun right_2(_right_run, right_cut, _right_run.num_rows());

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
            SortedRun left_1(_left_run, 0, left_cut);
            SortedRun left_2(_left_run, left_cut, _left_run.num_rows());

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

    return result;
}

// @return true if data ready, else return false
bool MergeTwoCursor::move_cursor() {
    DCHECK(is_data_ready());
    DCHECK(!is_eos());

    if (_left_run.empty() && !_left_cursor->is_eos()) {
        auto chunk = _left_cursor->try_get_next();
        if (!chunk.first) {
            return false;
        }
        _left_run = SortedRun(ChunkPtr(chunk.first.release()), chunk.second);
    }
    if (_right_run.empty() && !_right_cursor->is_eos()) {
        auto chunk = _right_cursor->try_get_next();
        if (!chunk.first) {
            return false;
        }
        _right_run = SortedRun(ChunkPtr(chunk.first.release()), chunk.second);
    }

    return true;
}

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
                                   std::vector<std::unique_ptr<SimpleChunkSortCursor>>&& cursors,
                                   ChunkConsumer consumer) {
    MergeCursorsCascade merger;
    RETURN_IF_ERROR(merger.init(sort_desc, std::move(cursors)));
    CHECK(merger.is_data_ready());
    merger.consume_all(consumer);
    return Status::OK();
}

} // namespace starrocks::vectorized