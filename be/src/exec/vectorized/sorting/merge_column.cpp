// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/datum.h"
#include "column/fixed_length_column_base.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized/join_hash_map.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"
#include "runtime/int128_arithmetics_x86_64.h"
#include "runtime/vectorized/chunk_cursor.h"

namespace starrocks::vectorized {

struct EqualRange {
    using Range = std::pair<size_t, size_t>;

    Range left_range;
    Range right_range;

    EqualRange(Range left, Range right) : left_range(left), right_range(right) {}
};

class MergeTwoColumn final : public ColumnVisitorAdapter<MergeTwoColumn> {
public:
    MergeTwoColumn(const Column* left_col, const Column* right_col, std::vector<EqualRange>* equal_range,
                   Permutation* perm)
            : ColumnVisitorAdapter(this),
              _left_col(left_col),
              _right_col(right_col),
              _equal_ranges(equal_range),
              _perm(perm) {}

    template <class ColumnType>
    void do_merge() {
        std::vector<EqualRange> next_ranges;
        next_ranges.reserve(_equal_ranges->size());
        auto left_col = down_cast<const ColumnType*>(_left_col);
        auto right_col = down_cast<const ColumnType*>(_right_col);

        // Iterate each equal-range
        for (auto equal_range : *_equal_ranges) {
            size_t lhs = equal_range.left_range.first;
            size_t rhs = equal_range.right_range.first;
            size_t lhs_end = equal_range.left_range.second;
            size_t rhs_end = equal_range.right_range.second;
            size_t output_index = lhs + rhs;

            // Merge rows in the equal-range
            while (lhs < lhs_end || rhs < rhs_end) {
                auto left_range = fetch_left(left_col, lhs, lhs_end);
                auto right_range = fetch_right(right_col, rhs, rhs_end);

                // TODO: optimize the compare
                int x = 0;
                if (lhs < lhs_end && rhs < rhs_end) {
                    // TODO: avoid specialization
                    if constexpr (std::is_same_v<FixedLengthColumn<int32_t>, ColumnType> ||
                                  std::is_same_v<FixedLengthColumnBase<int32_t>, ColumnType>) {
                        auto& left_data = left_col->get_data();
                        auto& right_data = right_col->get_data();
                        x = SorterComparator<int32_t>::compare(left_data[left_range.first],
                                                               right_data[right_range.first]);
                    } else {
                        x = left_col->compare_at(left_range.first, right_range.first, *right_col, 1);
                    }
                } else if (lhs < lhs_end) {
                    x = -1;
                } else if (rhs < rhs_end) {
                    x = 1;
                }

                if (x <= 0) {
#ifndef NDEBUG
                    fmt::print("merge left [{}, {}]\n", left_range.first, left_range.second);
#endif
                    lhs = left_range.second;
                    for (size_t i = left_range.first; i < left_range.second; i++) {
                        (*_perm)[output_index++] = PermutationItem(kLeftIndex, i);
                    }
                }
                if (x >= 0) {
#ifndef NDEBUG
                    fmt::print("merge right [{}, {}]\n", right_range.first, right_range.second);
#endif
                    rhs = right_range.second;
                    for (size_t i = right_range.first; i < right_range.second; i++) {
                        (*_perm)[output_index++] = PermutationItem(kRightIndex, i);
                    }
                }

                if (x == 0) {
#ifndef NDEBUG
                    fmt::print("merge equal [{}, {}) + [{}, {})\n", left_range.first, left_range.second,
                               right_range.first, right_range.second);
#endif
                    next_ranges.emplace_back(left_range, right_range);
                }
            }

            DCHECK_EQ(lhs, lhs_end);
            DCHECK_EQ(rhs, rhs_end);
        }

        _equal_ranges->swap(next_ranges);
    }

    template <class ColumnType>
    Status do_visit(const ColumnType&) {
        do_merge<ColumnType>();
        return Status::OK();
    }

    template <class ColumnType>
    std::pair<size_t, size_t> fetch_left(const ColumnType* left_col, size_t lhs, size_t lhs_end) {
        size_t first = lhs;
        size_t last = lhs + 1;
        // TODO: avoid specialization
        if constexpr (std::is_same_v<FixedLengthColumnBase<int32_t>, ColumnType> ||
                      std::is_same_v<FixedLengthColumn<int32_t>, ColumnType>) {
            auto& left_data = left_col->get_data();
            while (last < lhs_end && left_data[last - 1] == left_data[last]) {
                last++;
            }
        } else {
            while (last < lhs_end && left_col->compare_at(last - 1, last, *left_col, 1) == 0) {
                last++;
            }
        }
        return {first, last};
    }

    template <class ColumnType>
    std::pair<size_t, size_t> fetch_right(const ColumnType* right_col, size_t rhs, size_t rhs_end) {
        size_t first = rhs;
        size_t last = rhs + 1;
        // TODO: avoid specialization
        if constexpr (std::is_same_v<FixedLengthColumnBase<int32_t>, ColumnType> ||
                      std::is_same_v<FixedLengthColumn<int32_t>, ColumnType>) {
            auto& left_data = right_col->get_data();
            while (last < rhs_end && left_data[last - 1] == left_data[last]) {
                last++;
            }
        } else {
            while (last < rhs_end && right_col->compare_at(last - 1, last, *right_col, 1) == 0) {
                last++;
            }
        }
        return {first, last};
    }

private:
    constexpr static uint32_t kLeftIndex = 0;
    constexpr static uint32_t kRightIndex = 1;

    const Column* _left_col;
    const Column* _right_col;
    std::vector<EqualRange>* _equal_ranges;
    Permutation* _perm;
};

// Merge two-way sorted run into on sorted run
// TODO: specify the ordering
Status merge_sorted_chunks_two_way(const ChunkPtr left, const ChunkPtr right, Permutation* output) {
    DCHECK_EQ(left->num_columns(), right->num_columns());

    SortedRun left_run(left, std::make_pair(0, left->num_rows()));
    SortedRun right_run(right, std::make_pair(0, right->num_rows()));
    return merge_sorted_chunks_two_way(left_run, right_run, output);
}

Status merge_sorted_chunks_two_way(const SortedRun& left_run, const SortedRun& right_run, Permutation* output) {
    DCHECK(!!left_run.chunk);
    DCHECK(!!right_run.chunk);
    DCHECK_EQ(left_run.chunk->num_columns(), right_run.chunk->num_columns());

    if (left_run.empty()) {
        size_t count = right_run.range.second - right_run.range.first;
        output->resize(count);
        for (size_t i = 0; i < count; i++) {
            (*output)[i].chunk_index = 1;
            (*output)[i].index_in_chunk = i + right_run.range.first;
        }
    } else if (right_run.empty()) {
        size_t count = left_run.range.second - left_run.range.first;
        output->resize(count);
        for (size_t i = 0; i < count; i++) {
            (*output)[i].chunk_index = 0;
            (*output)[i].index_in_chunk = i + left_run.range.first;
        }
    } else {
        // TODO: optimize with tie
        // The first column
        std::vector<EqualRange> equal_ranges;
        equal_ranges.emplace_back(left_run.range, right_run.range);

        output->resize(left_run.range.second + right_run.range.second);
        // Iterate each column
        for (int col = 0; col < left_run.num_columns(); col++) {
            const Column* left_col = left_run.chunk->get_column_by_index(col).get();
            const Column* right_col = right_run.chunk->get_column_by_index(col).get();
            MergeTwoColumn merge2(left_col, right_col, &equal_ranges, output);
            Status st = left_col->accept(&merge2);
            CHECK(st.ok());
        }
    }

    return Status::OK();
}

// Merge two sorted chunk cusor
class MergeTwoCursor {
public:
    static Status merge_sorted_cursor_two_way(ChunkCursor& left_cursor, ChunkCursor& right_cursor,
                                              ChunkConsumer output) {
        // 1. Find smaller tail
        // 2. Cutoff the chunk based on tail
        // 3. Merge two chunks and output
        // 4. Move to next

        SortedRun left_chunk;
        SortedRun right_chunk;

        left_cursor.next_chunk_for_pipeline();
        right_cursor.next_chunk_for_pipeline();
        if (left_cursor.has_next()) {
            left_chunk = SortedRun(left_cursor.get_current_chunk());
        }
        if (right_cursor.has_next()) {
            right_chunk = SortedRun(right_cursor.get_current_chunk());
        }

        while (!left_chunk.empty() || !right_chunk.empty()) {
            if (left_chunk.empty()) {
                // TODO: avoid copy
                if (right_chunk.num_rows() == right_chunk.chunk->num_rows()) {
                    RETURN_IF_ERROR(output(right_chunk.chunk->clone_unique().release()));
                } else {
                    RETURN_IF_ERROR(output(right_chunk.clone_chunk().release()));
                }
                right_chunk.reset();
            } else if (right_chunk.empty()) {
                if (left_chunk.num_rows() == left_chunk.chunk->num_rows()) {
                    RETURN_IF_ERROR(output(left_chunk.chunk->clone_unique().release()));
                } else {
                    RETURN_IF_ERROR(output(left_chunk.clone_chunk().release()));
                }
                left_chunk.reset();
            } else {
                int tail_cmp = compare_tail(left_chunk, right_chunk);
                if (tail_cmp <= 0) {
                    // Cutoff right by left tail
                    size_t right_cut = cutoff_run(right_chunk, std::make_pair(left_chunk, left_chunk.num_rows() - 1));
                    SortedRun right_1(right_chunk.chunk, 0, right_cut);
                    SortedRun right_2(right_chunk.chunk, right_cut, right_chunk.num_rows());

                    // Merge partial chunk
                    Permutation perm;
                    RETURN_IF_ERROR(merge_sorted_chunks_two_way(left_chunk, right_1, &perm));
                    trim_permutation(left_chunk, right_1, perm);
                    DCHECK_EQ(left_chunk.num_rows() + right_1.num_rows(), perm.size());
                    std::unique_ptr<Chunk> merged = left_chunk.chunk->clone_empty(perm.size());
                    append_by_permutation(merged.get(), {left_chunk.chunk, right_1.chunk}, perm);


                    left_chunk.reset();
                    right_chunk = right_2;
#ifndef NDEBUG
                    fmt::print("merge right chunk [0, {})\n", right_cut);
                    for (int i = 0; i < merged->num_rows(); i++) {
                        fmt::print("merge row: {}\n", merged->debug_row(i));
                    }
#endif

                    // Output
                    RETURN_IF_ERROR(output(merged.release()));
                } else {
                    // Cutoff left by right tail
                    size_t left_cut = cutoff_run(left_chunk, std::make_pair(right_chunk, right_chunk.num_rows() - 1));
                    SortedRun left_1(left_chunk.chunk, 0, left_cut);
                    SortedRun left_2(left_chunk.chunk, left_cut, left_chunk.num_rows());

                    // Merge partial chunk
                    Permutation perm;
                    RETURN_IF_ERROR(merge_sorted_chunks_two_way(right_chunk, left_1, &perm));
                    trim_permutation(left_1, right_chunk, perm);
                    DCHECK_EQ(right_chunk.num_rows() + left_1.num_rows(), perm.size());
                    std::unique_ptr<Chunk> merged = left_chunk.chunk->clone_empty(perm.size());
                    append_by_permutation(merged.get(), {right_chunk.chunk, left_1.chunk}, perm);


                    left_chunk = left_2;
                    right_chunk.reset();
#ifndef NDEBUG
                    fmt::print("merge left chunk [0, {})\n", left_cut);
                    for (int i = 0; i < merged->num_rows(); i++) {
                        fmt::print("merge row: {}\n", merged->debug_row(i));
                    }
#endif

                    // Output
                    RETURN_IF_ERROR(output(merged.release()));
                }
            }

            if (left_chunk.empty()) {
                left_cursor.next_chunk_for_pipeline();
                if (left_cursor.has_next()) {
                    left_chunk = SortedRun(left_cursor.get_current_chunk());
                }
            }
            if (right_chunk.empty()) {
                right_cursor.next_chunk_for_pipeline();
                if (right_cursor.has_next()) {
                    right_chunk = SortedRun(right_cursor.get_current_chunk());
                }
            }
        }
        return Status::OK();
    }

    static void trim_permutation(SortedRun left, SortedRun right, Permutation& perm) {
        size_t start = left.range.first + right.range.first;
        size_t end = left.range.second + right.range.second;
        std::copy(perm.begin() + start, perm.end() + end, perm.begin());
        perm.resize(end - start);
    }

    // Cutoff by upper_bound
    // @return last row index of upper bound
    static size_t cutoff_run(SortedRun run, std::pair<SortedRun, size_t> cut) {
        size_t res = 0;
        std::pair<size_t, size_t> search_range = run.range;

        for (int i = 0; i < run.num_columns(); i++) {
            auto& lhs_col = *run.get_column(i);
            auto& rhs_col = *cut.first.get_column(i);
            size_t lower = lower_bound(lhs_col, search_range, rhs_col, cut.second);
            size_t upper = upper_bound(lhs_col, search_range, rhs_col, cut.second);
            res = upper;
            if (upper - lower <= 1) {
                break;
            }
            search_range = {lower, upper};
        }
        return res;
    }

    // Find upper_bound in left column based on right row
    static size_t upper_bound(const Column& column, std::pair<size_t, size_t> range, const Column& rhs_column,
                              size_t rhs_row) {
        size_t first = range.first;
        size_t count = range.second - range.first;
        while (count > 0) {
            size_t mid = first + count / 2;
            if (column.compare_at(mid, rhs_row, rhs_column, -1) <= 0) {
                first = mid + 1;
                count -= count / 2 + 1;
            } else {
                count /= 2;
            }
        }
        return first;
    }

    static size_t lower_bound(const Column& column, std::pair<size_t, size_t> range, const Column& rhs_column,
                              size_t rhs_row) {
        size_t first = range.first;
        size_t count = range.second - range.first;
        while (count > 0) {
            size_t mid = first + count / 2;
            if (column.compare_at(mid, rhs_row, rhs_column, -1) < 0) {
                first = mid + 1;
                count -= count / 2 + 1;
            } else {
                count /= 2;
            }
        }
        return first;
    }

    static int compare_tail(const SortedRun& left, const SortedRun& right) {
        size_t lhs_tail = left.num_rows() - 1;
        size_t rhs_tail = right.num_rows() - 1;
        return compare_chunk_row(*(left.chunk), *(right.chunk), lhs_tail, rhs_tail);
    }
};

Status merge_sorted_cursor_two_way(ChunkCursor& left_cursor, ChunkCursor& right_cursor, ChunkConsumer output) {
    return MergeTwoCursor::merge_sorted_cursor_two_way(left_cursor, right_cursor, output);
}

Status merge_sorted_chunks_heap_based(const std::vector<SortedRun>& runs, Chunk* output) {
    return Status::NotSupported("TODO");
}

Status merge_sorted_chunks(const std::vector<ChunkPtr>& chunks, Chunk* output) {
    if (chunks.empty()) {
        return Status::OK();
    }

    std::vector<SortedRun> runs;
    for (auto chunk : chunks) {
        runs.push_back(SortedRun(chunk));
    }

    int num_columns = chunks[0]->num_columns();
    for (int i = 0; i < num_columns; i++) {
    }

    return Status::OK();
}
} // namespace starrocks::vectorized