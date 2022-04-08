// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/datum.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"
#include "runtime/int128_arithmetics_x86_64.h"

namespace starrocks::vectorized {

struct SortedRun {
    const Chunk* chunk;
    std::pair<size_t, size_t> range;

    SortedRun(const Chunk* ichunk, std::pair<size_t, size_t> irange) : chunk(ichunk), range(irange) {}

    size_t num_columns() const { return chunk->num_columns(); }
    size_t num_rows() const { return range.second - range.first; }
};

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
        auto left_col = down_cast<const ColumnType*>(_left_col);
        auto right_col = down_cast<const ColumnType*>(_right_col);

        // Iterate each equal-range
        for (auto equal_range : *_equal_ranges) {
            size_t lhs = equal_range.left_range.first;
            size_t rhs = equal_range.right_range.first;
            size_t lhs_end = equal_range.left_range.second;
            size_t rhs_end = equal_range.right_range.second;

            size_t output_index = lhs + rhs;
            DCHECK_LT(lhs, lhs_end);
            DCHECK_LT(rhs, rhs_end);

            // Merge rows in the equal-range
            while (lhs < lhs_end || rhs < rhs_end) {
                auto left_range = fetch_left(left_col, lhs, lhs_end);
                auto right_range = fetch_right(right_col, rhs, rhs_end);

                // TODO: optimize the compare
                int x = 0;
                if (lhs < lhs_end && rhs < rhs_end) {
                    x = left_col->compare_at(left_range.first, right_range.first, *right_col, 1);
                } else if (lhs < lhs_end) {
                    x = -1;
                } else if (rhs < rhs_end) {
                    x = 1;
                }

                if (x <= 0) {
                    fmt::print("merge left [{}, {}]\n", left_range.first, left_range.second);
                    lhs = left_range.second;
                    for (size_t i = left_range.first; i < left_range.second; i++) {
                        (*_perm)[output_index++] = PermutationItem(kLeftIndex, i);
                    }
                } else if (x >= 0) {
                    fmt::print("merge right [{}, {}]\n", right_range.first, right_range.second);
                    rhs = right_range.second;
                    for (size_t i = right_range.first; i < right_range.second; i++) {
                        (*_perm)[output_index++] = PermutationItem(kRightIndex, i);
                    }
                }

                if (x == 0) {
                    fmt::print("merge equal [{}, {}) + [{}, {})\n", left_range.first, left_range.second,
                               right_range.first, right_range.second);
                    next_ranges.emplace_back(left_range, right_range);
                }
            }

            DCHECK_EQ(lhs, lhs_end);
            DCHECK_EQ(rhs, rhs_end);
        }

        _equal_ranges->swap(next_ranges);
    }

    template <class ColumnType>
    Status do_visit(const ColumnType& column) {
        do_merge<ColumnType>();
        return Status::OK();
    }

    template <class ColumnType>
    std::pair<size_t, size_t> fetch_left(const ColumnType* left_col, size_t lhs, size_t lhs_end) {
        size_t first = lhs;
        size_t last = lhs + 1;
        while (last < lhs_end && left_col->compare_at(last - 1, last, *left_col, 1) == 0) {
            last++;
        }
        return {first, last};
    }

    template <class ColumnType>
    std::pair<size_t, size_t> fetch_right(const ColumnType* right_col, size_t rhs, size_t rhs_end) {
        size_t first = rhs;
        size_t last = rhs + 1;
        while (last < rhs_end && right_col->compare_at(last - 1, last, *right_col, 1) == 0) {
            last++;
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
class MergeSortedTwoWay {
public:
    MergeSortedTwoWay(const SortedRun& left, const SortedRun& right, Permutation* perm)
            : _left(left), _right(right), _perm(perm) {}

    void run() {
        // TODO: optimize with tie
        std::vector<EqualRange> equal_ranges;
        _perm->resize(_left.num_rows() + _right.num_rows());

        // The first column
        equal_ranges.emplace_back(_left.range, _right.range);

        // Iterate each column
        for (int col = 0; col < _left.num_columns(); col++) {
            const Column* left_col = _left.chunk->get_column_by_index(col).get();
            const Column* right_col = _right.chunk->get_column_by_index(col).get();
            MergeTwoColumn merge2(left_col, right_col, &equal_ranges, _perm);
            Status st = left_col->accept(&merge2);
            CHECK(st.ok());
        }
    }

private:
    const SortedRun& _left;
    const SortedRun& _right;
    Permutation* _perm;
};

Status merge_sorted_chunks_two_way(const Chunk* left, const Chunk* right, Permutation* output) {
    DCHECK_EQ(left->num_columns(), right->num_columns());

    SortedRun left_run(left, std::make_pair(0, left->num_rows()));
    SortedRun right_run(right, std::make_pair(0, right->num_rows()));
    MergeSortedTwoWay merger(left_run, right_run, output);
    merger.run();
    return Status::OK();
}

Status merge_sorted_chunks_heap_based(const std::vector<SortedRun>& runs, Chunk* output) {
    return Status::NotSupported("TODO");
}

Status merge_sorted_chunks(const std::vector<ChunkPtr>& chunks, Chunk* output) {
    if (chunks.empty()) {
        return Status::OK();
    }

    std::vector<SortedRun> runs;
    for (auto& chunk : chunks) {
        runs.emplace_back(chunk.get(), std::make_pair(0, chunk->num_rows()));
    }

    int num_columns = chunks[0]->num_columns();
    for (int i = 0; i < num_columns; i++) {
    }

    return Status::OK();
}
} // namespace starrocks::vectorized