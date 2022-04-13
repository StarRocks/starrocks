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
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"
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
    MergeTwoColumn(SortDesc desc, const Column* left_col, const Column* right_col, std::vector<EqualRange>* equal_range,
                   Permutation* perm)
            : ColumnVisitorAdapter(this),
              _sort_order(desc.sort_order),
              _null_first(desc.null_first),
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
                        x *= _sort_order;
                    } else {
                        x = left_col->compare_at(left_range.first, right_range.first, *right_col, _null_first);
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
        while (last < lhs_end && left_col->compare_at(last - 1, last, *left_col, _null_first) == 0) {
            last++;
        }
        return {first, last};
    }

    template <class ColumnType>
    std::pair<size_t, size_t> fetch_right(const ColumnType* right_col, size_t rhs, size_t rhs_end) {
        size_t first = rhs;
        size_t last = rhs + 1;
        while (last < rhs_end && right_col->compare_at(last - 1, last, *right_col, _null_first) == 0) {
            last++;
        }
        return {first, last};
    }

private:
    constexpr static uint32_t kLeftIndex = 0;
    constexpr static uint32_t kRightIndex = 1;

    const int _sort_order;
    const int _null_first;
    const Column* _left_col;
    const Column* _right_col;
    std::vector<EqualRange>* _equal_ranges;
    Permutation* _perm;
};

class MergeTwoChunk {
public:
    static Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const SortedRun& left_run,
                                              const SortedRun& right_run, Permutation* output) {
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
            std::vector<EqualRange> equal_ranges;
            equal_ranges.emplace_back(left_run.range, right_run.range);
            output->resize(left_run.range.second + right_run.range.second);

            for (int col = 0; col < sort_desc.num_columns(); col++) {
                const Column* left_col = left_run.chunk->get_column_by_index(col).get();
                const Column* right_col = right_run.chunk->get_column_by_index(col).get();
                MergeTwoColumn merge2(sort_desc.get_column_desc(col), left_col, right_col, &equal_ranges, output);
                Status st = left_col->accept(&merge2);
                CHECK(st.ok());
            }
        }

        return Status::OK();
    }

private:
};

Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const ChunkPtr left, const ChunkPtr right,
                                   Permutation* output) {
    DCHECK_LE(sort_desc.num_columns(), left->num_columns());
    DCHECK_LE(sort_desc.num_columns(), right->num_columns());

    SortedRun left_run(left, std::make_pair(0, left->num_rows()));
    SortedRun right_run(right, std::make_pair(0, right->num_rows()));
    return MergeTwoChunk::merge_sorted_chunks_two_way(sort_desc, left_run, right_run, output);
}

Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const SortedRun& left, const SortedRun& right,
                                   Permutation* output) {
    return MergeTwoChunk::merge_sorted_chunks_two_way(sort_desc, left, right, output);
}

Status merge_sorted_chunks_two_way_rowwise(const SortDescs& descs, const ChunkPtr left_chunk,
                                           const ChunkPtr right_chunk, Permutation* output, size_t limit) {
    constexpr int kLeftChunkIndex = 0;
    constexpr int kRightChunkIndex = 1;
    size_t index_of_merging = 0, index_of_left = 0, index_of_right = 0;
    size_t left_size = left_chunk->num_rows();
    size_t right_size = right_chunk->num_rows();
    output->reserve(limit);

    while ((index_of_merging < limit) && (index_of_left < left_size) && (index_of_right < right_size)) {
        int cmp = compare_chunk_row(descs, *left_chunk, *right_chunk, index_of_left, index_of_right);
        if (cmp <= 0) {
            output->emplace_back(PermutationItem(kLeftChunkIndex, index_of_left, 0));
            ++index_of_left;
        } else {
            output->emplace_back(PermutationItem(kRightChunkIndex, index_of_right, 0));
            ++index_of_right;
        }
        ++index_of_merging;
    }
    while (index_of_left < left_size && index_of_merging < limit) {
        output->emplace_back(kLeftChunkIndex, index_of_left, 0);
        ++index_of_left;
    }
    while (index_of_right < right_size && index_of_merging < limit) {
        output->emplace_back(kRightChunkIndex, index_of_right, 0);
        ++index_of_right;
    }
    return Status::OK();
}

} // namespace starrocks::vectorized