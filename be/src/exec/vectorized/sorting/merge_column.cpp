// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/datum.h"
#include "column/fixed_length_column_base.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"
#include "runtime/vectorized/chunk_cursor.h"

namespace starrocks::vectorized {

struct EqualRange {
    using Range = std::pair<uint32_t, uint32_t>;

    Range left_range;
    Range right_range;

    EqualRange(Range left, Range right) : left_range(left), right_range(right) {}
};

// MergeTwoColumn incremental merge two columns
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

    template <class Cmp, class LeftEqual, class RightEqual>
    Status do_merge(Cmp cmp, LeftEqual equal_left, RightEqual equal_right) {
        std::vector<EqualRange> next_ranges;
        next_ranges.reserve(_equal_ranges->size());

        // Iterate each equal-range
        for (auto equal_range : *_equal_ranges) {
            size_t lhs = equal_range.left_range.first;
            size_t rhs = equal_range.right_range.first;
            size_t lhs_end = equal_range.left_range.second;
            size_t rhs_end = equal_range.right_range.second;
            size_t output_index = lhs + rhs;

            // Merge rows in the equal-range
            auto left_range = fetch_equal(lhs, lhs_end, equal_left);
            auto right_range = fetch_equal(rhs, rhs_end, equal_right);
            while (lhs < lhs_end || rhs < rhs_end) {
                int x = 0;
                if (lhs < lhs_end && rhs < rhs_end) {
                    x = cmp(left_range.first, right_range.first);
                } else if (lhs < lhs_end) {
                    x = -1;
                } else if (rhs < rhs_end) {
                    x = 1;
                }

                if (x == 0) {
                    next_ranges.emplace_back(left_range, right_range);
                }
                if (x <= 0) {
                    for (size_t i = left_range.first; i < left_range.second; i++) {
                        (*_perm)[output_index++] = PermutationItem(kLeftIndex, i);
                    }
                    lhs = left_range.second;
                    left_range = fetch_equal(lhs, lhs_end, equal_left);
                }
                if (x >= 0) {
                    for (size_t i = right_range.first; i < right_range.second; i++) {
                        (*_perm)[output_index++] = PermutationItem(kRightIndex, i);
                    }
                    rhs = right_range.second;
                    right_range = fetch_equal(rhs, rhs_end, equal_right);
                }
            }

            DCHECK_EQ(lhs, lhs_end);
            DCHECK_EQ(rhs, rhs_end);
        }

        _equal_ranges->swap(next_ranges);
        return Status::OK();
    }

    template <class Equal>
    EqualRange::Range fetch_equal(size_t lhs, size_t lhs_end, Equal equal) {
        uint32_t first = lhs;
        uint32_t last = lhs + 1;
        while (last < lhs_end && equal(last - 1, last)) {
            last++;
        }
        return {first, last};
    }

    // General implementation
    template <class ColumnType>
    Status do_visit(const ColumnType&) {
        auto cmp = [&](size_t lhs_index, size_t rhs_index) {
            int x = _left_col->compare_at(lhs_index, rhs_index, *_right_col, _null_first);
            if (_sort_order == -1) {
                x *= -1;
            }
            return x;
        };
        auto equal_left = [&](size_t lhs_index, size_t rhs_index) {
            return _left_col->compare_at(lhs_index, rhs_index, *_left_col, _null_first) == 0;
        };
        auto equal_right = [&](size_t lhs_index, size_t rhs_index) {
            return _right_col->compare_at(lhs_index, rhs_index, *_right_col, _null_first) == 0;
        };
        do_merge(cmp, equal_left, equal_right);
        return Status::OK();
    }

    template <class Container, class ValueType>
    Status merge_ordinary_column(const Container& left_data, const Container& right_data) {
        auto cmp = [&](size_t lhs_index, size_t rhs_index) {
            int x = SorterComparator<ValueType>::compare(left_data[lhs_index], right_data[rhs_index]);
            if (_sort_order == -1) {
                x *= -1;
            }
            return x;
        };
        auto cmp_left = [&](size_t lhs_index, size_t rhs_index) {
            return left_data[lhs_index] == left_data[rhs_index];
        };
        auto cmp_right = [&](size_t lhs_index, size_t rhs_index) {
            return right_data[lhs_index] == right_data[rhs_index];
        };
        return do_merge(cmp, cmp_left, cmp_right);
    }

    // Specific version for FixedlengthColumn
    template <class T>
    Status do_visit(const FixedLengthColumn<T>& _) {
        using ColumnType = const FixedLengthColumn<T>;
        using Container = typename ColumnType::Container;
        auto& left_data = down_cast<ColumnType*>(_left_col)->get_data();
        auto& right_data = down_cast<ColumnType*>(_right_col)->get_data();
        return merge_ordinary_column<Container, T>(left_data, right_data);
    }

    template <typename SizeT>
    Status do_visit(const BinaryColumnBase<SizeT>& _) {
        using ColumnType = const BinaryColumnBase<SizeT>;
        using Container = typename ColumnType::Container;
        auto& left_data = down_cast<ColumnType*>(_left_col)->get_data();
        auto& right_data = down_cast<ColumnType*>(_right_col)->get_data();
        return merge_ordinary_column<Container, Slice>(left_data, right_data);
    }

    // TODO: Murphy
    // Status do_visit(const NullableColumn& _) {
    // return Status::NotSupported("TODO");
    // }

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

// MergeTwoChunk merge two chunk in column-wise
// 1. Merge the first column, record the equal rows into equal-range
// 2. Merge the second column within the equal-range of previous column
// 3. Repeat it until no equal-range or the last column
class MergeTwoChunk {
public:
    static constexpr int kLeftChunkIndex = 0;
    static constexpr int kRightChunkIndex = 1;

    static Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const SortedRun& left_run,
                                              const SortedRun& right_run, Permutation* output) {
        DCHECK(!!left_run.chunk);
        DCHECK(!!right_run.chunk);
        DCHECK_EQ(left_run.num_columns(), right_run.num_columns());

        if (left_run.empty()) {
            size_t count = right_run.num_rows();
            output->resize(count);
            for (size_t i = 0; i < count; i++) {
                (*output)[i].chunk_index = kRightChunkIndex;
                (*output)[i].index_in_chunk = i + right_run.range.first;
            }
        } else if (right_run.empty()) {
            size_t count = left_run.num_rows();
            output->resize(count);
            for (size_t i = 0; i < count; i++) {
                (*output)[i].chunk_index = kLeftChunkIndex;
                (*output)[i].index_in_chunk = i + left_run.range.first;
            }
        } else {
            int intersect = run_intersect(sort_desc, left_run, right_run);
            if (intersect != 0) {
                size_t left_rows = left_run.num_rows();
                size_t right_rows = right_run.num_rows();
                output->resize(0);
                output->reserve(left_rows + right_rows);

                if (intersect < 0) {
                    // TODO: avoid copy chunk if two run have no intersection
                    for (size_t i = 0; i < left_rows; i++) {
                        output->emplace_back(kLeftChunkIndex, i + left_run.range.first);
                    }
                    for (size_t i = 0; i < right_rows; i++) {
                        output->emplace_back(kRightChunkIndex, i + right_run.range.first);
                    }
                } else {
                    for (size_t i = 0; i < right_rows; i++) {
                        output->emplace_back(kRightChunkIndex, i + right_run.range.first);
                    }
                    for (size_t i = 0; i < left_rows; i++) {
                        output->emplace_back(kLeftChunkIndex, i + left_run.range.first);
                    }
                }
            } else {
                std::vector<EqualRange> equal_ranges;
                equal_ranges.emplace_back(left_run.range, right_run.range);
                size_t count = left_run.range.second + right_run.range.second;
                output->resize(count);
                equal_ranges.reserve(std::max((size_t)1, count / 4));

                for (int col = 0; col < sort_desc.num_columns(); col++) {
                    const Column* left_col = left_run.get_column(col);
                    const Column* right_col = right_run.get_column(col);
                    MergeTwoColumn merge2(sort_desc.get_column_desc(col), left_col, right_col, &equal_ranges, output);
                    Status st = left_col->accept(&merge2);
                    CHECK(st.ok());
                    if (equal_ranges.size() == 0) {
                        break;
                    }
                }
            }
        }

        return Status::OK();
    }

private:
    // Check if two run has intersect, if not we don't need to merge them row by row
    // @return 0 if two run has intersect, -1 if left is less than right, 1 if left is greater than right
    static int run_intersect(const SortDescs& sort_desc, const SortedRun& left_run, const SortedRun& right_run) {
        // Compare left tail with right head
        int left_tail_cmp = compare_chunk_row(sort_desc, left_run.orderby, right_run.orderby, left_run.range.second - 1,
                                              right_run.range.first);
        if (left_tail_cmp < 0) {
            return -1;
        }

        // Compare left head with right tail
        int left_head_cmp = compare_chunk_row(sort_desc, left_run.orderby, right_run.orderby, left_run.range.first,
                                              right_run.range.second - 1);
        if (left_head_cmp > 0) {
            return 1;
        }
        return 0;
    }
};

Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const ChunkPtr left, const ChunkPtr right,
                                   Permutation* output) {
    DCHECK_LE(sort_desc.num_columns(), left->num_columns());
    DCHECK_LE(sort_desc.num_columns(), right->num_columns());

    SortedRun left_run(left, 0, left->num_rows());
    SortedRun right_run(right, 0, right->num_rows());
    return MergeTwoChunk::merge_sorted_chunks_two_way(sort_desc, left_run, right_run, output);
}

Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const SortedRun& left, const SortedRun& right,
                                   Permutation* output) {
    return MergeTwoChunk::merge_sorted_chunks_two_way(sort_desc, left, right, output);
}

Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const SortedRuns& left, const SortedRuns& right,
                                   SortedRuns* output) {
    CHECK(false) << "TODO";
    return Status::NotSupported("TODO");
}

// Merge multiple chunks in two-way merge
Status merge_sorted_chunks(const SortDescs& descs, const std::vector<ChunkPtr>& chunks, ChunkPtr* output) {
    std::vector<ChunkPtr> current(chunks);
    while (current.size() > 1) {
        std::vector<ChunkPtr> next_level;
        int level_size = current.size() & ~1;

        for (int i = 0; i < level_size; i += 2) {
            Permutation perm;
            ChunkPtr left = current[i];
            ChunkPtr right = current[i + 1];
            RETURN_IF_ERROR(merge_sorted_chunks_two_way(descs, left, right, &perm));

            ChunkPtr merged = left->clone_empty(left->num_rows() + right->num_rows());
            append_by_permutation(merged.get(), {left, right}, perm);
            next_level.push_back(merged);
        }
        if (current.size() % 2 == 1) {
            next_level.push_back(current.back());
        }
        current = std::move(next_level);
    }
    *output = current.front();

    return Status::OK();
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
        int cmp =
                compare_chunk_row(descs, left_chunk->columns(), right_chunk->columns(), index_of_left, index_of_right);
        if (cmp <= 0) {
            output->emplace_back(PermutationItem(kLeftChunkIndex, index_of_left));
            ++index_of_left;
        } else {
            output->emplace_back(PermutationItem(kRightChunkIndex, index_of_right));
            ++index_of_right;
        }
        ++index_of_merging;
    }
    while (index_of_left < left_size && index_of_merging < limit) {
        output->emplace_back(kLeftChunkIndex, index_of_left);
        ++index_of_left;
    }
    while (index_of_right < right_size && index_of_merging < limit) {
        output->emplace_back(kRightChunkIndex, index_of_right);
        ++index_of_right;
    }
    return Status::OK();
}

} // namespace starrocks::vectorized