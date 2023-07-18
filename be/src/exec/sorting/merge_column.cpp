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

#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/datum.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "runtime/chunk_cursor.h"

namespace starrocks {

struct EqualRange {
    using Range = std::pair<uint32_t, uint32_t>;

    Range left_range;
    Range right_range;

    EqualRange(Range left, Range right) : left_range(std::move(left)), right_range(std::move(right)) {}
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
    Status do_visit_slow(const ColumnType&) {
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
        RETURN_IF_ERROR(do_merge(cmp, equal_left, equal_right));
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

    template <class ColumnType>
    Status do_visit(const ColumnType& _) {
        return do_visit_slow(_);
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
        using Container = typename BinaryColumnBase<SizeT>::BinaryDataProxyContainer;
        auto& left_data = down_cast<const ColumnType*>(_left_col)->get_proxy_data();
        auto& right_data = down_cast<const ColumnType*>(_right_col)->get_proxy_data();
        return merge_ordinary_column<Container, Slice>(left_data, right_data);
    }

    Status do_visit(const NullableColumn& _) {
        // Fast path
        if (!_left_col->has_null() && !_right_col->has_null()) {
            DCHECK(_left_col->is_nullable() && _right_col->is_nullable());
            const auto* lhs_data = down_cast<const NullableColumn*>(_left_col)->data_column().get();
            const auto* rhs_data = down_cast<const NullableColumn*>(_right_col)->data_column().get();
            MergeTwoColumn merge2({_sort_order, _null_first}, lhs_data, rhs_data, _equal_ranges, _perm);
            return lhs_data->accept(&merge2);
        }

        // Slow path
        return do_visit_slow(_);
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
            int intersect = left_run.intersect(sort_desc, right_run);
            if (intersect != 0) {
                size_t left_rows = left_run.num_rows();
                size_t right_rows = right_run.num_rows();
                DCHECK_LT(left_rows + right_rows, Column::MAX_CAPACITY_LIMIT);
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
                DCHECK_LT(count, Column::MAX_CAPACITY_LIMIT);
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
};

SortedRun::SortedRun(const ChunkPtr& ichunk, const std::vector<ExprContext*>* exprs)
        : chunk(ichunk), range(0, ichunk->num_rows()) {
    DCHECK(ichunk);
    if (!ichunk->is_empty()) {
        for (auto& expr : *exprs) {
            auto column = EVALUATE_NULL_IF_ERROR(expr, expr->root(), ichunk.get());
            orderby.push_back(column);
        }
    }
}

void SortedRun::reset() {
    chunk.reset();
    orderby.clear();
    range = {};
}

void SortedRun::resize(size_t size) {
    if (num_rows() <= size) {
        return;
    }
    // Only resize the range but not clone chunk
    range.second = range.first + (uint32_t)size;
}

int SortedRun::intersect(const SortDescs& sort_desc, const SortedRun& right_run) const {
    if (empty()) {
        return 1;
    }
    if (right_run.empty()) {
        return -1;
    }
    // Compare left tail with right head
    int left_tail_cmp = compare_row(sort_desc, right_run, end_index() - 1, right_run.start_index());
    if (left_tail_cmp < 0) {
        return -1;
    }

    // Compare left head with right tail
    int left_head_cmp = compare_row(sort_desc, right_run, start_index(), right_run.end_index() - 1);
    if (left_head_cmp > 0) {
        return 1;
    }
    return 0;
}

ChunkUniquePtr SortedRun::clone_slice() const {
    if (range.first == 0 && range.second == chunk->num_rows()) {
        return chunk->clone_unique();
    } else {
        size_t slice_rows = num_rows();
        DCHECK_LT(slice_rows, Column::MAX_CAPACITY_LIMIT);
        ChunkUniquePtr cloned = chunk->clone_empty(slice_rows);
        cloned->append(*chunk, range.first, slice_rows);
        return cloned;
    }
}

std::pair<ChunkPtr, Columns> SortedRun::steal(bool steal_orderby, size_t size, size_t skipped_rows) {
    if (empty()) {
        return {};
    }
    if (skipped_rows >= num_rows()) {
        // all data should be skipped
        chunk.reset();
        range.first = range.second = 0;
        return {};
    }

    size_t reserved_rows = num_rows() - skipped_rows;

    if (size >= reserved_rows) {
        ChunkPtr res_chunk;
        Columns res_orderby;
        if (skipped_rows == 0 && range.first == 0 && range.second == chunk->num_rows()) {
            // No others reference this chunk
            res_chunk = chunk;

            if (steal_orderby) {
                res_orderby = std::move(orderby);
            }
        } else {
            res_chunk = chunk->clone_empty(reserved_rows);
            res_chunk->append(*chunk, range.first + skipped_rows, reserved_rows);

            if (steal_orderby) {
                for (auto& column : orderby) {
                    auto copy = column->clone_empty();
                    copy->reserve(reserved_rows);
                    copy->append(*column, range.first + skipped_rows, reserved_rows);
                    res_orderby.push_back(std::move(copy));
                }
            }
        }
        range.first = range.second = 0;
        chunk.reset();
        orderby.clear();
        return std::make_pair(std::move(res_chunk), std::move(res_orderby));
    } else {
        size_t required_rows = std::min(size, reserved_rows);
        ChunkPtr res_chunk = chunk->clone_empty(required_rows);
        Columns res_orderby;
        res_chunk->append(*chunk, range.first + skipped_rows, required_rows);

        if (steal_orderby) {
            for (auto& column : orderby) {
                auto copy = column->clone_empty();
                copy->reserve(reserved_rows);
                copy->append(*column, range.first + skipped_rows, required_rows);
                res_orderby.push_back(std::move(copy));
            }
        }

        range.first += skipped_rows + required_rows;

        return std::make_pair(std::move(res_chunk), std::move(res_orderby));
    }
}

int SortedRun::compare_row(const SortDescs& desc, const SortedRun& rhs, size_t lhs_row, size_t rhs_row) const {
    DCHECK_LT(lhs_row, range.second);
    DCHECK_LT(rhs_row, rhs.range.second);
    for (int i = 0; i < desc.num_columns(); i++) {
        int x = get_column(i)->compare_at(lhs_row, rhs_row, *rhs.get_column(i), desc.get_column_desc(i).null_first);
        if (x != 0) {
            return x * desc.get_column_desc(i).sort_order;
        }
    }
    return 0;
}

bool SortedRun::is_sorted(const SortDescs& desc) const {
    for (size_t row = range.first; row + 1 < range.second; row++) {
        if (compare_row(desc, *this, row, row + 1) > 0) {
            return false;
        }
    }

    return true;
}

int SortedRun::debug_dump() const {
    for (int i = start_index(); i < end_index(); i++) {
        LOG(INFO) << fmt::format("row {}: {}", i, chunk->debug_row(i));
    }
    return 0;
}

size_t SortedRuns::num_rows() const {
    size_t res = 0;
    for (auto& run : chunks) {
        res += run.num_rows();
    }
    return res;
}

void SortedRuns::resize(size_t size) {
    // Do not expand if prodive a larger size
    if (num_rows() <= size) {
        return;
    }
    size_t accumulate = 0;
    for (int i = 0; i < chunks.size(); i++) {
        auto& run = chunks[i];
        if (accumulate + run.num_rows() >= size) {
            run.resize(size - accumulate);
            chunks.resize(i + 1);
            break;
        }
        accumulate += run.num_rows();
    }
}

void SortedRuns::clear() {
    chunks.clear();
}

bool SortedRuns::is_sorted(const SortDescs& sort_desc) const {
    for (int i = 0; i < chunks.size(); i++) {
        auto& run = chunks[i];
        if (!run.is_sorted(sort_desc)) {
            return false;
        }
        if (i > 0) {
            auto& prev = chunks[i - 1];
            if (!prev.empty()) {
                int x = prev.compare_row(sort_desc, run, prev.end_index() - 1, run.start_index());
                if (x > 0) {
                    return false;
                }
            }
        }
    }

    return true;
}

int SortedRuns::debug_dump() const {
    for (int k = 0; k < num_chunks(); k++) {
        chunks[k].debug_dump();
    }
    return 0;
}

ChunkPtr SortedRuns::assemble() const {
    if (chunks.empty()) {
        return {};
    }
    ChunkPtr result(chunks.front().clone_slice().release());
    for (int i = 1; i < chunks.size(); i++) {
        auto& run = chunks[i];
        result->append(*run.chunk, run.range.first, run.num_rows());
    }
    return result;
}

Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const SortedRun& left, const SortedRun& right,
                                   Permutation* output) {
    return MergeTwoChunk::merge_sorted_chunks_two_way(sort_desc, left, right, output);
}

Status merge_sorted_chunks(const SortDescs& descs, const std::vector<ExprContext*>* sort_exprs,
                           std::vector<ChunkUniquePtr>& chunks, SortedRuns* output) {
    std::vector<std::unique_ptr<SimpleChunkSortCursor>> cursors;
    std::vector<size_t> chunk_index(chunks.size(), 0);

    for (size_t i = 0; i < chunks.size(); i++) {
        if (chunks[i] == nullptr || chunks[i]->is_empty()) {
            continue;
        }
        cursors.emplace_back(std::make_unique<SimpleChunkSortCursor>(
                [&, i](ChunkUniquePtr* output, bool* eos) -> bool {
                    if (output == nullptr || eos == nullptr) {
                        return true;
                    }
                    *eos = true;
                    if (chunk_index[i] > 0) {
                        return false;
                    }
                    chunk_index[i]++;
                    *output = std::move(chunks[i]);
                    return true;
                },
                sort_exprs));
    }
    if (cursors.empty()) {
        return Status::OK();
    }

    ChunkConsumer consumer = [&](ChunkUniquePtr chunk) {
        output->chunks.emplace_back(ChunkPtr(chunk.release()), sort_exprs);
        return Status::OK();
    };
    return merge_sorted_cursor_cascade(descs, std::move(cursors), consumer);
}

Status merge_sorted_chunks_two_way_rowwise(const SortDescs& descs, const Columns& left_columns,
                                           const Columns& right_columns, Permutation* output, size_t limit) {
    constexpr int kLeftChunkIndex = 0;
    constexpr int kRightChunkIndex = 1;
    size_t index_of_merging = 0, index_of_left = 0, index_of_right = 0;
    size_t left_size = left_columns[0]->size();
    size_t right_size = right_columns[0]->size();
    output->reserve(limit);
    while ((index_of_merging < limit) && (index_of_left < left_size) && (index_of_right < right_size)) {
        int cmp = compare_chunk_row(descs, left_columns, right_columns, index_of_left, index_of_right);
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

} // namespace starrocks
