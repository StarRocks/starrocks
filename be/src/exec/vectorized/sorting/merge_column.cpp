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
        // TODO: avoid specialization
        if constexpr (std::is_same_v<FixedLengthColumnBase<int32_t>, ColumnType> ||
                      std::is_same_v<FixedLengthColumn<int32_t>, ColumnType>) {
            auto& left_data = left_col->get_data();
            while (last < lhs_end && left_data[last - 1] == left_data[last]) {
                last++;
            }
        } else {
            while (last < lhs_end && left_col->compare_at(last - 1, last, *left_col, _null_first) == 0) {
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
            while (last < rhs_end && right_col->compare_at(last - 1, last, *right_col, _null_first) == 0) {
                last++;
            }
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

// Merge two sorted cusor
class MergeTwoCursor {
public:
    MergeTwoCursor(const SortDescs& sort_desc, std::unique_ptr<ChunkCursor>&& left_cursor,
                   std::unique_ptr<ChunkCursor>&& right_cursor)
            : _sort_desc(sort_desc), _left_cursor(std::move(left_cursor)), _right_cursor(std::move(right_cursor)) {
        _left_cursor->next_chunk_for_pipeline();
        _right_cursor->next_chunk_for_pipeline();
        if (_left_cursor->has_next()) {
            _left_run = SortedRun(_left_cursor->get_current_chunk());
        }
        if (_right_cursor->has_next()) {
            _right_run = SortedRun(_right_cursor->get_current_chunk());
        }
        _chunk_supplier = [&](Chunk** output) -> bool {
            auto chunk = next();
            if (!chunk.ok()) return false;
            if (!chunk.value()) return false;
            *output = chunk.value().release();
            return true;
        };
    }

    // Use it as iterator
    // Return nullptr if no output
    StatusOr<ChunkUniquePtr> next() {
        if (_left_run.empty() && _right_run.empty()) {
            return ChunkUniquePtr();
        }
        return merge_sorted_cursor_two_way();
    }

    // Consume all inputs and produce output through the callback function
    Status consume_all(ChunkConsumer output) {
        auto chunk = next();
        while (chunk.ok() && !!chunk.value()) {
            output(std::move(chunk.value()));
            chunk = next();
        }
        return Status::OK();
    }

    // Use this as a supplier
    ChunkProbeSupplier& as_supplier() { return _chunk_supplier; }

    // use this as cursor
    std::unique_ptr<ChunkCursor> as_chunk_cursor() {
        if (!_output_cursor) {
            _output_cursor.reset(new ChunkCursor(as_supplier(), _left_cursor->get_sort_exprs(),
                                                 *_left_cursor->get_sort_orders(), *_left_cursor->get_null_firsts()));
        }
        return std::move(_output_cursor);
    }

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
            // TODO: optimize with tie
            // The first column
            std::vector<EqualRange> equal_ranges;
            equal_ranges.emplace_back(left_run.range, right_run.range);

            output->resize(left_run.range.second + right_run.range.second);
            // Iterate each column
            for (int col = 0; col < left_run.num_columns(); col++) {
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
    // 1. Find smaller tail
    // 2. Cutoff the chunk based on tail
    // 3. Merge two chunks and output
    // 4. Move to next
    StatusOr<ChunkUniquePtr> merge_sorted_cursor_two_way() {
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
            int tail_cmp = compare_tail(sort_desc, _left_run, _right_run);
            if (tail_cmp <= 0) {
                // Cutoff right by left tail
                size_t right_cut =
                        cutoff_run(sort_desc, _right_run, std::make_pair(_left_run, _left_run.num_rows() - 1));
                SortedRun right_1(_right_run.chunk, 0, right_cut);
                SortedRun right_2(_right_run.chunk, right_cut, _right_run.num_rows());

                // Merge partial chunk
                Permutation perm;
                RETURN_IF_ERROR(MergeTwoCursor::merge_sorted_chunks_two_way(sort_desc, _left_run, right_1, &perm));
                trim_permutation(_left_run, right_1, perm);
                DCHECK_EQ(_left_run.num_rows() + right_1.num_rows(), perm.size());
                ChunkUniquePtr merged = _left_run.chunk->clone_empty(perm.size());
                append_by_permutation(merged.get(), {_left_run.chunk, right_1.chunk}, perm);

                _left_run.reset();
                _right_run = right_2;
                result = std::move(merged);
            } else {
                // Cutoff left by right tail
                size_t left_cut =
                        cutoff_run(sort_desc, _left_run, std::make_pair(_right_run, _right_run.num_rows() - 1));
                SortedRun left_1(_left_run.chunk, 0, left_cut);
                SortedRun left_2(_left_run.chunk, left_cut, _left_run.num_rows());

                // Merge partial chunk
                Permutation perm;
                RETURN_IF_ERROR(MergeTwoCursor::merge_sorted_chunks_two_way(sort_desc, _right_run, left_1, &perm));
                trim_permutation(left_1, _right_run, perm);
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
        if (_left_run.empty()) {
            _left_cursor->next_chunk_for_pipeline();
            if (_left_cursor->has_next()) {
                _left_run = SortedRun(_left_cursor->get_current_chunk());
            }
        }
        if (_right_run.empty()) {
            _right_cursor->next_chunk_for_pipeline();
            if (_right_cursor->has_next()) {
                _right_run = SortedRun(_right_cursor->get_current_chunk());
            }
        }

        DCHECK(!!result);
        return result;
    }

    SortDescs _sort_desc;
    SortedRun _left_run;
    SortedRun _right_run;
    std::unique_ptr<ChunkCursor> _left_cursor;
    std::unique_ptr<ChunkCursor> _right_cursor;
    ChunkProbeSupplier _chunk_supplier;
    std::unique_ptr<ChunkCursor> _output_cursor = nullptr;

private:
    static void trim_permutation(SortedRun left, SortedRun right, Permutation& perm) {
        size_t start = left.range.first + right.range.first;
        size_t end = left.range.second + right.range.second;
        std::copy(perm.begin() + start, perm.begin() + end, perm.begin());
        perm.resize(end - start);
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

    static int compare_tail(const SortDescs& desc, const SortedRun& left, const SortedRun& right) {
        size_t lhs_tail = left.num_rows() - 1;
        size_t rhs_tail = right.num_rows() - 1;
        return compare_chunk_row(desc, *(left.chunk), *(right.chunk), lhs_tail, rhs_tail);
    }
};

// Merge multiple cursors in cascade way
class MergeCursorsCascade {
public:
    static Status merge_sorted_cursor_cascade(const SortDescs& sort_desc,
                                              std::vector<std::unique_ptr<ChunkCursor>>& cursors,
                                              ChunkConsumer consumer) {
        // Build a cascade merge tree
        std::vector<std::vector<std::unique_ptr<ChunkCursor>>> level_cursors;
        int level_size = cursors.size();
        level_cursors.push_back(std::move(cursors));
        std::vector<std::unique_ptr<MergeTwoCursor>> mergers;

        while (level_size > 1) {
            std::vector<std::unique_ptr<ChunkCursor>> next_level;
            auto& current_level = level_cursors.back();
            next_level.reserve(current_level.size() / 2);

            for (int i = 0; i < current_level.size(); i += 2) {
                auto& left = current_level[i];
                auto& right = current_level[i + 1];
                mergers.emplace_back(std::make_unique<MergeTwoCursor>(sort_desc, std::move(left), std::move(right)));
                next_level.push_back(mergers.back()->as_chunk_cursor());
            }
            if (current_level.size() % 2 == 1) {
                next_level.push_back(std::move(current_level.back()));
            }

            level_size = next_level.size();
            level_cursors.push_back(std::move(next_level));
        }

        CHECK_EQ(1, level_cursors.back().size());
        auto& root_cursor = level_cursors.back()[0];

        root_cursor->next_chunk_for_pipeline();
        while (root_cursor->has_next()) {
            ChunkPtr chunk = root_cursor->get_current_chunk();
            RETURN_IF_ERROR(consumer(chunk->clone_unique()));

            root_cursor->next_chunk_for_pipeline();
        }

        return Status::OK();
    }

private:
};

// Merge two-way sorted run into on sorted run
// TODO: specify the ordering
Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const ChunkPtr left, const ChunkPtr right,
                                   Permutation* output) {
    DCHECK_EQ(left->num_columns(), right->num_columns());

    SortedRun left_run(left, std::make_pair(0, left->num_rows()));
    SortedRun right_run(right, std::make_pair(0, right->num_rows()));
    return MergeTwoCursor::merge_sorted_chunks_two_way(sort_desc, left_run, right_run, output);
}

Status merge_sorted_cursor_two_way(const SortDescs& sort_desc, std::unique_ptr<ChunkCursor> left_cursor,
                                   std::unique_ptr<ChunkCursor> right_cursor, ChunkConsumer output) {
    MergeTwoCursor merger(sort_desc, std::move(left_cursor), std::move(right_cursor));
    return merger.consume_all(output);
}

Status merge_sorted_cursor_cascade(const SortDescs& sort_desc, std::vector<std::unique_ptr<ChunkCursor>>& cursors,
                                   ChunkConsumer consumer) {
    return MergeCursorsCascade::merge_sorted_cursor_cascade(sort_desc, cursors, consumer);
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