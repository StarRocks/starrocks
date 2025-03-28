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

#include "exec/sorting/merge_path.h"

#include <atomic>
#include <chrono>
#include <limits>
#include <mutex>

#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/sort/sort_context.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"

namespace starrocks::merge_path {

void merge(const SortDescs& descs, InputSegment& left, InputSegment& right, OutputSegment& dest,
           const size_t parallel_idx, const size_t degree_of_parallelism) {
    DCHECK_GT(degree_of_parallelism, 0);
    DCHECK_GE(left.len + right.len, dest.total_len);
    DCHECK_LE(left.len, left.runs.num_rows());
    DCHECK_LE(right.len, right.runs.num_rows());

    if (dest.total_len == 0) {
        left.forward = 0;
        right.forward = 0;
        return;
    }
    size_t li, ri;
    detail::_eval_diagonal_intersection(descs, left, right, dest.total_len, parallel_idx, degree_of_parallelism, &li,
                                        &ri);

    const bool is_last_parallelism = (parallel_idx == degree_of_parallelism - 1);
    const size_t start_di = parallel_idx * dest.total_len / degree_of_parallelism;
    const size_t next_start_di = (parallel_idx + 1) * dest.total_len / degree_of_parallelism;
    const size_t length = is_last_parallelism ? (dest.total_len - start_di) : (next_start_di - start_di);

    detail::_do_merge_along_merge_path(descs, left, li, right, ri, dest, start_di, length);

    dest.run.reset_range();

    if (is_last_parallelism) {
        left.forward = li - left.start;
        right.forward = ri - right.start;
        DCHECK_EQ(left.forward + right.forward, dest.total_len);
    }
}

std::vector<int32_t> detail::_build_orderby_indexes(const ChunkPtr& chunk,
                                                    const std::vector<ExprContext*>& sort_exprs) {
    std::vector<int32_t> orderby_indexes;
    auto& slot_id_to_index_map = chunk->get_slot_id_to_index_map();
    for (auto* expr_ctx : sort_exprs) {
        auto* expr = expr_ctx->root();
        if (expr->is_slotref()) {
            const auto& slot_id = down_cast<ColumnRef*>(expr)->slot_id();
            auto it = slot_id_to_index_map.find(slot_id);
            if (it != slot_id_to_index_map.end()) {
                orderby_indexes.push_back(it->second);
            } else {
                orderby_indexes.push_back(-1);
            }
        }
    }
    return orderby_indexes;
}

void detail::_eval_diagonal_intersection(const SortDescs& descs, const InputSegment& left, const InputSegment& right,
                                         const size_t d_size, const size_t parallel_idx,
                                         const size_t degree_of_parallelism, size_t* l_start, size_t* r_start) {
    const size_t diag = parallel_idx * d_size / degree_of_parallelism;
    DCHECK(diag < d_size);

    size_t high = diag;
    size_t low = 0;
    if (high > left.len) {
        high = left.len;
    }

    bool has_true;
    bool has_false;

    // binary search
    while (low < high) {
        size_t l_offset = low + (high - low) / 2;
        DCHECK(l_offset <= diag);
        size_t r_offset = diag - l_offset;

        _is_intersection(descs, left, left.start + l_offset, right, right.start + r_offset, has_true, has_false);
        bool is_intersection = has_true && has_false;

        if (is_intersection) {
            *l_start = left.start + l_offset;
            *r_start = right.start + r_offset;
            return;
        } else if (has_true) {
            high = l_offset;
        } else {
            low = l_offset + 1;
        }
    }

    // edge cases
    for (size_t offset = 0; offset <= 1; offset++) {
        size_t l_offset = low + offset;
        if (l_offset > diag) {
            break;
        }
        size_t r_offset = diag - l_offset;

        _is_intersection(descs, left, left.start + l_offset, right, right.start + r_offset, has_true, has_false);
        bool is_intersection = has_true && has_false;

        if (is_intersection) {
            *l_start = left.start + l_offset;
            *r_start = right.start + r_offset;
            return;
        }
    }

    CHECK(false);
}

void detail::_is_intersection(const SortDescs& descs, const InputSegment& left, const size_t li,
                              const InputSegment& right, const size_t ri, bool& has_true, bool& has_false) {
    auto evaluator = [&descs, &left, &right](const int64_t lii, const int64_t rii) {
        if (lii < static_cast<int64_t>(left.start)) {
            return false;
        } else if (lii >= static_cast<int64_t>(left.start + left.len)) {
            return true;
        } else if (rii < static_cast<int64_t>(right.start)) {
            return true;
        } else if (rii >= static_cast<int64_t>(right.start + right.len)) {
            return false;
        } else {
            const auto [l_run_idx, l_run_offset] = left.runs.get_run_idx(lii).value();
            const auto [r_run_idx, r_run_offset] = right.runs.get_run_idx(rii).value();
            const SortedRun& l_run = left.runs.get_run(l_run_idx);
            const SortedRun& r_run = right.runs.get_run(r_run_idx);
            return l_run.compare_row(descs, r_run, l_run_offset, r_run_offset) > 0;
        }
    };

    has_true = false;
    has_false = false;

    if (evaluator(static_cast<int64_t>(li) - 1, static_cast<int64_t>(ri) - 1)) {
        has_true = true;
    } else {
        has_false = true;
    }
    if (evaluator(static_cast<int64_t>(li) - 1, static_cast<int64_t>(ri))) {
        has_true = true;
    } else {
        has_false = true;
    }
    if (evaluator(static_cast<int64_t>(li), static_cast<int64_t>(ri) - 1)) {
        has_true = true;
    } else {
        has_false = true;
    }
    if (evaluator(static_cast<int64_t>(li), static_cast<int64_t>(ri))) {
        has_true = true;
    } else {
        has_false = true;
    }
}

void detail::_do_merge_along_merge_path(const SortDescs& descs, const InputSegment& left, size_t& li,
                                        const InputSegment& right, size_t& ri, OutputSegment& dest,
                                        const size_t start_di, const size_t length) {
    struct MergeIterator {
        // Input
        const InputSegment& input;

        // Index of input.runs
        size_t run_idx = std::numeric_limits<size_t>::max();
        // The SortedRun which run_idx is pointing at
        const SortedRun* run = nullptr;
        // Offset of chunk in run
        size_t offset = std::numeric_limits<size_t>::max();

        // Auxiliary data structure for Chunk::append
        std::pair<size_t, size_t> range;

        MergeIterator(const InputSegment& input) : input(input) {}
    };

    MergeIterator l_it(left);
    auto l_run_opt = left.runs.get_run_idx(li);
    if (l_run_opt.has_value()) {
        l_it.run_idx = l_run_opt.value().first;
        l_it.offset = l_run_opt.value().second;
        l_it.run = &left.runs.get_run(l_it.run_idx);
        DCHECK_GE(l_it.offset, l_it.run->start_index());
        DCHECK_LT(l_it.offset, l_it.run->end_index());
    }

    MergeIterator r_it(right);
    auto r_run_opt = right.runs.get_run_idx(ri);
    if (r_run_opt.has_value()) {
        r_it.run_idx = r_run_opt.value().first;
        r_it.offset = r_run_opt.value().second;
        r_it.run = &right.runs.get_run(r_it.run_idx);
        DCHECK_GE(r_it.offset, r_it.run->start_index());
        DCHECK_LT(r_it.offset, r_it.run->end_index());
    }

    std::vector<bool> skip_col_ids;
    skip_col_ids.resize(dest.run.chunk->num_columns());
    skip_col_ids.assign(dest.run.chunk->num_columns(), false);
    for (size_t col = 0; col < dest.run.chunk->num_columns(); col++) {
        auto it = std::find(dest.orderby_indexes.begin(), dest.orderby_indexes.end(), col);
        if (it != dest.orderby_indexes.end()) {
            skip_col_ids[col] = true;
        }
    }

    auto append = [&skip_col_ids](const MergeIterator& src_it, OutputSegment& dest) {
        auto column_append = [](auto& src_column, auto& dest_column, const MergeIterator& it) {
            dest_column->append(*src_column, it.range.first, it.range.second);
        };
        for (size_t col = 0; col < src_it.run->orderby.size(); col++) {
            auto& src_column = src_it.run->orderby[col];
            auto& dest_column = dest.run.orderby[col];
            column_append(src_column, dest_column, src_it);
        }
        for (size_t col = 0; col < src_it.run->chunk->num_columns(); col++) {
            if (skip_col_ids[col]) {
                continue;
            }
            auto& src_column = src_it.run->chunk->get_column_by_index(col);
            auto& dest_column = dest.run.chunk->get_column_by_index(col);
            column_append(src_column, dest_column, src_it);
        }
    };

    auto forward_iterator = [](MergeIterator& it) {
        DCHECK(it.run != nullptr);
        if (it.offset >= it.run->end_index()) {
            it.run = nullptr;
            do {
                it.run_idx++;
                if (it.run_idx >= it.input.runs.num_chunks()) {
                    break;
                }
                it.run = &it.input.runs.get_run(it.run_idx);
                it.offset = it.run->start_index();
            } while (it.run->num_rows() == 0);
        }
    };

    size_t di = start_di;
    while (di - start_di < length) {
        const size_t max_step = length - (di - start_di);
        if (li >= left.start + left.len) {
            // Left input has already been exhausted
            DCHECK_GT(r_it.run->end_index(), r_it.offset);
            const size_t remain = std::min(r_it.run->end_index() - r_it.offset, max_step);
            di += remain;
            ri += remain;
            r_it.range = std::make_pair(r_it.offset, remain);
            append(r_it, dest);
            r_it.offset += remain;
            forward_iterator(r_it);
        } else if (ri >= right.start + right.len) {
            // Right input has already been exhausted
            DCHECK_GT(l_it.run->end_index(), l_it.offset);
            const size_t remain = std::min(l_it.run->end_index() - l_it.offset, max_step);
            di += remain;
            li += remain;
            l_it.range = std::make_pair(l_it.offset, remain);
            append(l_it, dest);
            l_it.offset += remain;
            forward_iterator(l_it);
        } else {
            auto range_first_process = [&dest, &append, &forward_iterator, max_step](
                                               MergeIterator& it, const std::function<bool()>& satisfy, size_t& di,
                                               size_t& index) {
                // Try to find the rightmost offset that satisifed comparison
                const size_t original_offset = it.offset;
                size_t previous_offset;
                uint64_t step = 1;

                while (true) {
                    previous_offset = it.offset;
                    it.offset += step;
                    if (it.offset - original_offset >= max_step) {
                        break;
                    }
                    if (it.offset >= it.run->end_index()) {
                        break;
                    }
                    if (!satisfy()) {
                        if (step == 1) {
                            break;
                        }
                        // Forward too much, try to forward half of the former size
                        it.offset = previous_offset;
                        step >>= 1;
                        continue;
                    }
                    // Try to double forward step
                    step <<= 1;
                    const size_t remain = std::min(it.run->end_index() - it.offset, max_step);
                    if (step > remain) {
                        step = remain;
                    }
                }

                DCHECK_GE(previous_offset, original_offset);
                it.offset = original_offset;
                const size_t len = previous_offset + 1 - original_offset;
                it.range = std::make_pair(original_offset, len);
                di += len;
                index += len;
                append(it, dest);
                it.offset += len;
                forward_iterator(it);
            };

            auto compare = [&descs, &l_it, &r_it]() {
                return l_it.run->compare_row(descs, *r_it.run, l_it.offset, r_it.offset);
            };
            if (compare() <= 0) {
                auto satisfy = [&compare]() { return compare() <= 0; };
                range_first_process(l_it, satisfy, di, li);
            } else {
                auto satisfy = [&compare]() { return compare() > 0; };
                range_first_process(r_it, satisfy, di, ri);
            }
        }
    }

    // append order by columns
    for (size_t i = 0; i < dest.orderby_indexes.size(); i++) {
        int32_t col = dest.orderby_indexes[i];
        if (col >= 0) {
            dest.run.chunk->get_column_by_index(col) = dest.run.orderby[i];
        }
    }

    DCHECK_EQ(di - start_di, length);
    DCHECK_EQ(dest.run.chunk->num_rows(), length);
}

bool detail::Node::parent_input_full() {
    if (_parent == nullptr) {
        return false;
    }
    return _parent->input_full(this);
}

void detail::MergeNode::process_input(const int32_t parallel_idx) {
    _setup_input();

    if (!has_more_output() || parent_input_full()) {
        return;
    }

    DCHECK(_global_2_local_parallel_idx.find(parallel_idx) != _global_2_local_parallel_idx.end());
    const int32_t local_parallel_idx = _global_2_local_parallel_idx[parallel_idx];
    DCHECK(_output_segments[local_parallel_idx] == nullptr);

    InputSegment* primitive = nullptr;
    if (_left_buffer.runs.num_chunks() > 0) {
        primitive = &_left_buffer;
    } else if (_right_buffer.runs.num_chunks() > 0) {
        primitive = &_right_buffer;
    } else {
        DCHECK_EQ(_left_buffer.len, 0);
        DCHECK_EQ(_right_buffer.len, 0);
        return;
    }
    ChunkPtr dest_chunk = primitive->runs.chunks[0].chunk->clone_empty();
    Columns dest_orderby;
    for (auto& column : primitive->runs.chunks[0].orderby) {
        dest_orderby.push_back(column->clone_empty());
    }
    SortedRun dest_run(std::move(dest_chunk), std::move(dest_orderby));
    std::vector<int32_t> orderby_indexes = _build_orderby_indexes(dest_run.chunk, _merger->sort_exprs());

    _output_segments[local_parallel_idx] =
            std::make_unique<OutputSegment>(std::move(dest_run), std::move(orderby_indexes), _merge_length);

    merge(_merger->sort_descs(), _left_buffer, _right_buffer, *_output_segments[local_parallel_idx], local_parallel_idx,
          _global_2_local_parallel_idx.size());
}

[[maybe_unused]] size_t output_segment_size(const std::vector<OutputSegmentPtr>& output_segments) {
    size_t size = 0;
    for (auto& output_segment : output_segments) {
        if (output_segment != nullptr) {
            size += output_segment->run.num_rows();
        }
    }
    return size;
}

void detail::MergeNode::process_input_done() {
    DCHECK_EQ(_left_buffer.forward + _right_buffer.forward, _merge_length);
    DCHECK(_output_segments.empty() || _output_segments.size() == _global_2_local_parallel_idx.size());
    DCHECK_EQ(output_segment_size(_output_segments), _merge_length);

    _left_buffer.move_forward();
    _right_buffer.move_forward();
    _merge_length = 0;

    _input_ready = false;
}

bool detail::MergeNode::has_more_output() {
    return !_left->eos() || !_right->eos() || _left_buffer.len > 0 || _right_buffer.len > 0;
}

bool detail::MergeNode::input_full(Node* child) {
    if (child == _left) {
        return _left_buffer.len >= _merger->streaming_batch_size();
    } else {
        DCHECK_EQ(child, _right);
        return _right_buffer.len >= _merger->streaming_batch_size();
    }
}

void detail::MergeNode::_setup_input() {
    std::lock_guard<std::mutex> l(_m);

    if (_input_ready) {
        return;
    }

    DCHECK(_output_segments.empty());
    SortedRuns left_runs;
    SortedRuns right_runs;
    std::vector<OutputSegmentPtr> left_output_segments(std::move(_left->output_segments()));
    std::vector<OutputSegmentPtr> right_output_segments(std::move(_right->output_segments()));

    for (auto& output : left_output_segments) {
        if (output != nullptr && output->run.num_rows() > 0) {
            left_runs.chunks.push_back(std::move(output->run));
        }
    }
    for (auto& output : right_output_segments) {
        if (output != nullptr && output->run.num_rows() > 0) {
            right_runs.chunks.push_back(std::move(output->run));
        }
    }

    if (left_runs.num_rows() > 0) {
        _left_buffer.len += left_runs.num_rows();
        _left_buffer.runs.merge_runs(left_runs);
    }
    if (right_runs.num_rows() > 0) {
        _right_buffer.len += right_runs.num_rows();
        _right_buffer.runs.merge_runs(right_runs);
    }

    // CHECK(_left_buffer.runs.is_sorted(_merger->sort_descs()));
    // CHECK(_right_buffer.runs.is_sorted(_merger->sort_descs()));

    const size_t streaming_batch_size = _merger->streaming_batch_size();
    auto get_min = [](size_t n1, size_t n2, size_t n3) { return std::min(std::min(n1, n2), n3); };
    auto all_ge = [this](const InputSegment& large, const InputSegment& small) -> bool {
        if (large.len == 0 || small.len == 0) {
            return false;
        }

        // Smallest row in large
        const auto [large_run_idx, large_run_offset] = large.runs.get_run_idx(large.start).value();
        // Largest row in small
        const auto [small_run_idx, small_run_offset] = small.runs.get_run_idx(small.start + small.len - 1).value();

        return large.runs.get_run(large_run_idx)
                       .compare_row(_merger->sort_descs(), small.runs.get_run(small_run_idx), large_run_offset,
                                    small_run_offset) >= 0;
    };

    if (parent_input_full()) {
        _merge_length = 0;
    } else if (_left->eos() && _right->eos()) {
        _merge_length = std::min(_left_buffer.len + _right_buffer.len, streaming_batch_size);
    } else if (_left->eos()) {
        if (_left_buffer.len > 0) {
            // Generally, we can only move forward by the length of std::min(_left_buffer.len, _right_buffer.len).
            // And it may slow down the merge process to great extent if the length of _left_buffer is very small.
            // But if all rows in _left_buffer are greater than all rows in _right_buffer, we can simply move forward
            // by the length of _right_buffer, which can speed up the merge process significantly.
            if (all_ge(_left_buffer, _right_buffer)) {
                _merge_length = std::min(_right_buffer.len, streaming_batch_size);
            } else {
                _merge_length = get_min(_left_buffer.len, _right_buffer.len, streaming_batch_size);
            }
        } else {
            if (_right_buffer.len < streaming_batch_size) {
                // Just wait for more data
                _merge_length = 0;
            } else {
                _merge_length = streaming_batch_size;
            }
        }
    } else if (_right->eos()) {
        if (_right_buffer.len > 0) {
            // Generally, we can only move forward by the length of std::min(_left_buffer.len, _right_buffer.len).
            // And it may slow down the merge process to great extent if the length of _right_buffer is very small.
            // But if all rows in _right_buffer are greater than all rows in _left_buffer, we can simply move forward
            // by the length of _left_buffer, which can speed up the merge process significantly.
            if (all_ge(_right_buffer, _left_buffer)) {
                _merge_length = std::min(_left_buffer.len, streaming_batch_size);
            } else {
                _merge_length = get_min(_left_buffer.len, _right_buffer.len, streaming_batch_size);
            }
        } else {
            if (_left_buffer.len < streaming_batch_size) {
                // Just wait for more data
                _merge_length = 0;
            } else {
                _merge_length = streaming_batch_size;
            }
        }
    } else {
        if (_left_buffer.len < streaming_batch_size || _right_buffer.len < streaming_batch_size) {
            // Just wait for more data
            _merge_length = 0;
        } else {
            _merge_length = streaming_batch_size;
        }
    }

    _output_segments.resize(_global_2_local_parallel_idx.size());
    _input_ready = true;
}

void detail::LeafNode::process_input(const int32_t parallel_idx) {
    DCHECK_EQ(degree_of_parallelism(), 1);

    if (!has_more_output() || parent_input_full()) {
        return;
    }

    size_t output_size = 0;
    DCHECK(_output_segments.empty());

    while (!_provider_eos) {
        ChunkPtr chunk;
        {
            SCOPED_TIMER(_merger->get_metrics(parallel_idx)._sorted_run_provider_timer);
            if (!_provider(false, &chunk, &_provider_eos)) {
                break;
            }
        }
        if (chunk == nullptr || chunk->is_empty()) {
            continue;
        }

        Columns orderby;
        for (auto* expr : _merger->sort_exprs()) {
            auto column = EVALUATE_NULL_IF_ERROR(expr, expr->root(), chunk.get());
            orderby.push_back(column);
        }

        auto add_to_output_segments = [this, &output_size](ChunkPtr& standard_chunk, Columns& standard_orderby) {
            const size_t num_rows = standard_chunk->num_rows();
            SortedRun run(std::move(standard_chunk), std::move(standard_orderby));
            std::vector<int32_t> orderby_indexes;
            auto output_segment = std::make_unique<OutputSegment>(std::move(run), std::move(orderby_indexes), num_rows);

            output_size += output_segment->total_len;
            _output_segments.push_back(std::move(output_segment));
        };

        if (_late_materialization) {
            SCOPED_TIMER(_merger->get_metrics(parallel_idx)._late_materialization_generate_ordinal_timer);
            if (chunk->num_rows() <= MergePathCascadeMerger::MAX_CHUNK_SIZE) {
                const size_t num_rows = chunk->num_rows();
                const size_t chunk_id = _merger->add_original_chunk(std::move(chunk));
                chunk = _generate_ordinal(chunk_id, num_rows);
                add_to_output_segments(chunk, orderby);
            } else {
                size_t offset = 0;
                while (offset < chunk->num_rows()) {
                    const size_t num_rows =
                            std::min(chunk->num_rows() - offset, MergePathCascadeMerger::MAX_CHUNK_SIZE);
                    ChunkPtr standard_chunk = chunk->clone_empty(num_rows);
                    standard_chunk->append(*chunk, offset, num_rows);
                    DCHECK_EQ(standard_chunk->num_rows(), num_rows);
                    const size_t chunk_id = _merger->add_original_chunk(std::move(standard_chunk));
                    standard_chunk = _generate_ordinal(chunk_id, num_rows);

                    Columns standard_orderby;
                    for (const auto& column : orderby) {
                        ColumnPtr standard_column = column->clone_empty();
                        standard_column->reserve(num_rows);
                        standard_column->append(*column, offset, num_rows);
                        standard_orderby.push_back(std::move(standard_column));
                    }

                    add_to_output_segments(standard_chunk, standard_orderby);

                    offset += num_rows;
                }
            }
        } else {
            add_to_output_segments(chunk, orderby);
        }

        if (output_size >= _merger->streaming_batch_size()) {
            break;
        }
    }
}

ChunkPtr detail::LeafNode::_generate_ordinal(const size_t chunk_id, const size_t num_rows) {
    static TypeDescriptor s_type_desc = TypeDescriptor(TYPE_BIGINT);
    static Chunk::SlotHashMap s_slot_map = {{0, 0}};
    MutableColumnPtr ordinal_column = ColumnHelper::create_column(s_type_desc, false);
    ordinal_column->resize(num_rows);
    auto* raw_array = down_cast<Int64Column*>(ordinal_column.get())->get_data().data();

    for (size_t row = 0; row < num_rows; row++) {
        // The first (64 - OFFSET_BITS) bits are used for chunk_id
        // The last OFFSET_BITS bits are used for offset in chunk
        raw_array[row] =
                (static_cast<int64_t>(chunk_id) << MergePathCascadeMerger::OFFSET_BITS) | static_cast<int64_t>(row);
    }

    Columns columns;
    columns.push_back(std::move(ordinal_column));
    return std::make_shared<Chunk>(columns, s_slot_map);
}

MergePathCascadeMerger::MergePathCascadeMerger(const size_t chunk_size, const int32_t degree_of_parallelism,
                                               std::vector<ExprContext*> sort_exprs, const SortDescs& sort_descs,
                                               const TupleDescriptor* tuple_desc, const TTopNType::type topn_type,
                                               const int64_t offset, const int64_t limit,
                                               std::vector<MergePathChunkProvider> chunk_providers,
                                               TLateMaterializeMode::type mode)
        : _chunk_size(chunk_size > MAX_CHUNK_SIZE ? MAX_CHUNK_SIZE : chunk_size),
          _streaming_batch_size(4 * chunk_size * degree_of_parallelism),
          _degree_of_parallelism(degree_of_parallelism),
          _sort_exprs(std::move(sort_exprs)),
          _sort_descs(std::move(sort_descs)),
          _tuple_desc(tuple_desc),
          _topn_type(topn_type),
          _offset(offset),
          _limit(limit),
          _chunk_providers(std::move(chunk_providers)),
          _process_cnts(degree_of_parallelism),
          _output_chunks(degree_of_parallelism),
          _late_materialization_mode(mode) {
    _working_nodes.resize(_degree_of_parallelism);
    _metrics.resize(_degree_of_parallelism);

    _forward_stage(detail::Stage::INIT, 1);
}

bool MergePathCascadeMerger::is_current_stage_finished(const int32_t parallel_idx, const bool sync) {
    if (sync) {
        std::lock_guard<std::recursive_mutex> l(_status_m);
        return _process_cnts[parallel_idx] == 0;
    } else {
        return _process_cnts[parallel_idx] == 0;
    }
}

bool MergePathCascadeMerger::is_pending(const int32_t parallel_idx) {
    // For quick check outside the lock
    if (_stage != detail::Stage::PENDING) {
        return false;
    }

    std::lock_guard<std::recursive_mutex> l(_status_m);
    if (_stage != detail::Stage::PENDING) {
        return false;
    }

    if (is_current_stage_finished(parallel_idx, true)) {
        return true;
    }

    for (auto* leaf : _leafs) {
        if (leaf->provider_pending()) {
            return true;
        }
    }

    if (!_is_first_pending) {
        const auto now = std::chrono::steady_clock::now();
        const auto pending_time = std::chrono::duration_cast<std::chrono::nanoseconds>(now - _pending_start).count();
        std::for_each(_metrics.begin(), _metrics.end(), [pending_time](auto& metrics) {
            COUNTER_UPDATE(metrics.stage_counter, 1);
            COUNTER_UPDATE(metrics.stage_counters[detail::Stage::PENDING], 1);
            COUNTER_UPDATE(metrics.stage_timer, pending_time);
            COUNTER_UPDATE(metrics.stage_timers[detail::Stage::PENDING], pending_time);
        });
    }
    _is_first_pending = false;

    _forward_stage(detail::Stage::PREPARE, 1);
    return false;
}

bool MergePathCascadeMerger::is_finished() {
    // Since FINISHED is the final stage, so no lock needed here.
    return _stage == detail::Stage::FINISHED;
}

ChunkPtr MergePathCascadeMerger::try_get_next(const int32_t parallel_idx) {
    ChunkPtr chunk = nullptr;

    if (is_current_stage_finished(parallel_idx, true)) {
        return chunk;
    }

    const detail::Stage stage = _stage;

    COUNTER_UPDATE(_metrics[parallel_idx].stage_counter, 1);
    COUNTER_UPDATE(_metrics[parallel_idx].stage_counters[stage], 1);
    SCOPED_TIMER(_metrics[parallel_idx].stage_timer);
    SCOPED_TIMER(_metrics[parallel_idx].stage_timers[stage]);

    switch (stage) {
    case detail::INIT:
        _init();
        break;
    case detail::PREPARE:
        _prepare();
        break;
    case detail::PROCESS:
        _process(parallel_idx);
        break;
    case detail::SPLIT_CHUNK:
        _split_chunk(parallel_idx);
        break;
    case detail::FETCH_CHUNK:
        _fetch_chunk(parallel_idx, chunk);
        break;
    case detail::PENDING:
        break;
    case detail::FINISHED:
        break;
    default:
        CHECK(false);
    }

    return chunk;
}

void MergePathCascadeMerger::bind_profile(const int32_t parallel_idx, RuntimeProfile* profile) {
    auto& metrics = _metrics[parallel_idx];
    metrics.profile = profile;

    metrics.profile->add_info_string("Limit", std::to_string(_limit));
    metrics.profile->add_info_string("Offset", std::to_string(_offset));
    metrics.profile->add_info_string("StreamingBatchSize", std::to_string(_streaming_batch_size));

    const std::string overall_stage_timer_name = "OverallStageTime";
    metrics.stage_timer = ADD_TIMER(metrics.profile, overall_stage_timer_name);

    metrics.stage_timers.resize(detail::Stage::FINISHED + 1);
    metrics.stage_timers[detail::Stage::INIT] =
            ADD_CHILD_TIMER(metrics.profile, "1-InitStageTime", overall_stage_timer_name);
    metrics.stage_timers[detail::Stage::PREPARE] =
            ADD_CHILD_TIMER(metrics.profile, "2-PrepareStageTime", overall_stage_timer_name);
    metrics.stage_timers[detail::Stage::PROCESS] =
            ADD_CHILD_TIMER(metrics.profile, "3-ProcessStageTime", overall_stage_timer_name);
    metrics.stage_timers[detail::Stage::SPLIT_CHUNK] =
            ADD_CHILD_TIMER(metrics.profile, "4-SplitChunkStageTime", overall_stage_timer_name);
    metrics.stage_timers[detail::Stage::FETCH_CHUNK] =
            ADD_CHILD_TIMER(metrics.profile, "5-FetchChunkStageTime", overall_stage_timer_name);
    metrics.stage_timers[detail::Stage::PENDING] =
            ADD_CHILD_TIMER(metrics.profile, "6-PendingStageTime", overall_stage_timer_name);
    metrics.stage_timers[detail::Stage::FINISHED] =
            ADD_CHILD_TIMER(metrics.profile, "7-FinishedStageTime", overall_stage_timer_name);
    metrics._sorted_run_provider_timer =
            ADD_CHILD_TIMER(metrics.profile, "SortedRunProviderTime", "3-ProcessStageTime");
    metrics._late_materialization_generate_ordinal_timer =
            ADD_CHILD_TIMER(metrics.profile, "LateMaterializationGenerateOrdinalTime", "3-ProcessStageTime");
    metrics._late_materialization_restore_according_to_ordinal_timer = ADD_CHILD_TIMER(
            metrics.profile, "LateMaterializationRestoreAccordingToOrdinalTime", "4-SplitChunkStageTime");
    metrics._late_materialization_max_buffer_chunk_num = metrics.profile->add_counter(
            "LateMaterializationMaxBufferChunkNum", TUnit::UNIT,
            RuntimeProfile::Counter::create_strategy(TCounterAggregateType::SUM, TCounterMergeType::SKIP_FIRST_MERGE));

    const std::string overall_stage_counter_name = "OverallStageCount";
    metrics.stage_counter = ADD_COUNTER(metrics.profile, overall_stage_counter_name, TUnit::UNIT);

    metrics.stage_counters.resize(detail::Stage::FINISHED + 1);
    metrics.stage_counters[detail::Stage::INIT] =
            ADD_CHILD_COUNTER(metrics.profile, "1-InitStageCount", TUnit::UNIT, overall_stage_counter_name);
    metrics.stage_counters[detail::Stage::PREPARE] =
            ADD_CHILD_COUNTER(metrics.profile, "2-PrepareStageCount", TUnit::UNIT, overall_stage_counter_name);
    metrics.stage_counters[detail::Stage::PROCESS] =
            ADD_CHILD_COUNTER(metrics.profile, "3-ProcessStageCount", TUnit::UNIT, overall_stage_counter_name);
    metrics.stage_counters[detail::Stage::SPLIT_CHUNK] =
            ADD_CHILD_COUNTER(metrics.profile, "4-SplitChunkStageCount", TUnit::UNIT, overall_stage_counter_name);
    metrics.stage_counters[detail::Stage::FETCH_CHUNK] =
            ADD_CHILD_COUNTER(metrics.profile, "5-FetchChunkStageCount", TUnit::UNIT, overall_stage_counter_name);
    metrics.stage_counters[detail::Stage::PENDING] =
            ADD_CHILD_COUNTER(metrics.profile, "6-PendingStageCount", TUnit::UNIT, overall_stage_counter_name);
    metrics.stage_counters[detail::Stage::FINISHED] =
            ADD_CHILD_COUNTER(metrics.profile, "7-FinishedStageCount", TUnit::UNIT, overall_stage_counter_name);
}

size_t MergePathCascadeMerger::add_original_chunk(ChunkPtr&& chunk) {
    std::lock_guard<std::mutex> l(_late_materialization_m);

    if (_orderby_indexes.empty()) {
        auto& slot_id_to_index_map = chunk->get_slot_id_to_index_map();
        for (auto* expr_ctx : sort_exprs()) {
            auto* expr = expr_ctx->root();
            if (expr->is_slotref()) {
                const auto& slot_id = down_cast<ColumnRef*>(expr)->slot_id();
                auto it = slot_id_to_index_map.find(slot_id);
                if (it != slot_id_to_index_map.end()) {
                    _orderby_indexes.push_back(it->second);
                } else {
                    _orderby_indexes.push_back(-1);
                }
            }
        }
    }

    const size_t chunk_id = _chunk_id_generator++;
    const size_t chunk_size = chunk->num_rows();
    DCHECK_LE(chunk_size, MAX_CHUNK_SIZE);
    _original_chunk_buffer[chunk_id] = std::make_pair(std::move(chunk), chunk_size);
    _max_buffer_chunk_num = std::max(_max_buffer_chunk_num, _original_chunk_buffer.size());
    return chunk_id;
}

bool MergePathCascadeMerger::_is_current_stage_done() {
    std::lock_guard<std::recursive_mutex> l(_status_m);
    return std::all_of(_process_cnts.begin(), _process_cnts.end(), [](const auto& cnt) { return cnt == 0; });
}

void MergePathCascadeMerger::_forward_stage(const detail::Stage& stage, int32_t worker_num,
                                            std::vector<size_t>* process_cnts) {
    bool current_stage_finished = true;
    auto notify = defer_notify_source([&]() { return current_stage_finished; });
    std::lock_guard<std::recursive_mutex> l(_status_m);
    _stage = stage;
    DCHECK_GT(worker_num, 0);
    DCHECK_LE(worker_num, _degree_of_parallelism);

    if (process_cnts == nullptr) {
        for (size_t i = 0; i < _degree_of_parallelism; i++) {
            if (i < worker_num) {
                _process_cnts[i] = 1;
            } else {
                _process_cnts[i] = 0;
            }
        }
    } else {
        if (process_cnts != nullptr) {
            for (size_t i = 0; i < _degree_of_parallelism; i++) {
                _process_cnts[i] = (*process_cnts)[i];
            }
        }
    }
}

void MergePathCascadeMerger::_init() {
    DCHECK(_stage == detail::Stage::INIT);

    _init_late_materialization();

    std::vector<detail::NodePtr> leaf_nodes;
    for (auto& _chunk_provider : _chunk_providers) {
        auto leaf_node = std::make_unique<detail::LeafNode>(this, _late_materialization);
        leaf_node->set_provider(_chunk_provider);
        _leafs.push_back(leaf_node.get());
        leaf_nodes.push_back(std::move(leaf_node));
    }
    _levels.push_back(std::move(leaf_nodes));

    while (_levels[_levels.size() - 1].size() > 1) {
        std::vector<detail::NodePtr> next_level;

        auto& current_level = _levels[_levels.size() - 1];
        const auto current_size = current_level.size();

        int32_t i = 0;
        for (; i + 1 < current_size; i += 2) {
            auto& left = current_level[i];
            auto& right = current_level[i + 1];

            auto merge_node = std::make_unique<detail::MergeNode>(this, left.get(), right.get());
            left->bind_parent(merge_node.get());
            right->bind_parent(merge_node.get());
            next_level.push_back(std::move(merge_node));
        }

        if (i < current_size) {
            next_level.push_back(std::move(current_level[i]));
            current_level.resize(i);
        }
        _levels.push_back(std::move(next_level));
    }

    _root = _levels[_levels.size() - 1][0].get();

    _finish_current_stage(0, [this]() { _forward_stage(detail::Stage::PREPARE, 1); });
}

void MergePathCascadeMerger::_prepare() {
    DCHECK(_stage == detail::Stage::PREPARE);

    if (_level_idx >= _levels.size()) {
        _forward_stage(detail::Stage::SPLIT_CHUNK, _degree_of_parallelism);
        return;
    }

    auto& current_level = _levels[_level_idx];
    // Nodes need to get chunk from provider or perform merge algorithm.
    const size_t heavy_process_num = std::count_if(current_level.begin(), current_level.end(), [](const auto& node) {
        return !node->eos() && !node->parent_input_full();
    });
    // Nodes only need to merge output segments from its children into its input buffer.
    const size_t light_process_num = std::count_if(current_level.begin(), current_level.end(), [](const auto& node) {
        return !node->is_leaf() && !node->eos() && node->parent_input_full();
    });

    if (heavy_process_num + light_process_num == 0) {
        // All nodes of current level is finished
        _level_idx++;
        _forward_stage(detail::Stage::PREPARE, 1);
        return;
    }

    std::vector<size_t> process_cnts;
    process_cnts.assign(_degree_of_parallelism, 0);
    std::for_each(_working_nodes.begin(), _working_nodes.end(), [](auto& nodes) { nodes.clear(); });

    if (heavy_process_num > 0) {
        int32_t leaf_parallel_idx = 0;

        for (const auto& node : current_level) {
            if (node->eos() || node->parent_input_full()) {
                continue;
            }

            std::unordered_map<int32_t, int32_t> global_2_local_parallel_idx;
            if (node->is_leaf()) {
                if (leaf_parallel_idx >= _degree_of_parallelism) {
                    leaf_parallel_idx = 0;
                }

                global_2_local_parallel_idx[leaf_parallel_idx] = 0;
                _working_nodes[leaf_parallel_idx].push_back(node.get());
                process_cnts[leaf_parallel_idx]++;

                leaf_parallel_idx++;
            } else {
                for (size_t parallel_idx = 0; parallel_idx < _degree_of_parallelism; parallel_idx++) {
                    global_2_local_parallel_idx[parallel_idx] = parallel_idx;
                    _working_nodes[parallel_idx].push_back(node.get());
                    process_cnts[parallel_idx]++;
                }
            }

            node->bind_parallel_idxs(std::move(global_2_local_parallel_idx));
        }
    }

    if (light_process_num > 0) {
        // For light process nodes, we can only share the working_parallelism with heavy process nodes.
        int32_t parallel_idx = 0;
        for (const auto& node : current_level) {
            if (node->is_leaf() || node->eos() || !node->parent_input_full()) {
                continue;
            }
            if (parallel_idx >= _degree_of_parallelism) {
                parallel_idx = 0;
            }

            const size_t local_parallel_idx = 0;
            std::unordered_map<int32_t, int32_t> global_2_local_parallel_idx;

            global_2_local_parallel_idx[parallel_idx] = local_parallel_idx;
            _working_nodes[parallel_idx].push_back(node.get());
            process_cnts[parallel_idx]++;
            node->bind_parallel_idxs(std::move(global_2_local_parallel_idx));
            parallel_idx++;
        }
    }

    _finish_current_stage(0, [this, &process_cnts]() {
        _forward_stage(detail::Stage::PROCESS, _degree_of_parallelism, &process_cnts);
    });
}

void MergePathCascadeMerger::_process(const int32_t parallel_idx) {
    size_t cnt;
    {
        std::lock_guard<std::recursive_mutex> l(_status_m);
        cnt = _process_cnts[parallel_idx];
        DCHECK_GT(cnt, 0);
    }

    auto* node = _working_nodes[parallel_idx][cnt - 1];
    node->process_input(parallel_idx);

    _finish_current_stage(parallel_idx, [this]() {
        for (auto& node : _levels[_level_idx]) {
            node->process_input_done();
        }

        _level_idx++;
        _forward_stage(detail::Stage::PREPARE, 1);
    });
}

void MergePathCascadeMerger::_split_chunk(const int32_t parallel_idx) {
    DeferOp defer([this, parallel_idx]() {
        _finish_current_stage(parallel_idx, [this]() {
            if (_root->is_leaf()) {
                DCHECK(!_late_materialization);
                for (auto& output_segment : _root->output_segments()) {
                    if (output_segment->run.chunk != nullptr && !output_segment->run.chunk->is_empty()) {
                        _flat_output_chunks.push_back(std::move(output_segment->run.chunk));
                    }
                }
            } else {
                for (auto& chunks : _output_chunks) {
                    for (auto& chunk : chunks) {
                        if (chunk != nullptr && !chunk->is_empty()) {
                            _flat_output_chunks.push_back(std::move(chunk));
                        }
                    }
                }
            }
            _root->output_segments().clear();

            // Make all the parallelism attend this process, otherwise may block forever
            _forward_stage(detail::Stage::FETCH_CHUNK, _degree_of_parallelism);
        });
    });

    if (_root->is_leaf()) {
        // Chunks from leafNode already satisifed the size limitation
        return;
    }

    if (_root->output_segments().empty()) {
        return;
    }

    // If root is MergeNode, then will use all the available parallelism
    DCHECK_EQ(_degree_of_parallelism, _root->output_segments().size());
    if (_root->output_segments()[parallel_idx] == nullptr) {
        return;
    }
    SortedRun big_run = std::move(_root->output_segments()[parallel_idx]->run);
    DCHECK(big_run.chunk != nullptr);
    // CHECK(big_run.is_sorted(_sort_descs));

    size_t remain_size = big_run.num_rows();
    while (remain_size > 0) {
        auto pair = big_run.steal(_late_materialization, _chunk_size, 0);
        ChunkPtr chunk = pair.first;
        Columns orderby = std::move(pair.second);
        DCHECK(chunk != nullptr);
        DCHECK_GE(remain_size, chunk->num_rows());
        remain_size -= chunk->num_rows();
        if (_late_materialization) {
            DCHECK_EQ(orderby.size(), _sort_exprs.size());
            DCHECK(std::all_of(orderby.begin(), orderby.end(),
                               [&chunk](auto& column) { return column->size() == chunk->num_rows(); }));
            chunk = _restore_according_to_ordinal(parallel_idx, chunk, std::move(orderby));
        }
        _output_chunks[parallel_idx].push_back(std::move(chunk));
    }
}

void MergePathCascadeMerger::_fetch_chunk(const int32_t parallel_idx, ChunkPtr& chunk) {
    bool finished = false;
    DeferOp defer([this, parallel_idx, &finished]() {
        if (!finished) {
            return;
        }
        _finish_current_stage(parallel_idx, [this]() {
            DCHECK(_root->output_segments().empty());
            _reset_output();
            if (_short_circuit || _root->eos()) {
                _finishing();
                _forward_stage(detail::FINISHED, _degree_of_parallelism);
            } else if (_has_pending_node()) {
                _find_unfinished_level();
                _pending_start = std::chrono::steady_clock::now();
                _forward_stage(detail::Stage::PENDING, 1);
            } else {
                _find_unfinished_level();
                _forward_stage(detail::Stage::PREPARE, 1);
            }
        });
    });

    // Only the first parallelism can output chunk
    if (parallel_idx != 0) {
        finished = true;
        return;
    }

    if (_output_idx < _flat_output_chunks.size()) {
        chunk = std::move(_flat_output_chunks[_output_idx++]);
        DCHECK(chunk != nullptr);
        _process_limit(chunk);
    }

    finished = _short_circuit || _output_idx >= _flat_output_chunks.size();
}

void MergePathCascadeMerger::_finishing() {
    std::for_each(_metrics.begin(), _metrics.end(), [this](auto& metrics) {
        COUNTER_UPDATE(metrics._late_materialization_max_buffer_chunk_num, _max_buffer_chunk_num);
    });
}

void MergePathCascadeMerger::_init_late_materialization() {
    DeferOp defer([this]() {
        std::for_each(_metrics.begin(), _metrics.end(), [this](auto& metrics) {
            metrics.profile->add_info_string("LateMaterialization", _late_materialization ? "True" : "False");
        });
    });
    if (_chunk_providers.size() <= 2) {
        _late_materialization = false;
        return;
    }
    if (_late_materialization_mode == TLateMaterializeMode::ALWAYS) {
        _late_materialization = true;
        return;
    } else if (_late_materialization_mode == TLateMaterializeMode::NEVER) {
        _late_materialization = false;
        return;
    }

    const auto level_size = static_cast<size_t>(std::ceil(std::log2(_chunk_providers.size())));
    std::unordered_set<SlotId> early_materialized_slots;
    for (ExprContext* expr_ctx : _sort_exprs) {
        auto* expr = expr_ctx->root();
        if (expr->is_slotref()) {
            early_materialized_slots.insert(down_cast<ColumnRef*>(expr)->slot_id());
        }
    }
    size_t non_orderby_materialized_cost_per_level = 0;
    for (auto* slot : _tuple_desc->slots()) {
        if (early_materialized_slots.count(slot->id()) > 0) {
            continue;
        }

        // nullable column always contribute 1 byte to materialized cost.
        non_orderby_materialized_cost_per_level += slot->is_nullable();
        if (slot->type().is_string_type()) {
            // Slice is 16 bytes
            non_orderby_materialized_cost_per_level += 16;
        } else {
            non_orderby_materialized_cost_per_level += std::max<int>(1, slot->type().get_slot_size());
        }
    }
    const size_t total_original_cost = non_orderby_materialized_cost_per_level * level_size;

    // For late materialization, a auxiliary column(Int64Column) will be added to record the
    // original chunk id and row offset.
    // And in terms of locality, the larger the level, the worse the locality will be, so we need to apply
    // a locality decay factor for the late materialization
    double locality_decay_factor;
    if (level_size <= 2) {
        locality_decay_factor = 1.5;
    } else if (level_size == 3) {
        locality_decay_factor = 2.5;
    } else if (level_size == 4) {
        locality_decay_factor = 3.5;
    } else if (level_size == 5) {
        locality_decay_factor = 4.5;
    } else {
        locality_decay_factor = 5.5;
    }
    static TypeDescriptor s_auxiliary_column_type = TypeDescriptor(TYPE_BIGINT);
    const size_t total_late_materialized_cost = s_auxiliary_column_type.get_slot_size() * level_size +
                                                non_orderby_materialized_cost_per_level * locality_decay_factor;

    _late_materialization = total_late_materialized_cost <= total_original_cost;
}

ChunkPtr MergePathCascadeMerger::_restore_according_to_ordinal(const int32_t parallel_idx, const ChunkPtr& chunk,
                                                               Columns orderby) {
    SCOPED_TIMER(_metrics[parallel_idx]._late_materialization_restore_according_to_ordinal_timer);

    if (chunk == nullptr || chunk->is_empty()) {
        return nullptr;
    }

    const auto& ordinals = down_cast<Int64Column*>(chunk->get_column_by_index(0).get())->get_data();

    ChunkPtr output = nullptr;

    const size_t num_rows = chunk->num_rows();
    int64_t prev_chunk_id = -1;
    size_t prev_row = std::numeric_limits<size_t>::max();
    size_t range_start = std::numeric_limits<size_t>::max();

    auto get_original_pair = [this](size_t chunk_id) -> std::pair<ChunkPtr, size_t>& {
        std::lock_guard<std::mutex> l(_late_materialization_m);
        return _original_chunk_buffer[chunk_id];
    };

    std::vector<bool> skip_col_ids;
    auto init_skip_col_ids = [this, &skip_col_ids](const ChunkPtr& original_chunk) {
        if (!skip_col_ids.empty()) {
            return;
        }
        skip_col_ids.resize(original_chunk->num_columns());
        skip_col_ids.assign(original_chunk->num_columns(), false);
        for (size_t col = 0; col < original_chunk->num_columns(); col++) {
            auto it = std::find(_orderby_indexes.begin(), _orderby_indexes.end(), col);
            if (it != _orderby_indexes.end()) {
                skip_col_ids[col] = true;
            }
        }
    };

    auto append_original = [this, &get_original_pair, &skip_col_ids, &init_skip_col_ids](
                                   ChunkPtr& output, size_t chunk_id, size_t offset, size_t count) {
        auto& pair = get_original_pair(chunk_id);
        init_skip_col_ids(pair.first);
        for (size_t col = 0; col < pair.first->num_columns(); col++) {
            if (skip_col_ids[col]) {
                continue;
            }
            ColumnPtr& dest_column = output->get_column_by_index(col);
            dest_column->append(*pair.first->get_column_by_index(col), offset, count);
        }

        std::lock_guard<std::mutex> l(_late_materialization_m);
        DCHECK_GE(pair.second, count);
        pair.second -= count;
        if (pair.second == 0) {
            _original_chunk_buffer.erase(chunk_id);
        }
    };

    for (int64_t ordinal : ordinals) {
        // The first (64 - OFFSET_BITS) bits are used for chunk_id
        // The last OFFSET_BITS bits are used for offset in chunk
        const auto chunk_id = static_cast<size_t>(ordinal) >> OFFSET_BITS;
        const auto row = static_cast<size_t>(ordinal) & MAX_CHUNK_SIZE;
        if (prev_chunk_id == -1) {
            auto& pair = get_original_pair(chunk_id);
            output = pair.first->clone_empty(num_rows);
            prev_chunk_id = chunk_id;
            prev_row = row;
            range_start = row;
            continue;
        }
        if (chunk_id == prev_chunk_id && row == prev_row + 1) {
            prev_row = row;
            continue;
        }

        // append previous range
        append_original(output, prev_chunk_id, range_start, prev_row - range_start + 1);

        prev_chunk_id = chunk_id;
        range_start = row;
        prev_row = row;
    }

    // append last part
    append_original(output, prev_chunk_id, range_start, prev_row - range_start + 1);

    // append order by columns
    for (size_t i = 0; i < _orderby_indexes.size(); i++) {
        int32_t col = _orderby_indexes[i];
        if (col >= 0) {
            output->get_column_by_index(col) = orderby[i];
        }
    }

    return output;
}

void MergePathCascadeMerger::_process_limit(ChunkPtr& chunk) {
    if (_limit < 0) {
        return;
    }
    // TODO(hcf) handle RANK type
    if (_topn_type != TTopNType::ROW_NUMBER) {
        return;
    }
    const size_t current_num_rows = chunk->num_rows();
    if (_output_row_num + current_num_rows < _offset) {
        // Just drop it
        chunk = nullptr;
    } else {
        // Now _output_row_num + current_num_rows >= _offset
        size_t front_drop_size;
        if (_output_row_num >= _offset) {
            // Keep all
            front_drop_size = 0;
        } else {
            front_drop_size = _offset - _output_row_num;
        }
        if (front_drop_size > 0) {
            DCHECK_GE(chunk->num_rows(), front_drop_size);
            for (auto& column : chunk->columns()) {
                column->remove_first_n_values(front_drop_size);
            }
        }

        if (_output_row_num + current_num_rows >= _offset + _limit) {
            const size_t back_drop_size = _output_row_num + current_num_rows - (_offset + _limit);
            if (back_drop_size > 0) {
                DCHECK_GE(chunk->num_rows(), back_drop_size);
                for (auto& column : chunk->columns()) {
                    column->resize(column->size() - back_drop_size);
                }
            }
            _short_circuit = true;
        }
    }
    _output_row_num += current_num_rows;
}

void MergePathCascadeMerger::_find_unfinished_level() {
    _level_idx = 0;
    while (_level_idx < _levels.size()) {
        for (const auto& node : _levels[_level_idx]) {
            if (node->has_more_output()) {
                return;
            }
        }
        _level_idx++;
    }
}

void MergePathCascadeMerger::_finish_current_stage(const int32_t parallel_idx,
                                                   const std::function<void()>& stage_done_action) {
    std::lock_guard<std::recursive_mutex> l(_status_m);
    DCHECK_GT(_process_cnts[parallel_idx], 0);
    --_process_cnts[parallel_idx];
    if (_is_current_stage_done()) {
        stage_done_action();
    }
}

bool MergePathCascadeMerger::_has_pending_node() {
    for (auto* leaf : _leafs) {
        if (leaf->provider_pending()) {
            return true;
        }
    }
    return false;
}

void MergePathCascadeMerger::_reset_output() {
    for (auto& chunks : _output_chunks) {
        chunks.clear();
    }
    _flat_output_chunks.clear();
    _output_idx = 0;
}

} // namespace starrocks::merge_path
