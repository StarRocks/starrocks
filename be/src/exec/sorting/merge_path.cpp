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
    const size_t start_di = parallel_idx * dest.total_len / degree_of_parallelism;
    const size_t next_start_di = (parallel_idx + 1) * dest.total_len / degree_of_parallelism;
    const size_t length = next_start_di - start_di;

    detail::_do_merge_along_merge_path(descs, left, li, right, ri, dest, start_di, length);

    dest.run.reset_range();

    if (parallel_idx == degree_of_parallelism - 1) {
        left.forward = li - left.start;
        right.forward = ri - right.start;
    }
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
                                        const InputSegment& right, size_t& ri, OutputSegment& dest, size_t start_di,
                                        const size_t length) {
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
        std::optional<std::pair<size_t, size_t>> range;

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

    auto append = [](const MergeIterator& src_it, OutputSegment& dest) {
        auto column_append = [](auto& src_column, auto& dest_column, const MergeIterator& it) {
            if (!it.range.has_value()) {
                if (src_column->is_null(it.offset)) {
                    dest_column->append_nulls(1);
                } else {
                    dest_column->append_datum(src_column->get(it.offset));
                }
            } else {
                dest_column->append(*src_column, it.range.value().first, it.range.value().second);
            }
        };
        for (size_t col = 0; col < src_it.run->orderby.size(); col++) {
            auto& src_column = src_it.run->orderby[col];
            auto& dest_column = dest.run.orderby[col];
            column_append(src_column, dest_column, src_it);
        }
        for (size_t col = 0; col < src_it.run->chunk->num_columns(); col++) {
            auto& src_column = src_it.run->chunk->get_column_by_index(col);
            auto& dest_column = dest.run.chunk->get_column_by_index(col);
            column_append(src_column, dest_column, src_it);
        }
    };

    auto forward_iterator = [](MergeIterator& it) {
        DCHECK(it.run != nullptr);
        if (it.offset >= it.run->end_index()) {
            it.run = nullptr;
            it.range.reset();
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
    while (di - start_di < length && di < dest.total_len) {
        const size_t max_step = std::min(length - (di - start_di), dest.total_len - di);
        if (li >= left.start + left.len) {
            // Left input has already been exhausted
            DCHECK_GT(r_it.run->end_index(), r_it.offset);
            const size_t remain = std::min(r_it.run->end_index() - r_it.offset, max_step);
            di += remain;
            ri += remain;
            r_it.range = std::make_pair(r_it.offset, remain);
            append(r_it, dest);
            r_it.offset += remain;
            r_it.range.reset();
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
            l_it.range.reset();
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
                if (original_offset == previous_offset) {
                    di++;
                    index++;
                    append(it, dest);
                    it.offset++;
                    forward_iterator(it);
                } else {
                    const size_t len = previous_offset + 1 - original_offset;
                    it.range = std::make_pair(original_offset, len);
                    di += len;
                    index += len;
                    append(it, dest);
                    it.offset += len;
                    it.range.reset();
                    forward_iterator(it);
                }
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
    DCHECK_LE(di - start_di, length);
    DCHECK_LE(di, dest.total_len);
}

void detail::MergeNode::process_input(int32_t parallel_idx) {
    if (input_finished()) {
        return;
    }

    _setup_input();

    DCHECK(_global_2_local_parallel_idx.find(parallel_idx) != _global_2_local_parallel_idx.end());
    const int32_t local_parallel_idx = _global_2_local_parallel_idx[parallel_idx];
    DCHECK(_output_segments[local_parallel_idx] == nullptr);

    InputSegment* primitive = nullptr;
    if (_left_input->runs.num_chunks() > 0) {
        primitive = _left_input.get();
    } else if (_right_input->runs.num_chunks() > 0) {
        primitive = _right_input.get();
    } else {
        return;
    }
    ChunkPtr dest_chunk = primitive->runs.chunks[0].chunk->clone_empty();
    Columns dest_orderby;
    for (auto& column : primitive->runs.chunks[0].orderby) {
        dest_orderby.push_back(column->clone_empty());
    }
    SortedRun dest_run(std::move(dest_chunk), std::move(dest_orderby));

    _output_segments[local_parallel_idx] = std::make_unique<OutputSegment>(std::move(dest_run), _merge_length);

    merge(_merger->sort_descs(), *_left_input, *_right_input, *_output_segments[local_parallel_idx], local_parallel_idx,
          _global_2_local_parallel_idx.size());
}

void detail::MergeNode::process_input_done() {
    _left_input->move_forward();
    _right_input->move_forward();
    _input_ready = false;
}

void detail::MergeNode::_setup_input() {
    std::lock_guard<std::mutex> l(_m);

    if (_input_ready) {
        return;
    }

    DCHECK(output_empty());
    SortedRuns left_runs;
    SortedRuns right_runs;
    std::vector<OutputSegmentPtr> left_output_segments(_left->output_segments());
    std::vector<OutputSegmentPtr> right_output_segments(_right->output_segments());

    for (auto& output : left_output_segments) {
        if (output != nullptr) {
            left_runs.chunks.push_back(std::move(output->run));
        }
    }
    for (auto& output : right_output_segments) {
        if (output != nullptr) {
            right_runs.chunks.push_back(std::move(output->run));
        }
    }

    if (_left_input == nullptr) {
        _left_input = std::make_unique<InputSegment>(std::move(left_runs), 0, left_runs.num_rows());
    } else {
        _left_input->len += left_runs.num_rows();
        _left_input->runs.merge_runs(left_runs);
    }
    if (_right_input == nullptr) {
        _right_input = std::make_unique<InputSegment>(std::move(right_runs), 0, right_runs.num_rows());
    } else {
        _right_input->len += right_runs.num_rows();
        _right_input->runs.merge_runs(right_runs);
    }

    // DCHECK(_left_input->runs.is_sorted(_merger->sort_descs()));
    // DCHECK(_right_input->runs.is_sorted(_merger->sort_descs()));

    const size_t streaming_batch_size = _merger->streaming_batch_size();
    auto get_min = [](size_t n1, size_t n2, size_t n3) { return std::min(std::min(n1, n2), n3); };

    if (_left->dependency_finished() && _right->dependency_finished()) {
        _merge_length = _left_input->len + _right_input->len;
    } else if (_left->dependency_finished()) {
        if (_left_input->len > 0) {
            _merge_length = get_min(_left_input->len, _right_input->len, streaming_batch_size);
        } else {
            if (_right_input->len < streaming_batch_size) {
                // Just wait for more data
                _merge_length = 0;
            } else {
                _merge_length = streaming_batch_size;
            }
        }
    } else if (_right->dependency_finished()) {
        if (_right_input->len > 0) {
            _merge_length = get_min(_left_input->len, _right_input->len, streaming_batch_size);
        } else {
            if (_left_input->len < streaming_batch_size) {
                // Just wait for more data
                _merge_length = 0;
            } else {
                _merge_length = streaming_batch_size;
            }
        }
    } else {
        if (_left_input->len < streaming_batch_size || _right_input->len < streaming_batch_size) {
            // Just wait for more data
            _merge_length = 0;
        } else {
            _merge_length = streaming_batch_size;
        }
    }

    _output_segments.resize(_global_2_local_parallel_idx.size());
    _input_ready = true;
}

void detail::LeafNode::process_input(int32_t parallel_idx) {
    DCHECK_EQ(degree_of_parallelism(), 1);

    if (input_finished()) {
        return;
    }

    size_t output_size = 0;
    DCHECK(output_empty());

    while (!_provider_eos) {
        ChunkPtr chunk;
        if (!_provider(false, &chunk, &_provider_eos)) {
            break;
        }
        if (chunk == nullptr || chunk->is_empty()) {
            continue;
        }
        Columns orderby;
        for (auto* expr : _merger->sort_exprs()) {
            auto column = EVALUATE_NULL_IF_ERROR(expr, expr->root(), chunk.get());
            orderby.push_back(column);
        }
        SortedRun run(std::move(chunk), std::move(orderby));
        auto output_segment = std::make_unique<OutputSegment>(std::move(run), chunk->num_rows());

        output_size += output_segment->total_len;
        _output_segments.push_back(std::move(output_segment));

        if (output_size >= _merger->streaming_batch_size()) {
            break;
        }
    }
}

MergePathCascadeMerger::MergePathCascadeMerger(const int32_t degree_of_parallelism,
                                               std::vector<ExprContext*> sort_exprs, const SortDescs& sort_descs,
                                               std::vector<MergePathChunkProvider> chunk_providers,
                                               const size_t chunk_size)
        : _chunk_size(chunk_size),
          _streaming_batch_size(chunk_size * degree_of_parallelism),
          _degree_of_parallelism(degree_of_parallelism),
          _sort_exprs(std::move(sort_exprs)),
          _sort_descs(std::move(sort_descs)),
          _chunk_providers(std::move(chunk_providers)),
          _process_cnts(degree_of_parallelism),
          _output_chunks(degree_of_parallelism) {
    _working_nodes.resize(_degree_of_parallelism);
    _metrics.resize(_degree_of_parallelism);

    _forward_stage(detail::Stage::INIT, 1);
}

bool MergePathCascadeMerger::is_current_stage_finished(int32_t parallel_idx) {
    if (_is_forwarding_stage.load(std::memory_order_relaxed)) {
        return true;
    }

    return parallel_idx >= _working_parallelism ? true
                                                : _process_cnts[parallel_idx].load(std::memory_order_relaxed) == 0;
}

bool MergePathCascadeMerger::is_pending(int32_t parallel_idx) {
    if (_is_forwarding_stage.load(std::memory_order_relaxed)) {
        return true;
    }

    if (_stage.load(std::memory_order_relaxed) != detail::Stage::PENDING) {
        return false;
    }

    if (is_current_stage_finished(parallel_idx)) {
        return true;
    }

    for (auto* leaf : _leafs) {
        if (leaf->is_pending()) {
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
    if (_is_forwarding_stage.load(std::memory_order_relaxed)) {
        return false;
    }

    return _stage == detail::Stage::FINISHED;
}

ChunkPtr MergePathCascadeMerger::try_get_next(const int32_t parallel_idx) {
    ChunkPtr chunk = nullptr;

    if (is_current_stage_finished(parallel_idx)) {
        return chunk;
    }

    detail::Stage stage = _stage.load(std::memory_order_relaxed);

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
    case detail::FINISHED:
        break;
    default:
        CHECK(false);
    }

    return chunk;
}

void MergePathCascadeMerger::bind_profile(int32_t parallel_idx, RuntimeProfile* profile) {
    auto& metrics = _metrics[parallel_idx];
    metrics.profile = profile;

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

bool MergePathCascadeMerger::_is_current_stage_done() {
    return std::all_of(_process_cnts.begin(), _process_cnts.begin() + _working_parallelism,
                       [](auto& cnt) { return cnt.load(std::memory_order_relaxed) == 0; });
}

void MergePathCascadeMerger::_forward_stage(const detail::Stage& stage, int32_t worker_num,
                                            std::vector<size_t>* process_cnts) {
    // Enter critical section
    _is_forwarding_stage.store(true, std::memory_order_relaxed);

    _stage.store(stage, std::memory_order_relaxed);
    _working_parallelism = worker_num;
    for (auto& cnt : _process_cnts) {
        cnt.store(1, std::memory_order_relaxed);
    }
    if (process_cnts != nullptr) {
        for (size_t i = 0; i < worker_num; i++) {
            _process_cnts[i].store((*process_cnts)[i], std::memory_order_relaxed);
        }
    }

    // Exit critical section
    _is_forwarding_stage.store(false, std::memory_order_relaxed);
}

void MergePathCascadeMerger::_init() {
    DCHECK(_stage == detail::Stage::INIT);

    std::vector<detail::NodePtr> leaf_nodes;
    for (auto& _chunk_provider : _chunk_providers) {
        auto leaf_node = std::make_unique<detail::LeafNode>(this);
        leaf_node->set_provider(std::move(_chunk_provider));
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
    const size_t active_size =
            std::count_if(current_level.begin(), current_level.end(), [](const auto& node) { return !node->eos(); });
    // In the context of global merge, the number of LeafNode is defined by sender num,
    // which may greater than the degree_of_parallelism. So in this situation, one parallelism may need to
    // handle more than one nodes.
    const bool overloaded = active_size > _degree_of_parallelism;
    const auto avg_local_degree_of_parallelism = _degree_of_parallelism / std::max(1ul, active_size);

    int32_t parallel_idx = 0;
    int32_t working_parallelism = overloaded ? _degree_of_parallelism : 0;
    std::vector<size_t> process_cnts;
    process_cnts.assign(_degree_of_parallelism, 0);

    std::for_each(_working_nodes.begin(), _working_nodes.end(), [](auto& nodes) { nodes.clear(); });

    for (const auto& node : current_level) {
        if (node->eos()) {
            continue;
        }
        size_t local_degree_of_parallelism;
        if (overloaded) {
            local_degree_of_parallelism = 1;
        } else {
            if (node->is_leaf()) {
                local_degree_of_parallelism = 1;
            } else if (working_parallelism + avg_local_degree_of_parallelism > _degree_of_parallelism) {
                DCHECK_LE(working_parallelism, _degree_of_parallelism);
                local_degree_of_parallelism = _degree_of_parallelism - working_parallelism;
            } else {
                local_degree_of_parallelism = avg_local_degree_of_parallelism;
            }
            working_parallelism += local_degree_of_parallelism;
        }

        std::unordered_map<int32_t, int32_t> global_2_local_parallel_idx;
        for (size_t local_parallel_idx = 0; local_parallel_idx < local_degree_of_parallelism; local_parallel_idx++) {
            if (parallel_idx >= _degree_of_parallelism) {
                // Reuse parallelism if overloaded
                DCHECK(overloaded);
                parallel_idx = 0;
            }

            global_2_local_parallel_idx[parallel_idx] = local_parallel_idx;
            _working_nodes[parallel_idx].push_back(node.get());

            process_cnts[parallel_idx]++;

            ++parallel_idx;
        }

        node->bind_parallel_idxs(std::move(global_2_local_parallel_idx));
    }

    _finish_current_stage(0, [this, working_parallelism, &process_cnts]() {
        _forward_stage(detail::Stage::PROCESS, working_parallelism, &process_cnts);
    });
}

void MergePathCascadeMerger::_process(int32_t parallel_idx) {
    const size_t cnt = _process_cnts[parallel_idx].load(std::memory_order_relaxed);
    DCHECK_GT(cnt, 0);

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

void MergePathCascadeMerger::_split_chunk(int32_t parallel_idx) {
    DeferOp defer([this, parallel_idx]() {
        _finish_current_stage(parallel_idx, [this]() {
            if (_root->is_leaf()) {
                for (auto&& output_segment : _root->output_segments()) {
                    _flat_output_chunks.push_back(std::move(output_segment->run.chunk));
                }
            } else {
                for (auto& chunks : _output_chunks) {
                    for (auto& chunk : chunks) {
                        _flat_output_chunks.push_back(std::move(chunk));
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

    // If root is MergeNode, then will use all the available parallelism
    DCHECK_EQ(_degree_of_parallelism, _root->output_segments().size());
    if (_root->output_segments()[parallel_idx] == nullptr) {
        return;
    }
    SortedRun big_run = std::move(_root->output_segments()[parallel_idx]->run);
    DCHECK(big_run.chunk != nullptr);
    // DCHECK(big_run.is_sorted(_sort_descs));

    size_t remain_size = big_run.num_rows();
    while (remain_size > 0) {
        ChunkPtr chunk = big_run.steal_chunk(_chunk_size);
        DCHECK(chunk != nullptr);
        DCHECK_GE(remain_size, chunk->num_rows());
        remain_size -= chunk->num_rows();
        _output_chunks[parallel_idx].push_back(std::move(chunk));
    }
}

void MergePathCascadeMerger::_fetch_chunk(int32_t parallel_idx, ChunkPtr& chunk) {
    bool finished = false;
    DeferOp defer([this, parallel_idx, &finished]() {
        if (!finished) {
            return;
        }
        _finish_current_stage(parallel_idx, [this]() {
            DCHECK(_root->output_empty());
            _reset_output();
            if (_root->eos()) {
                _forward_stage(detail::FINISHED, _working_parallelism);
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
    }

    finished = _output_idx >= _flat_output_chunks.size();
}

void MergePathCascadeMerger::_find_unfinished_level() {
    _level_idx = 0;
    while (_level_idx < _levels.size()) {
        for (const auto& node : _levels[_level_idx]) {
            if (!node->input_finished()) {
                return;
            }
        }
        _level_idx++;
    }
}

void MergePathCascadeMerger::_finish_current_stage(int32_t parallel_idx,
                                                   const std::function<void()>& stage_done_action) {
    std::lock_guard<std::mutex> l(_m);
    DCHECK_GT(_process_cnts[parallel_idx], 0);
    _process_cnts[parallel_idx].fetch_sub(1, std::memory_order_relaxed);
    if (_is_current_stage_done()) {
        stage_done_action();
    }
}

bool MergePathCascadeMerger::_has_pending_node() {
    for (auto* leaf : _leafs) {
        if (leaf->is_pending()) {
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
