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

#include "analytic_sink_operator.h"

#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status AnalyticSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    // _analytor is shared by sink operator and source operator
    // we must only prepare it at sink operator
    RETURN_IF_ERROR(_analytor->prepare(state, state->obj_pool(), _unique_metrics.get()));
    RETURN_IF_ERROR(_analytor->open(state));

    TAnalyticWindow window = _tnode.analytic_node.window;

    _process_by_partition_if_necessary = &AnalyticSinkOperator::_process_by_partition_if_necessary_materializing;
    if (!_tnode.analytic_node.__isset.window) {
        _process_by_partition = &AnalyticSinkOperator::_process_by_partition_for_unbounded_frame;
    } else if (window.type == TAnalyticWindowType::RANGE) {
        // RANGE windows must have UNBOUNDED PRECEDING
        // RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING
        DCHECK(!window.__isset.window_start);
        DCHECK(!window.__isset.window_end || window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW);
        if (!window.__isset.window_end) {
            // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            _process_by_partition = &AnalyticSinkOperator::_process_by_partition_for_unbounded_frame;
        } else {
            // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            DCHECK_EQ(window.window_end.type, TAnalyticWindowBoundaryType::CURRENT_ROW);
            if (!_analytor->need_partition_materializing()) {
                _process_by_partition_if_necessary =
                        &AnalyticSinkOperator::
                                _process_by_partition_if_necessary_for_unbounded_preceding_range_frame_streaming;
                _process_by_partition = nullptr;
            } else {
                _process_by_partition =
                        &AnalyticSinkOperator::_process_by_partition_for_unbounded_preceding_range_frame_materializing;
            }
        }
    } else {
        if (!window.__isset.window_start && !window.__isset.window_end) {
            // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            _process_by_partition = &AnalyticSinkOperator::_process_by_partition_for_unbounded_frame;
        } else if (!window.__isset.window_start && window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW) {
            // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            if (!_analytor->need_partition_materializing()) {
                _process_by_partition_if_necessary =
                        &AnalyticSinkOperator::
                                _process_by_partition_if_necessary_for_unbounded_preceding_rows_frame_streaming;
                _process_by_partition = nullptr;
            } else {
                _process_by_partition =
                        &AnalyticSinkOperator::_process_by_partition_for_unbounded_preceding_rows_frame_materializing;
            }
        } else {
            // ROWS BETWEEN N PRECEDING AND M FOLLOWING or
            // ROWS BETWEEN N PRECEDING AND CURRENT ROW
            _process_by_partition = &AnalyticSinkOperator::_process_by_partition_for_sliding_frame;
        }
    }

    return Status::OK();
}

void AnalyticSinkOperator::close(RuntimeState* state) {
    _analytor->unref(state);
    Operator::close(state);
}

Status AnalyticSinkOperator::set_finishing(RuntimeState* state) {
    // skip processing if cancelled
    if (state->is_cancelled()) {
        return Status::Cancelled("runtime state is cancelled");
    }
    _is_finished = true;
    _analytor->input_eos() = true;
    RETURN_IF_ERROR((this->*_process_by_partition_if_necessary)());
    _analytor->sink_complete();
    return Status::OK();
}

StatusOr<ChunkPtr> AnalyticSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status AnalyticSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _analytor->remove_unused_buffer_values(state);

    RETURN_IF_ERROR(_analytor->add_chunk(chunk));
    RETURN_IF_ERROR((this->*_process_by_partition_if_necessary)());

    return _analytor->check_has_error();
}

Status AnalyticSinkOperator::_process_by_partition_if_necessary_materializing() {
    while (_analytor->has_output()) {
        if (_analytor->reached_limit()) {
            return Status::OK();
        }

        _analytor->find_partition_end();
        // Only process after all the data in a partition is reached
        if (!_analytor->found_partition_end().first) {
            return Status::OK();
        }

        const auto chunk_size = static_cast<int64_t>(_analytor->current_chunk_size());
        _analytor->create_agg_result_columns(chunk_size);

        bool is_new_partition = _analytor->is_new_partition();
        if (is_new_partition) {
            _analytor->reset_state_for_cur_partition();
        }

        (this->*_process_by_partition)(chunk_size, is_new_partition);

        // Chunk may contains multiply partitions, so the chunk need to be reprocessed
        if (_analytor->is_current_chunk_finished_eval(chunk_size)) {
            ChunkPtr chunk;
            RETURN_IF_ERROR(_analytor->output_result_chunk(&chunk));
            _analytor->offer_chunk_to_buffer(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticSinkOperator::_process_by_partition_if_necessary_for_unbounded_preceding_rows_frame_streaming() {
    // When set_finishing(), the has_output() may be false, so add the check
    if (!_analytor->has_output()) {
        return Status::OK();
    }

    if (_analytor->reached_limit()) {
        return Status::OK();
    }

    // reset state for the first partition
    if (_analytor->current_row_position() == 0) {
        _analytor->reset_window_state();
    }

    const auto chunk_size = static_cast<int64_t>(_analytor->current_chunk_size());
    _analytor->create_agg_result_columns(chunk_size);

    do {
        _analytor->find_partition_end();

        while (_analytor->current_row_position() < _analytor->found_partition_end().second) {
            _analytor->update_window_batch(_analytor->partition_start(), _analytor->found_partition_end().second,
                                           _analytor->current_row_position(), _analytor->current_row_position() + 1);

            _analytor->update_window_result_position(1);
            int64_t frame_start = _analytor->get_total_position(_analytor->current_row_position()) -
                                  _analytor->first_total_position_of_current_chunk();

            DCHECK_GE(frame_start, 0);
            _analytor->get_window_function_result(frame_start, _analytor->window_result_position());
            _analytor->update_current_row_position(1);
        }

        if (_analytor->found_partition_end().first) {
            _analytor->reset_state_for_next_partition();
        }
    } while (!_analytor->is_current_chunk_finished_eval(chunk_size));

    ChunkPtr chunk;
    RETURN_IF_ERROR(_analytor->output_result_chunk(&chunk));
    _analytor->offer_chunk_to_buffer(chunk);

    return Status::OK();
}

Status AnalyticSinkOperator::_process_by_partition_if_necessary_for_unbounded_preceding_range_frame_streaming() {
    // reset state for the first partition
    if (_analytor->current_row_position() == 0) {
        _analytor->reset_window_state();
    }

    bool has_finish_current_partition = true;
    while (_analytor->has_output()) {
        if (_analytor->reached_limit()) {
            return Status::OK();
        }
        if (has_finish_current_partition) {
            _analytor->find_partition_end();
        }
        _analytor->find_peer_group_end();

        // We cannot evaluate if peer group end is not reached
        if (!_analytor->found_peer_group_end().first) {
            DCHECK(!_analytor->found_partition_end().first);
            break;
        }

        if (_analytor->current_row_position() < _analytor->peer_group_end()) {
            _analytor->update_window_batch(_analytor->peer_group_start(), _analytor->peer_group_end(),
                                           _analytor->peer_group_start(), _analytor->peer_group_end());
        }
        while (_analytor->current_row_position() < _analytor->peer_group_end()) {
            const auto chunk_size = static_cast<int64_t>(_analytor->current_chunk_size());

            _analytor->create_agg_result_columns(chunk_size);

            int64_t chunk_first_row_position = _analytor->first_total_position_of_current_chunk();
            // Why use current_row_position to evaluate peer_group_start_offset here?
            // Because the peer group may cross multiply chunks, we only need to update from the start of remaining part
            int64_t peer_group_start_offset =
                    _analytor->get_total_position(_analytor->current_row_position()) - chunk_first_row_position;
            int64_t peer_group_end_offset =
                    _analytor->get_total_position(_analytor->peer_group_end()) - chunk_first_row_position;
            if (peer_group_end_offset > chunk_size) {
                peer_group_end_offset = chunk_size;
            }
            _analytor->set_window_result_position(peer_group_end_offset);
            DCHECK_GE(peer_group_start_offset, 0);
            DCHECK_GT(peer_group_end_offset, peer_group_start_offset);

            _analytor->get_window_function_result(peer_group_start_offset, peer_group_end_offset);
            _analytor->update_current_row_position(peer_group_end_offset - peer_group_start_offset);

            if (_analytor->is_current_chunk_finished_eval(chunk_size)) {
                ChunkPtr chunk;
                RETURN_IF_ERROR(_analytor->output_result_chunk(&chunk));
                _analytor->offer_chunk_to_buffer(chunk);
            }
            if (_analytor->reached_limit()) {
                return Status::OK();
            }
        }

        if (_analytor->found_partition_end().first &&
            _analytor->current_row_position() == _analytor->found_partition_end().second) {
            has_finish_current_partition = true;
            _analytor->reset_state_for_next_partition();
        } else {
            has_finish_current_partition = false;
        }
    }

    return Status::OK();
}

void AnalyticSinkOperator::_process_by_partition_for_unbounded_frame(size_t chunk_size, bool is_new_partition) {
    if (is_new_partition) {
        _analytor->update_window_batch(_analytor->partition_start(), _analytor->partition_end(),
                                       _analytor->partition_start(), _analytor->partition_end());
    }

    int64_t chunk_first_row_position = _analytor->first_total_position_of_current_chunk();
    int64_t frame_start = _analytor->get_total_position(_analytor->current_row_position()) - chunk_first_row_position;
    int64_t frame_end = std::min<int64_t>(_analytor->current_row_position() + chunk_size, _analytor->partition_end());
    _analytor->set_window_result_position(
            std::min<int64_t>((_analytor->get_total_position(frame_end) - chunk_first_row_position), chunk_size));

    _analytor->get_window_function_result(frame_start, _analytor->window_result_position());
    _analytor->update_current_row_position(_analytor->window_result_position() - frame_start);
}

void AnalyticSinkOperator::_process_by_partition_for_unbounded_preceding_rows_frame_materializing(
        size_t chunk_size, bool is_new_partition) {
    while (_analytor->current_row_position() < _analytor->partition_end() &&
           !_analytor->is_current_chunk_finished_eval(chunk_size)) {
        _analytor->update_window_batch(_analytor->partition_start(), _analytor->partition_end(),
                                       _analytor->current_row_position(), _analytor->current_row_position() + 1);

        _analytor->update_window_result_position(1);
        int64_t frame_start = _analytor->get_total_position(_analytor->current_row_position()) -
                              _analytor->first_total_position_of_current_chunk();

        DCHECK_GE(frame_start, 0);
        _analytor->get_window_function_result(frame_start, _analytor->window_result_position());
        _analytor->update_current_row_position(1);
    }
}

void AnalyticSinkOperator::_process_by_partition_for_unbounded_preceding_range_frame_materializing(
        size_t chunk_size, bool is_new_partition) {
    if (_analytor->should_set_partition_size()) {
        _analytor->set_partition_size_for_function();
    }
    while (_analytor->current_row_position() < _analytor->partition_end() &&
           !_analytor->is_current_chunk_finished_eval(chunk_size)) {
        _analytor->find_peer_group_end();
        _analytor->update_window_batch(_analytor->peer_group_start(), _analytor->peer_group_end(),
                                       _analytor->peer_group_start(), _analytor->peer_group_end());

        int64_t chunk_first_row_position = _analytor->first_total_position_of_current_chunk();
        // Why use current_row_position to evaluate peer_group_start_offset here?
        // Because the peer group may cross multiply chunks, we only need to update from the start of remaining part
        int64_t peer_group_start_offset =
                _analytor->get_total_position(_analytor->current_row_position()) - chunk_first_row_position;
        int64_t peer_group_end_offset =
                _analytor->get_total_position(_analytor->peer_group_end()) - chunk_first_row_position;
        if (peer_group_end_offset > chunk_size) {
            peer_group_end_offset = chunk_size;
        }
        _analytor->set_window_result_position(peer_group_end_offset);
        DCHECK_GE(peer_group_start_offset, 0);
        DCHECK_GT(peer_group_end_offset, peer_group_start_offset);

        _analytor->get_window_function_result(peer_group_start_offset, peer_group_end_offset);
        _analytor->update_current_row_position(peer_group_end_offset - peer_group_start_offset);
    }
}

void AnalyticSinkOperator::_process_by_partition_for_sliding_frame(size_t chunk_size, bool is_new_partition) {
    if (_analytor->support_cumulative_algo()) {
        while (_analytor->current_row_position() < _analytor->partition_end() &&
               !_analytor->is_current_chunk_finished_eval(chunk_size)) {
            _analytor->update_window_batch_removable_cumulatively();

            _analytor->update_window_result_position(1);
            int64_t frame_start = _analytor->get_total_position(_analytor->current_row_position()) -
                                  _analytor->first_total_position_of_current_chunk();
            _analytor->get_window_function_result(frame_start, _analytor->window_result_position());
            _analytor->update_current_row_position(1);
        }
    } else {
        while (_analytor->current_row_position() < _analytor->partition_end() &&
               !_analytor->is_current_chunk_finished_eval(chunk_size)) {
            _analytor->reset_window_state();
            FrameRange range = _analytor->get_sliding_frame_range();
            _analytor->update_window_batch(_analytor->partition_start(), _analytor->partition_end(), range.start,
                                           range.end);

            _analytor->update_window_result_position(1);
            int64_t frame_start = _analytor->get_total_position(_analytor->current_row_position()) -
                                  _analytor->first_total_position_of_current_chunk();

            DCHECK_GE(frame_start, 0);
            _analytor->get_window_function_result(frame_start, _analytor->window_result_position());
            _analytor->update_current_row_position(1);
        }
    }
}

} // namespace starrocks::pipeline
