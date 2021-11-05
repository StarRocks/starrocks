// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "analytic_sink_operator.h"

#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status AnalyticSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    // _analytor is shared by sink operator and source operator
    // we must only prepare it at sink operator
    RETURN_IF_ERROR(_analytor->prepare(state, state->obj_pool(), get_runtime_profile()));
    RETURN_IF_ERROR(_analytor->open(state));

    TAnalyticWindow window = _tnode.analytic_node.window;

    if (!_tnode.analytic_node.__isset.window) {
        _process_by_partition = &AnalyticSinkOperator::_process_by_partition_for_unbounded_frame;
    } else if (window.type == TAnalyticWindowType::RANGE) {
        // RANGE windows must have UNBOUNDED PRECEDING
        // RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING
        if (!window.__isset.window_end) {
            _process_by_partition = &AnalyticSinkOperator::_process_by_partition_for_unbounded_frame;
        } else {
            _process_by_partition = &AnalyticSinkOperator::_process_by_partition_for_unbounded_preceding_range_frame;
        }
    } else {
        if (!window.__isset.window_start && !window.__isset.window_end) {
            _process_by_partition = &AnalyticSinkOperator::_process_by_partition_for_unbounded_frame;
        } else if (!window.__isset.window_start && window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW) {
            _process_by_partition = &AnalyticSinkOperator::_process_by_partition_for_unbounded_preceding_rows_frame;
        } else {
            _process_by_partition = &AnalyticSinkOperator::_process_by_partition_for_sliding_frame;
        }
    }

    return Status::OK();
}

bool AnalyticSinkOperator::is_finished() const {
    return _is_finished;
}

void AnalyticSinkOperator::finish(RuntimeState* state) {
    if (_is_finished) {
        return;
    }

    _is_finished = true;
    _analytor->input_eos() = true;
    _process_by_partition_if_necessary();
    _analytor->sink_complete();
}

StatusOr<vectorized::ChunkPtr> AnalyticSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status AnalyticSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _analytor->input_chunk_first_row_positions().emplace_back(_analytor->input_rows());
    size_t chunk_size = chunk->num_rows();
    _analytor->update_input_rows(chunk_size);

    for (size_t i = 0; i < _analytor->agg_fn_ctxs().size(); i++) {
        for (size_t j = 0; j < _analytor->agg_expr_ctxs()[i].size(); j++) {
            ColumnPtr column = _analytor->agg_expr_ctxs()[i][j]->evaluate(chunk.get());
            // Currently, only lead and lag window function have multi args.
            // For performance, we do this special handle.
            // In future, if need, we could remove this if else easily.
            if (j == 0) {
                _analytor->append_column(chunk_size, _analytor->agg_intput_columns()[i][j].get(), column);
            } else {
                _analytor->agg_intput_columns()[i][j]->append(*column, 0, column->size());
            }
        }
    }

    for (size_t i = 0; i < _analytor->partition_ctxs().size(); i++) {
        ColumnPtr column = _analytor->partition_ctxs()[i]->evaluate(chunk.get());
        _analytor->append_column(chunk_size, _analytor->partition_columns()[i].get(), column);
    }

    for (size_t i = 0; i < _analytor->order_ctxs().size(); i++) {
        ColumnPtr column = _analytor->order_ctxs()[i]->evaluate(chunk.get());
        _analytor->append_column(chunk_size, _analytor->order_columns()[i].get(), column);
    }

    _analytor->input_chunks().emplace_back(std::move(chunk));

    return _process_by_partition_if_necessary();
}

Status AnalyticSinkOperator::_process_by_partition_if_necessary() {
    while (_analytor->has_output()) {
        int64_t found_partition_end = _analytor->find_partition_end();

        // We only process it after all the data in a partition is reached
        if (!_analytor->is_partition_finished(found_partition_end)) {
            return Status::OK();
        }

        size_t chunk_size = _analytor->input_chunks()[_analytor->output_chunk_index()]->num_rows();
        _analytor->create_agg_result_columns(chunk_size);

        bool is_new_partition = _analytor->is_new_partition(found_partition_end);
        if (is_new_partition) {
            _analytor->reset_state_for_new_partition(found_partition_end);
        }

        (this->*_process_by_partition)(chunk_size, is_new_partition);

        // Chunk may contains multiply partitions, so the chunk need to be reprocessed
        if (_analytor->window_result_position() ==
            _analytor->input_chunks()[_analytor->output_chunk_index()]->num_rows()) {
            vectorized::ChunkPtr chunk;
            RETURN_IF_ERROR(_analytor->output_result_chunk(&chunk));
            _analytor->offer_chunk_to_buffer(chunk);
        }
    }
    return Status::OK();
}

void AnalyticSinkOperator::_process_by_partition_for_unbounded_frame(size_t chunk_size, bool is_new_partition) {
    if (is_new_partition) {
        _analytor->update_window_batch(_analytor->partition_start(), _analytor->partition_end(),
                                       _analytor->partition_start(), _analytor->partition_end());
    }

    int64_t chunk_first_row_position = _analytor->input_chunk_first_row_positions()[_analytor->output_chunk_index()];
    int64_t get_value_start =
            _analytor->get_total_position(_analytor->current_row_position()) - chunk_first_row_position;
    int64_t get_value_end =
            std::min<int64_t>(_analytor->current_row_position() + chunk_size, _analytor->partition_end());
    _analytor->set_window_result_position(
            std::min<int64_t>((_analytor->get_total_position(get_value_end) - chunk_first_row_position), chunk_size));

    _analytor->get_window_function_result(get_value_start, _analytor->window_result_position());
    _analytor->update_current_row_position(_analytor->window_result_position() - get_value_start);
}

void AnalyticSinkOperator::_process_by_partition_for_unbounded_preceding_range_frame(size_t chunk_size,
                                                                                     bool is_new_partition) {
    while (_analytor->current_row_position() < _analytor->partition_end() &&
           _analytor->window_result_position() < chunk_size) {
        if (_analytor->current_row_position() >= _analytor->peer_group_end()) {
            _analytor->find_peer_group_end();
            DCHECK_GE(_analytor->peer_group_end(), _analytor->peer_group_start());
            _analytor->update_window_batch(_analytor->peer_group_start(), _analytor->peer_group_end(),
                                           _analytor->peer_group_start(), _analytor->peer_group_end());
        }

        int64_t chunk_first_row_position =
                _analytor->input_chunk_first_row_positions()[_analytor->output_chunk_index()];
        int64_t get_value_start =
                _analytor->get_total_position(_analytor->current_row_position()) - chunk_first_row_position;
        _analytor->set_window_result_position(std::min<int64_t>(
                (_analytor->get_total_position(_analytor->peer_group_end()) - chunk_first_row_position), chunk_size));

        DCHECK_GE(get_value_start, 0);
        DCHECK_GT(_analytor->window_result_position(), get_value_start);

        _analytor->get_window_function_result(get_value_start, _analytor->window_result_position());
        _analytor->update_current_row_position(_analytor->window_result_position() - get_value_start);
    }
}

void AnalyticSinkOperator::_process_by_partition_for_unbounded_preceding_rows_frame(size_t chunk_size,
                                                                                    bool is_new_partition) {
    while (_analytor->current_row_position() < _analytor->partition_end() &&
           _analytor->window_result_position() < chunk_size) {
        _analytor->update_window_batch(_analytor->partition_start(), _analytor->partition_end(),
                                       _analytor->current_row_position(), _analytor->current_row_position() + 1);

        _analytor->update_window_result_position(1);
        int64_t frame_start = _analytor->get_total_position(_analytor->current_row_position()) -
                              _analytor->input_chunk_first_row_positions()[_analytor->output_chunk_index()];

        DCHECK_GE(frame_start, 0);
        _analytor->get_window_function_result(frame_start, _analytor->window_result_position());
        _analytor->update_current_row_position(1);
    }
}

void AnalyticSinkOperator::_process_by_partition_for_sliding_frame(size_t chunk_size, bool is_new_partition) {
    while (_analytor->current_row_position() < _analytor->partition_end() &&
           _analytor->window_result_position() < chunk_size) {
        _analytor->reset_window_state();
        FrameRange range = _analytor->get_sliding_frame_range();
        _analytor->update_window_batch(_analytor->partition_start(), _analytor->partition_end(), range.start,
                                       range.end);

        _analytor->update_window_result_position(1);
        int64_t result_start = _analytor->get_total_position(_analytor->current_row_position()) -
                               _analytor->input_chunk_first_row_positions()[_analytor->output_chunk_index()];

        DCHECK_GE(result_start, 0);
        _analytor->get_window_function_result(result_start, _analytor->window_result_position());
        _analytor->update_current_row_position(1);
    }
}

} // namespace starrocks::pipeline
