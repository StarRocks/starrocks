// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/analytic_node.h"

#include <algorithm>
#include <cmath>
#include <memory>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/pipeline/analysis/analytic_sink_operator.h"
#include "exec/pipeline/analysis/analytic_source_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exprs/agg/count.h"
#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

AnalyticNode::AnalyticNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _tnode(tnode),
          _result_tuple_desc(descs.get_tuple_descriptor(tnode.analytic_node.output_tuple_id)) {
    TAnalyticWindow window = tnode.analytic_node.window;
    if (!tnode.analytic_node.__isset.window) {
        _get_next = &AnalyticNode::_get_next_for_unbounded_frame;
    } else if (tnode.analytic_node.window.type == TAnalyticWindowType::RANGE) {
        // RANGE windows must have UNBOUNDED PRECEDING
        // RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING
        if (!window.__isset.window_end) {
            _get_next = &AnalyticNode::_get_next_for_unbounded_frame;
        } else {
            _get_next = &AnalyticNode::_get_next_for_unbounded_preceding_range_frame;
        }
    } else {
        if (!window.__isset.window_start && !window.__isset.window_end) {
            _get_next = &AnalyticNode::_get_next_for_unbounded_frame;
        } else if (!window.__isset.window_start && window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW) {
            _get_next = &AnalyticNode::_get_next_for_unbounded_preceding_rows_frame;
        } else {
            _get_next = &AnalyticNode::_get_next_for_sliding_frame;
        }
    }
}

Status AnalyticNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(_conjunct_ctxs.empty());

    return Status::OK();
}

Status AnalyticNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    DCHECK(child(0)->row_desc().is_prefix_of(row_desc()));

    _analytor = std::make_shared<Analytor>(_tnode, child(0)->row_desc(), _result_tuple_desc);
    RETURN_IF_ERROR(_analytor->prepare(state, _pool, mem_tracker(), expr_mem_tracker(), runtime_profile()));

    return Status::OK();
}

Status AnalyticNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));

    return _analytor->open(state);
}

Status AnalyticNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Vector query engine don't support row_batch");
}

Status AnalyticNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);

    if (_analytor->reached_limit() ||
        (_analytor->input_eos() && _analytor->output_chunk_index() == _analytor->input_chunks().size())) {
        *eos = true;
        return Status::OK();
    }

    _analytor->remove_unused_buffer_values();

    RETURN_IF_ERROR((this->*_get_next)(state, chunk, eos));
    if (*eos) {
        return Status::OK();
    }

    if (_analytor->input_rows() > 0 &&
        (_analytor->input_rows() & Analytor::memory_check_batch_size) < config::vector_chunk_size) {
        int64_t cur_memory_usage = _analytor->compute_memory_usage();
        int64_t delta_memory_usage = cur_memory_usage - _analytor->last_memory_usage();
        mem_tracker()->consume(delta_memory_usage);
        _analytor->set_last_memory_usage(cur_memory_usage);
        RETURN_IF_ERROR(state->check_query_state("Analytic Node"));
    }

    DCHECK(!(*chunk)->has_const_column());
    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

Status AnalyticNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    if (_analytor != nullptr) {
        _analytor->close(state);
    }

    return ExecNode::close(state);
}

Status AnalyticNode::_get_next_for_unbounded_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_analytor->input_eos() || _analytor->output_chunk_index() < _analytor->input_chunks().size()) {
        int64_t found_partition_end = 0;
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state, &found_partition_end));
        if (_analytor->input_eos() && _analytor->input_rows() == 0) {
            *eos = true;
            return Status::OK();
        }
        SCOPED_TIMER(_analytor->compute_timer());

        bool is_new_partition = _analytor->is_new_partition(found_partition_end);
        if (is_new_partition) {
            _analytor->reset_state_for_new_partition(found_partition_end);
        }

        size_t chunk_size = _analytor->input_chunks()[_analytor->output_chunk_index()]->num_rows();
        _analytor->create_agg_result_columns(chunk_size);

        if (is_new_partition) {
            _analytor->update_window_batch(_analytor->partition_start(), _analytor->partition_end(),
                                           _analytor->partition_start(), _analytor->partition_end());
        }

        int64_t first_chunk_row_position =
                _analytor->input_chunk_first_row_positions()[_analytor->output_chunk_index()];
        int64_t get_value_start =
                _analytor->get_total_position(_analytor->current_row_position()) - first_chunk_row_position;
        int64_t get_value_end =
                std::min<int64_t>(_analytor->current_row_position() + chunk_size, _analytor->partition_end());
        _analytor->set_window_result_position(std::min<int64_t>(
                (_analytor->get_total_position(get_value_end) - first_chunk_row_position), chunk_size));

        _analytor->get_window_function_result(get_value_start, _analytor->window_result_position());
        _analytor->update_current_row_position(_analytor->window_result_position() - get_value_start);

        if (_analytor->window_result_position() ==
            _analytor->input_chunks()[_analytor->output_chunk_index()]->num_rows()) {
            return _analytor->output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_unbounded_preceding_range_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_analytor->input_eos() || _analytor->output_chunk_index() < _analytor->input_chunks().size()) {
        int64_t found_partition_end = 0;
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state, &found_partition_end));
        if (_analytor->input_eos() && _analytor->input_rows() == 0) {
            *eos = true;
            return Status::OK();
        }

        SCOPED_TIMER(_analytor->compute_timer());

        bool is_new_partition = _analytor->is_new_partition(found_partition_end);
        if (is_new_partition) {
            _analytor->reset_state_for_new_partition(found_partition_end);
        }

        size_t chunk_size = _analytor->input_chunks()[_analytor->output_chunk_index()]->num_rows();
        _analytor->create_agg_result_columns(chunk_size);

        while (_analytor->current_row_position() < _analytor->partition_end() &&
               _analytor->window_result_position() < chunk_size) {
            if (_analytor->current_row_position() >= _analytor->peer_group_end()) {
                _analytor->find_peer_group_end();
                DCHECK_GE(_analytor->peer_group_end(), _analytor->peer_group_start());
                _analytor->update_window_batch(_analytor->peer_group_start(), _analytor->peer_group_end(),
                                               _analytor->peer_group_start(), _analytor->peer_group_end());
            }

            int64_t first_chunk_row_position =
                    _analytor->input_chunk_first_row_positions()[_analytor->output_chunk_index()];
            int64_t get_value_start =
                    _analytor->get_total_position(_analytor->current_row_position()) - first_chunk_row_position;
            _analytor->set_window_result_position(std::min<int64_t>(
                    (_analytor->get_total_position(_analytor->peer_group_end()) - first_chunk_row_position),
                    chunk_size));

            DCHECK_GE(get_value_start, 0);
            DCHECK_GT(_analytor->window_result_position(), get_value_start);

            _analytor->get_window_function_result(get_value_start, _analytor->window_result_position());
            _analytor->update_current_row_position(_analytor->window_result_position() - get_value_start);
        }

        if (_analytor->window_result_position() ==
            _analytor->input_chunks()[_analytor->output_chunk_index()]->num_rows()) {
            return _analytor->output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_sliding_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_analytor->input_eos() || _analytor->output_chunk_index() < _analytor->input_chunks().size()) {
        int64_t found_partition_end = 0;
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state, &found_partition_end));
        if (_analytor->input_eos() && _analytor->input_rows() == 0) {
            *eos = true;
            return Status::OK();
        }
        SCOPED_TIMER(_analytor->compute_timer());

        bool is_new_partition = _analytor->is_new_partition(found_partition_end);
        if (is_new_partition) {
            _analytor->reset_state_for_new_partition(found_partition_end);
        }

        size_t chunk_size = _analytor->input_chunks()[_analytor->output_chunk_index()]->num_rows();
        _analytor->create_agg_result_columns(chunk_size);

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

        if (_analytor->window_result_position() ==
            _analytor->input_chunks()[_analytor->output_chunk_index()]->num_rows()) {
            return _analytor->output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_unbounded_preceding_rows_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_analytor->input_eos() || _analytor->output_chunk_index() < _analytor->input_chunks().size()) {
        int64_t found_partition_end = 0;
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state, &found_partition_end));
        if (_analytor->input_eos() && _analytor->input_rows() == 0) {
            *eos = true;
            return Status::OK();
        }

        SCOPED_TIMER(_analytor->compute_timer());

        bool is_new_partition = _analytor->is_new_partition(found_partition_end);
        if (is_new_partition) {
            _analytor->reset_state_for_new_partition(found_partition_end);
        }

        size_t chunk_size = _analytor->input_chunks()[_analytor->output_chunk_index()]->num_rows();
        _analytor->create_agg_result_columns(chunk_size);

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

        if (_analytor->window_result_position() ==
            _analytor->input_chunks()[_analytor->output_chunk_index()]->num_rows()) {
            return _analytor->output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_try_fetch_next_partition_data(RuntimeState* state, int64_t* partition_end) {
    *partition_end = _analytor->find_partition_end();
    while (!_analytor->is_partition_finished(*partition_end)) {
        RETURN_IF_ERROR(_fetch_next_chunk(state));
        *partition_end = _analytor->find_partition_end();
    }
    return Status::OK();
}

Status AnalyticNode::_fetch_next_chunk(RuntimeState* state) {
    ChunkPtr child_chunk;
    RETURN_IF_CANCELLED(state);
    do {
        RETURN_IF_ERROR(_children[0]->get_next(state, &child_chunk, &_analytor->input_eos()));
    } while (!_analytor->input_eos() && child_chunk->is_empty());
    SCOPED_TIMER(_analytor->compute_timer());
    if (_analytor->input_eos()) {
        return Status::OK();
    }

    _analytor->input_chunk_first_row_positions().emplace_back(_analytor->input_rows());
    size_t chunk_size = child_chunk->num_rows();
    _analytor->update_input_rows(chunk_size);

    {
        for (size_t i = 0; i < _analytor->agg_fn_ctxs().size(); i++) {
            for (size_t j = 0; j < _analytor->agg_expr_ctxs()[i].size(); j++) {
                ColumnPtr column = _analytor->agg_expr_ctxs()[i][j]->evaluate(child_chunk.get());
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
            ColumnPtr column = _analytor->partition_ctxs()[i]->evaluate(child_chunk.get());
            _analytor->append_column(chunk_size, _analytor->partition_columns()[i].get(), column);
        }

        for (size_t i = 0; i < _analytor->order_ctxs().size(); i++) {
            ColumnPtr column = _analytor->order_ctxs()[i]->evaluate(child_chunk.get());
            _analytor->append_column(chunk_size, _analytor->order_columns()[i].get(), column);
        }
    }

    _analytor->input_chunks().emplace_back(std::move(child_chunk));
    return Status::OK();
}

std::vector<std::shared_ptr<pipeline::OperatorFactory> > AnalyticNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    OpFactories operators_with_sink = _children[0]->decompose_to_pipeline(context);
    context->maybe_interpolate_local_exchange(operators_with_sink);

    // shared by sink operator and source operator
    AnalytorPtr analytor = std::make_shared<Analytor>(_tnode, child(0)->row_desc(), _result_tuple_desc);

    operators_with_sink.emplace_back(
            std::make_shared<AnalyticSinkOperatorFactory>(context->next_operator_id(), id(), analytor));
    context->add_pipeline(operators_with_sink);

    OpFactories operators_with_source;
    auto source_operator = std::make_shared<AnalyticSourceOperatorFactory>(context->next_operator_id(), id(), analytor);

    // TODO(hcf) Currently, the shared data structure analytor does not support concurrency.
    // So the degree of parallism must set to 1, we'll fix it laterr
    source_operator->set_degree_of_parallelism(1);
    operators_with_source.push_back(std::move(source_operator));
    return operators_with_source;
}

} // namespace starrocks::vectorized
