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

#include "exec/analytic_node.h"

#include <algorithm>
#include <cmath>
#include <memory>

#include "column/chunk.h"
#include "exec/pipeline/analysis/analytic_sink_operator.h"
#include "exec/pipeline/analysis/analytic_source_operator.h"
#include "exec/pipeline/hash_partition_context.h"
#include "exec/pipeline/hash_partition_sink_operator.h"
#include "exec/pipeline/hash_partition_source_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exprs/agg/count.h"
#include "exprs/expr.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

AnalyticNode::AnalyticNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _tnode(tnode),
          _result_tuple_desc(descs.get_tuple_descriptor(tnode.analytic_node.output_tuple_id)) {
    TAnalyticWindow window = tnode.analytic_node.window;

    if (!tnode.analytic_node.__isset.window) {
        _get_next = &AnalyticNode::_get_next_for_unbounded_frame;
    } else if (window.type == TAnalyticWindowType::RANGE) {
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
            _get_next = &AnalyticNode::_get_next_for_rows_between_unbounded_preceding_and_current_row;
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
    RETURN_IF_ERROR(_analytor->prepare(state, _pool, runtime_profile()));

    return Status::OK();
}

Status AnalyticNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));

    return _analytor->open(state);
}

Status AnalyticNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);

    if (_analytor->reached_limit() || (_analytor->input_eos() && !_analytor->has_output())) {
        *eos = true;
        return Status::OK();
    }

    _analytor->remove_unused_buffer_values(state);

    RETURN_IF_ERROR((this->*_get_next)(state, chunk, eos));
    if (*eos) {
        return Status::OK();
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
        _analytor.reset();
    }

    return ExecNode::close(state);
}

Status AnalyticNode::_get_next_for_unbounded_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_analytor->input_eos() || _analytor->has_output()) {
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state));
        if (_analytor->input_eos() && !_analytor->has_output()) {
            *eos = true;
            return Status::OK();
        }
        DCHECK(_analytor->has_output());

        bool is_new_partition = _analytor->is_new_partition();
        if (is_new_partition) {
            _analytor->reset_state_for_cur_partition();
        }

        size_t chunk_size = _analytor->current_chunk_size();
        _analytor->create_agg_result_columns(chunk_size);

        if (is_new_partition) {
            _analytor->update_window_batch(_analytor->partition_start(), _analytor->partition_end(),
                                           _analytor->partition_start(), _analytor->partition_end());
        }

        int64_t chunk_first_row_position = _analytor->first_total_position_of_current_chunk();
        int64_t frame_start =
                _analytor->get_total_position(_analytor->current_row_position()) - chunk_first_row_position;
        int64_t frame_end =
                std::min<int64_t>(_analytor->current_row_position() + chunk_size, _analytor->partition_end());
        _analytor->set_window_result_position(
                std::min<int64_t>((_analytor->get_total_position(frame_end) - chunk_first_row_position), chunk_size));

        _analytor->get_window_function_result(frame_start, _analytor->window_result_position());
        _analytor->update_current_row_position(_analytor->window_result_position() - frame_start);

        if (_analytor->is_current_chunk_finished_eval(chunk_size)) {
            return _analytor->output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_unbounded_preceding_range_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_analytor->input_eos() || _analytor->has_output()) {
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state));
        if (_analytor->input_eos() && !_analytor->has_output()) {
            *eos = true;
            return Status::OK();
        }

        bool is_new_partition = _analytor->is_new_partition();
        if (is_new_partition) {
            _analytor->reset_state_for_cur_partition();
        }

        const size_t chunk_size = _analytor->current_chunk_size();
        _analytor->create_agg_result_columns(chunk_size);

        while (_analytor->current_row_position() < _analytor->partition_end() &&
               !_analytor->is_current_chunk_finished_eval(chunk_size)) {
            if (_analytor->current_row_position() >= _analytor->peer_group_end()) {
                // The condition `partition_start <= current_row_position < partition_end` is satisfied
                // so `partition_end` must point at the genuine partition boundary, and found_partition_end must equals
                // to partition_end if current partition haven't finished processing
                DCHECK_EQ(_analytor->partition_end(), _analytor->found_partition_end().second);
                // So the found_partition_end also points at genuine partition boundary, then we pass true to the following function
                _analytor->find_peer_group_end();
                DCHECK_GE(_analytor->peer_group_end(), _analytor->peer_group_start());
                _analytor->update_window_batch(_analytor->peer_group_start(), _analytor->peer_group_end(),
                                               _analytor->peer_group_start(), _analytor->peer_group_end());
            }

            int64_t chunk_first_row_position = _analytor->first_total_position_of_current_chunk();
            int64_t frame_start =
                    _analytor->get_total_position(_analytor->current_row_position()) - chunk_first_row_position;
            _analytor->set_window_result_position(std::min<int64_t>(
                    (_analytor->get_total_position(_analytor->peer_group_end()) - chunk_first_row_position),
                    chunk_size));

            DCHECK_GE(frame_start, 0);
            DCHECK_GT(_analytor->window_result_position(), frame_start);

            _analytor->get_window_function_result(frame_start, _analytor->window_result_position());
            _analytor->update_current_row_position(_analytor->window_result_position() - frame_start);
        }

        if (_analytor->is_current_chunk_finished_eval(chunk_size)) {
            return _analytor->output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_sliding_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_analytor->input_eos() || _analytor->has_output()) {
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state));
        if (_analytor->input_eos() && !_analytor->has_output()) {
            *eos = true;
            return Status::OK();
        }

        bool is_new_partition = _analytor->is_new_partition();
        if (is_new_partition) {
            _analytor->reset_state_for_cur_partition();
        }

        const size_t chunk_size = _analytor->current_chunk_size();
        _analytor->create_agg_result_columns(chunk_size);

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

        if (_analytor->is_current_chunk_finished_eval(chunk_size)) {
            return _analytor->output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_rows_between_unbounded_preceding_and_current_row(RuntimeState* state,
                                                                                    ChunkPtr* chunk, bool* eos) {
    RETURN_IF_ERROR(_fetch_next_chunk(state));
    if (_analytor->input_eos()) {
        *eos = true;
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

    return _analytor->output_result_chunk(chunk);
}

Status AnalyticNode::_try_fetch_next_partition_data(RuntimeState* state) {
    _analytor->find_partition_end();
    while (!_analytor->found_partition_end().first) {
        RETURN_IF_ERROR(state->check_mem_limit("analytic node fetch next partition data"));
        RETURN_IF_ERROR(_fetch_next_chunk(state));
        _analytor->find_partition_end();
    }
    return Status::OK();
}

Status AnalyticNode::_fetch_next_chunk(RuntimeState* state) {
    ChunkPtr child_chunk;
    RETURN_IF_CANCELLED(state);
    do {
        RETURN_IF_ERROR(_children[0]->get_next(state, &child_chunk, &_analytor->input_eos()));
    } while (!_analytor->input_eos() && child_chunk->is_empty());
    if (_analytor->input_eos()) {
        return Status::OK();
    }

    return _analytor->add_chunk(child_chunk);
}

pipeline::OpFactories AnalyticNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    OpFactories ops_with_sink = _children[0]->decompose_to_pipeline(context);
    auto* upstream_source_op = context->source_operator(ops_with_sink);

    if (_tnode.analytic_node.partition_exprs.empty()) {
        // analytic's dop must be 1 if with no partition clause
        ops_with_sink = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), ops_with_sink);
    } else if (_tnode.analytic_node.order_by_exprs.empty() && _tnode.analytic_node.__isset.use_hash_based_partition &&
               _tnode.analytic_node.use_hash_based_partition) {
        // analytic has only partition by columns but no order by columns
        auto pseudo_plan_node_id = context->next_pseudo_plan_node_id();
        HashPartitionContextFactoryPtr hash_partition_ctx_factory =
                std::make_shared<HashPartitionContextFactory>(_tnode.analytic_node.partition_exprs);
        ops_with_sink.emplace_back(std::make_shared<HashPartitionSinkOperatorFactory>(
                context->next_operator_id(), pseudo_plan_node_id, hash_partition_ctx_factory));
        context->add_pipeline(ops_with_sink);

        ops_with_sink.clear();
        auto hash_partition_source_op = std::make_shared<HashPartitionSourceOperatorFactory>(
                context->next_operator_id(), pseudo_plan_node_id, hash_partition_ctx_factory);
        context->inherit_upstream_source_properties(hash_partition_source_op.get(), upstream_source_op);
        ops_with_sink.push_back(std::move(hash_partition_source_op));
    }

    upstream_source_op = context->source_operator(ops_with_sink);
    auto degree_of_parallelism = upstream_source_op->degree_of_parallelism();

    AnalytorFactoryPtr analytor_factory =
            std::make_shared<AnalytorFactory>(degree_of_parallelism, _tnode, child(0)->row_desc(), _result_tuple_desc);
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));

    ops_with_sink.emplace_back(
            std::make_shared<AnalyticSinkOperatorFactory>(context->next_operator_id(), id(), _tnode, analytor_factory));
    this->init_runtime_filter_for_operator(ops_with_sink.back().get(), context, rc_rf_probe_collector);
    context->add_pipeline(ops_with_sink);

    OpFactories ops_with_source;
    auto source_op =
            std::make_shared<AnalyticSourceOperatorFactory>(context->next_operator_id(), id(), analytor_factory);
    this->init_runtime_filter_for_operator(source_op.get(), context, rc_rf_probe_collector);
    context->inherit_upstream_source_properties(source_op.get(), upstream_source_op);
    ops_with_source.push_back(std::move(source_op));

    if (limit() != -1) {
        ops_with_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    return ops_with_source;
}

} // namespace starrocks
