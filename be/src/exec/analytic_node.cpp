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
    _use_hash_based_partition = _tnode.analytic_node.order_by_exprs.empty() &&
                                _tnode.analytic_node.__isset.use_hash_based_partition &&
                                _tnode.analytic_node.use_hash_based_partition;
    if (_use_hash_based_partition) {
        RETURN_IF_ERROR(
                Expr::create_expr_trees(_pool, tnode.analytic_node.partition_exprs, &_hash_partition_exprs, state));
    }
    DCHECK(_conjunct_ctxs.empty());

    return Status::OK();
}

Status AnalyticNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    DCHECK(child(0)->row_desc().is_prefix_of(row_desc()));

    _analytor = std::make_shared<Analytor>(_tnode, child(0)->row_desc(), _result_tuple_desc, false);

    // Non-pipeline always use materializing mode and disable removable cumulative process
    _analytor->_need_partition_materializing = true;
    _analytor->_use_removable_cumulative_process = false;
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

    if (_analytor->reached_limit() || (_analytor->_input_eos && !_analytor->_has_output())) {
        *eos = true;
        return Status::OK();
    }

    _analytor->_remove_unused_rows(state);

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
    while (!_analytor->_input_eos || _analytor->_has_output()) {
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state));
        if (_analytor->_input_eos && !_analytor->_has_output()) {
            *eos = true;
            return Status::OK();
        }
        DCHECK(_analytor->_has_output());

        // reset state for the first partition
        if (_analytor->_get_global_position(_analytor->_current_row_position) == 0) {
            _analytor->_reset_window_state();
        }

        const size_t chunk_size = _analytor->_current_chunk_size();
        _analytor->_init_window_result_columns();

        if (_analytor->_current_row_position == _analytor->_partition.start) {
            _analytor->_update_window_batch(_analytor->_partition.start, _analytor->_partition.end,
                                            _analytor->_partition.start, _analytor->_partition.end);
        }

        int64_t base = _analytor->_first_global_position_of_current_chunk();
        int64_t start = _analytor->_get_global_position(_analytor->_current_row_position) - base;
        int64_t end = std::min<int64_t>(_analytor->_current_row_position + chunk_size, _analytor->_partition.end);
        end = std::min<int64_t>((_analytor->_get_global_position(end) - base), chunk_size);

        _analytor->_get_window_function_result(start, end);
        _analytor->_update_current_row_position(end - start);

        if (_analytor->_current_row_position == _analytor->_partition.end) {
            _analytor->_reset_state_for_next_partition();
        }

        if (_analytor->_is_current_chunk_finished_eval()) {
            return _analytor->_output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_unbounded_preceding_range_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_analytor->_input_eos || _analytor->_has_output()) {
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state));
        if (_analytor->_input_eos && !_analytor->_has_output()) {
            *eos = true;
            return Status::OK();
        }

        // reset state for the first partition
        if (_analytor->_get_global_position(_analytor->_current_row_position) == 0) {
            _analytor->_reset_window_state();
        }

        const size_t chunk_size = _analytor->_current_chunk_size();
        _analytor->_init_window_result_columns();

        while (_analytor->_current_row_position < _analytor->_partition.end &&
               !_analytor->_is_current_chunk_finished_eval()) {
            if (_analytor->_current_row_position >= _analytor->_peer_group.end) {
                // The condition `partition_start <= current_row_position < partition_end` is satisfied
                // so `partition_end` must point at the genuine partition boundary, and found_partition_end must equals
                // to partition_end if current partition haven't finished processing
                // So the found_partition_end also points at genuine partition boundary, then we pass true to the following function
                _analytor->_find_peer_group_end();
                DCHECK_GE(_analytor->_peer_group.end, _analytor->_peer_group.start);
                _analytor->_update_window_batch(_analytor->_peer_group.start, _analytor->_peer_group.end,
                                                _analytor->_peer_group.start, _analytor->_peer_group.end);
            }

            int64_t base = _analytor->_first_global_position_of_current_chunk();
            int64_t start = _analytor->_get_global_position(_analytor->_current_row_position) - base;
            int64_t end =
                    std::min<int64_t>((_analytor->_get_global_position(_analytor->_peer_group.end) - base), chunk_size);

            DCHECK_GE(start, 0);

            _analytor->_get_window_function_result(start, end);
            _analytor->_update_current_row_position(end - start);
        }

        if (_analytor->_current_row_position == _analytor->_partition.end) {
            _analytor->_reset_state_for_next_partition();
        }

        if (_analytor->_is_current_chunk_finished_eval()) {
            return _analytor->_output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_sliding_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_analytor->_input_eos || _analytor->_has_output()) {
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state));
        if (_analytor->_input_eos && !_analytor->_has_output()) {
            *eos = true;
            return Status::OK();
        }

        // reset state for the first partition
        if (_analytor->_get_global_position(_analytor->_current_row_position) == 0) {
            _analytor->_reset_window_state();
        }

        _analytor->_init_window_result_columns();

        while (_analytor->_current_row_position < _analytor->_partition.end &&
               !_analytor->_is_current_chunk_finished_eval()) {
            _analytor->_reset_window_state();
            auto range = _analytor->_get_frame_range();
            _analytor->_update_window_batch(_analytor->_partition.start, _analytor->_partition.end, range.start,
                                            range.end);
            _analytor->_get_window_function_result(_analytor->_window_result_position(),
                                                   _analytor->_window_result_position() + 1);
            _analytor->_update_current_row_position(1);
        }

        if (_analytor->_current_row_position == _analytor->_partition.end) {
            _analytor->_reset_state_for_next_partition();
        }

        if (_analytor->_is_current_chunk_finished_eval()) {
            return _analytor->_output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_rows_between_unbounded_preceding_and_current_row(RuntimeState* state,
                                                                                    ChunkPtr* chunk, bool* eos) {
    RETURN_IF_ERROR(_fetch_next_chunk(state));
    if (_analytor->_input_eos) {
        *eos = true;
        return Status::OK();
    }

    // reset state for the first partition
    if (_analytor->_get_global_position(_analytor->_current_row_position) == 0) {
        _analytor->_reset_window_state();
    }

    _analytor->_init_window_result_columns();

    do {
        _analytor->_find_partition_end();

        while (_analytor->_current_row_position < _analytor->_partition.end) {
            _analytor->_update_window_batch(_analytor->_partition.start, _analytor->_partition.end,
                                            _analytor->_current_row_position, _analytor->_current_row_position + 1);

            _analytor->_get_window_function_result(_analytor->_window_result_position(),
                                                   _analytor->_window_result_position() + 1);
            _analytor->_update_current_row_position(1);
        }

        if (_analytor->_partition.is_real) {
            _analytor->_reset_state_for_next_partition();
        }
    } while (!_analytor->_is_current_chunk_finished_eval());

    return _analytor->_output_result_chunk(chunk);
}

Status AnalyticNode::_try_fetch_next_partition_data(RuntimeState* state) {
    _analytor->_find_partition_end();
    while (!_analytor->_partition.is_real) {
        RETURN_IF_ERROR(state->check_mem_limit("analytic node fetch next partition data"));
        RETURN_IF_ERROR(_fetch_next_chunk(state));
        _analytor->_find_partition_end();
    }
    return Status::OK();
}

Status AnalyticNode::_fetch_next_chunk(RuntimeState* state) {
    ChunkPtr child_chunk;
    RETURN_IF_CANCELLED(state);
    do {
        RETURN_IF_ERROR(_children[0]->get_next(state, &child_chunk, &_analytor->_input_eos));
    } while (!_analytor->_input_eos && child_chunk->is_empty());
    if (_analytor->_input_eos) {
        return Status::OK();
    }

    return _analytor->_add_chunk(child_chunk);
}

pipeline::OpFactories AnalyticNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    OpFactories ops_with_sink = _children[0]->decompose_to_pipeline(context);
    auto* upstream_source_op = context->source_operator(ops_with_sink);

    if (_tnode.analytic_node.partition_exprs.empty()) {
        // analytic's dop must be 1 if with no partition clause
        ops_with_sink = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), ops_with_sink);
    } else if (_use_hash_based_partition) {
        // analytic has only partition by columns but no order by columns
        HashPartitionContextFactoryPtr hash_partition_ctx_factory =
                std::make_shared<HashPartitionContextFactory>(_tnode.analytic_node.partition_exprs);

        // prepend local shuffle to PartitionSortSinkOperator
        ops_with_sink = context->maybe_interpolate_local_shuffle_exchange(runtime_state(), id(), ops_with_sink,
                                                                          _hash_partition_exprs);
        upstream_source_op = context->source_operator(ops_with_sink);

        ops_with_sink.emplace_back(std::make_shared<HashPartitionSinkOperatorFactory>(context->next_operator_id(), id(),
                                                                                      hash_partition_ctx_factory));
        context->add_pipeline(ops_with_sink);

        ops_with_sink.clear();
        auto hash_partition_source_op = std::make_shared<HashPartitionSourceOperatorFactory>(
                context->next_operator_id(), id(), hash_partition_ctx_factory);
        context->inherit_upstream_source_properties(hash_partition_source_op.get(), upstream_source_op);
        ops_with_sink.push_back(std::move(hash_partition_source_op));
    }

    upstream_source_op = context->source_operator(ops_with_sink);
    auto degree_of_parallelism = upstream_source_op->degree_of_parallelism();

    AnalytorFactoryPtr analytor_factory = std::make_shared<AnalytorFactory>(
            degree_of_parallelism, _tnode, child(0)->row_desc(), _result_tuple_desc, _use_hash_based_partition);
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
