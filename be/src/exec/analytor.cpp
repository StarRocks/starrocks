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

#include "exec/analytor.h"

#include <cmath>
#include <ios>
#include <memory>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config.h"
#include "common/status.h"
#include "exprs/agg/count.h"
#include "exprs/agg/window.h"
#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/function_context.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "udf/java/utils.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"

// This macro is used to perform common pre-processing for each ProcessByPartitionIfNecessaryFunc
// 1. When set_finishing(), the has_output() may be false, so add the check here.
// 2. Reset state for the first partition. This cannot be invoded when there's no data.
#define PRE_PROCESSING()                                    \
    if (!_has_output()) {                                   \
        return Status::OK();                                \
    }                                                       \
    if (_get_global_position(_current_row_position) == 0) { \
        _reset_window_state();                              \
    }

namespace starrocks {
Status window_init_jvm_context(int64_t fid, const std::string& url, const std::string& checksum,
                               const std::string& symbol, FunctionContext* context);

Analytor::Analytor(const TPlanNode& tnode, const RowDescriptor& child_row_desc,
                   const TupleDescriptor* result_tuple_desc, bool use_hash_based_partition)
        : _tnode(tnode),
          _child_row_desc(child_row_desc),
          _result_tuple_desc(result_tuple_desc),
          _use_hash_based_partition(use_hash_based_partition) {
    if (tnode.analytic_node.__isset.buffered_tuple_id) {
        _buffered_tuple_id = tnode.analytic_node.buffered_tuple_id;
    }

    if (!config::pipeline_analytic_enable_streaming_process) {
        _need_partition_materializing = true;
    }

    TAnalyticWindow window = tnode.analytic_node.window;
    if (!tnode.analytic_node.__isset.window) {
        _need_partition_materializing = true;
    } else if (tnode.analytic_node.window.type == TAnalyticWindowType::RANGE) {
        // RANGE windows must have UNBOUNDED PRECEDING
        // RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING
        if (!window.__isset.window_start && !window.__isset.window_end) {
            _need_partition_materializing = true;
        }
        _is_unbounded_preceding = !window.__isset.window_start;
    } else {
        if (!window.__isset.window_start && !window.__isset.window_end) {
            _need_partition_materializing = true;
        } else {
            if (window.__isset.window_start) {
                TAnalyticWindowBoundary b = window.window_start;
                if (b.__isset.rows_offset_value) {
                    _rows_start_offset = b.rows_offset_value;
                    if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
                        _rows_start_offset *= -1;
                    }
                } else {
                    DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::CURRENT_ROW);
                    _rows_start_offset = 0;
                }
            }

            DCHECK(window.__isset.window_end);
            TAnalyticWindowBoundary b = window.window_end;
            if (b.__isset.rows_offset_value) {
                _rows_end_offset = b.rows_offset_value;
                if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
                    _rows_end_offset *= -1;
                }
            } else {
                DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::CURRENT_ROW);
                _rows_end_offset = 0;
            }
        }
        if (config::pipeline_analytic_enable_removable_cumulative_process) {
            _use_removable_cumulative_process = (window.__isset.window_start && window.__isset.window_end);
        }
        _is_unbounded_preceding = !window.__isset.window_start;
    }
}

Status Analytor::prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile) {
    _state = state;

    _pool = pool;
    _runtime_profile = runtime_profile;
    _limit = _tnode.limit;
    if (_tnode.analytic_node.__isset.sql_partition_keys) {
        _runtime_profile->add_info_string("PartitionKeys", _tnode.analytic_node.sql_partition_keys);
    }
    if (_tnode.analytic_node.__isset.sql_aggregate_functions) {
        _runtime_profile->add_info_string("AggregateFunctions", _tnode.analytic_node.sql_aggregate_functions);
    }
    _mem_pool = std::make_unique<MemPool>();

    const TAnalyticNode& analytic_node = _tnode.analytic_node;

    size_t agg_size = analytic_node.analytic_functions.size();
    _is_lead_lag_functions.resize(agg_size);
    _agg_fn_ctxs.resize(agg_size);
    _agg_functions.resize(agg_size);
    _agg_expr_ctxs.resize(agg_size);
    _agg_intput_columns.resize(agg_size);
    _agg_fn_types.resize(agg_size);
    _agg_states_offsets.resize(agg_size);
    _partition_size_required_function_index.resize(0);

    bool has_outer_join_child = analytic_node.__isset.has_outer_join_child && analytic_node.has_outer_join_child;

    _should_set_partition_size = false;
    for (int i = 0; i < agg_size; ++i) {
        const TExpr& desc = analytic_node.analytic_functions[i];
        const TFunction& fn = desc.nodes[0].fn;

        _agg_intput_columns[i].resize(desc.nodes[0].num_children);

        int node_idx = 0;
        for (int j = 0; j < desc.nodes[0].num_children; ++j) {
            ++node_idx;
            Expr* expr = nullptr;
            ExprContext* ctx = nullptr;
            RETURN_IF_ERROR(Expr::create_tree_from_thrift(_pool, desc.nodes, nullptr, &node_idx, &expr, &ctx, state));
            _agg_expr_ctxs[i].emplace_back(ctx);
        }

        if (fn.name.function_name == "ntile") {
            _need_partition_materializing = true;
        }

        if (_require_partition_size(fn.name.function_name)) {
            _should_set_partition_size = true;
            _partition_size_required_function_index.emplace_back(i);
            _need_partition_materializing = true;
        }

        if (!(fn.name.function_name == "sum" || fn.name.function_name == "avg" || fn.name.function_name == "count")) {
            _use_removable_cumulative_process = false;
        }

        bool is_input_nullable = false;
        if (fn.name.function_name == "count" || fn.name.function_name == "row_number" ||
            fn.name.function_name == "rank" || fn.name.function_name == "dense_rank" ||
            fn.name.function_name == "cume_dist" || fn.name.function_name == "percent_rank" ||
            fn.name.function_name == "ntile") {
            auto return_type = TYPE_BIGINT;
            if (fn.name.function_name == "cume_dist" || fn.name.function_name == "percent_rank") {
                return_type = TYPE_DOUBLE;
            }
            is_input_nullable = !fn.arg_types.empty() && (desc.nodes[0].has_nullable_child || has_outer_join_child);
            auto* func = get_window_function(fn.name.function_name, TYPE_BIGINT, return_type, is_input_nullable,
                                             fn.binary_type, state->func_version());
            _agg_functions[i] = func;
            _agg_fn_types[i] = {TypeDescriptor(return_type), false, false};
            // count(*) no input column, we manually resize it to 1 to process count(*)
            // like other agg function.
            _agg_intput_columns[i].resize(1);
        } else {
            const TypeDescriptor return_type = TypeDescriptor::from_thrift(fn.ret_type);
            const TypeDescriptor arg_type = TypeDescriptor::from_thrift(fn.arg_types[0]);

            auto return_typedesc = AnyValUtil::column_type_to_type_desc(return_type);
            // Collect arg_typedescs for aggregate function.
            std::vector<FunctionContext::TypeDesc> arg_typedescs;
            for (auto& type : fn.arg_types) {
                arg_typedescs.push_back(AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_thrift(type)));
            }

            _agg_fn_ctxs[i] = FunctionContext::create_context(state, _mem_pool.get(), return_typedesc, arg_typedescs);
            state->obj_pool()->add(_agg_fn_ctxs[i]);

            // For nullable aggregate function(sum, max, min, avg),
            // we should always use nullable aggregate function.
            is_input_nullable = true;
            const AggregateFunction* func = nullptr;
            std::string real_fn_name = fn.name.function_name;
            if (fn.ignore_nulls) {
                DCHECK(fn.name.function_name == "first_value" || fn.name.function_name == "last_value" ||
                       fn.name.function_name == "lead" || fn.name.function_name == "lag");
                // "in" means "ignore nulls", we use first_value_in/last_value_in instead of first_value/last_value
                // to find right AggregateFunction to support ignore nulls.
                real_fn_name += "_in";
                _need_partition_materializing = true;
            }
            func = get_window_function(real_fn_name, arg_type.type, return_type.type, is_input_nullable, fn.binary_type,
                                       state->func_version());
            if (func == nullptr) {
                return Status::InternalError(strings::Substitute(
                        "Invalid window function plan: ($0, $1, $2, $3, $4, $5)", real_fn_name, arg_type.type,
                        return_type.type, is_input_nullable, fn.binary_type, state->func_version()));
            }
            _agg_functions[i] = func;
            _agg_fn_types[i] = {return_type, is_input_nullable, desc.nodes[0].is_nullable};
        }

        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); ++j) {
            // We always treat first argument as non const, because most window function has only one args
            // and cant't handler const column within the function.
            if (j == 0) {
                _agg_intput_columns[i][j] =
                        ColumnHelper::create_column(_agg_expr_ctxs[i][j]->root()->type(), is_input_nullable);
            } else {
                _agg_intput_columns[i][j] = ColumnHelper::create_column(_agg_expr_ctxs[i][j]->root()->type(),
                                                                        _agg_expr_ctxs[i][j]->root()->is_nullable(),
                                                                        _agg_expr_ctxs[i][j]->root()->is_constant(), 0);
            }
        }

        DCHECK(_agg_functions[i] != nullptr);
        _is_lead_lag_functions[i] = (_agg_functions[i]->get_name() == "lead-lag");
    }

    // Compute agg state total size and offsets.
    for (int i = 0; i < agg_size; ++i) {
        _agg_states_offsets[i] = _agg_states_total_size;
        _agg_states_total_size += _agg_functions[i]->size();
        _max_agg_state_align_size = std::max(_max_agg_state_align_size, _agg_functions[i]->alignof_size());

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < _agg_fn_ctxs.size()) {
            size_t next_state_align_size = _agg_functions[i + 1]->alignof_size();
            // Extend total_size to next alignment requirement
            // Add padding by rounding up '_agg_states_total_size' to be a multiplier of next_state_align_size.
            _agg_states_total_size = (_agg_states_total_size + next_state_align_size - 1) / next_state_align_size *
                                     next_state_align_size;
        }
    }

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, analytic_node.partition_exprs, &_partition_ctxs, state));
    _partition_columns.resize(_partition_ctxs.size());
    for (size_t i = 0; i < _partition_ctxs.size(); i++) {
        _partition_columns[i] = ColumnHelper::create_column(
                _partition_ctxs[i]->root()->type(), _partition_ctxs[i]->root()->is_nullable() | has_outer_join_child,
                _partition_ctxs[i]->root()->is_constant(), 0);
    }

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, analytic_node.order_by_exprs, &_order_ctxs, state));
    _order_columns.resize(_order_ctxs.size());
    for (size_t i = 0; i < _order_ctxs.size(); i++) {
        _order_columns[i] = ColumnHelper::create_column(_order_ctxs[i]->root()->type(),
                                                        _order_ctxs[i]->root()->is_nullable() | has_outer_join_child,
                                                        _order_ctxs[i]->root()->is_constant(), 0);
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _peak_buffered_rows = ADD_PEAK_COUNTER(_runtime_profile, "PeakBufferedRows", TUnit::UNIT);
    _remove_unused_rows_cnt = ADD_COUNTER(_runtime_profile, "RemoveUnusedRowsCount", TUnit::UNIT);
    _remove_unused_total_rows = ADD_COUNTER(_runtime_profile, "RemoveUnusedTotalRows", TUnit::UNIT);
    _column_resize_timer = ADD_TIMER(_runtime_profile, "ColumnResizeTime");
    _partition_search_timer = ADD_TIMER(_runtime_profile, "PartitionSearchTime");
    _peer_group_search_timer = ADD_TIMER(_runtime_profile, "PeerGroupSearchTime");

    DCHECK_EQ(_result_tuple_desc->slots().size(), _agg_functions.size());

    for (const auto& ctx : _agg_expr_ctxs) {
        RETURN_IF_ERROR(Expr::prepare(ctx, state));
    }

    if (!_partition_ctxs.empty() || !_order_ctxs.empty()) {
        vector<TTupleId> tuple_ids;
        tuple_ids.push_back(_child_row_desc.tuple_descriptors()[0]->id());
        tuple_ids.push_back(_buffered_tuple_id);
        RowDescriptor cmp_row_desc(state->desc_tbl(), tuple_ids, vector<bool>(2, false));
        if (!_partition_ctxs.empty()) {
            RETURN_IF_ERROR(Expr::prepare(_partition_ctxs, state));
        }
        if (!_order_ctxs.empty()) {
            RETURN_IF_ERROR(Expr::prepare(_order_ctxs, state));
        }
    }

    _fns.reserve(_agg_fn_ctxs.size());
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        _fns.emplace_back(_tnode.analytic_node.analytic_functions[i].nodes[0].fn);
    }

    return _prepare_processing_mode(state, runtime_profile);
}

Status Analytor::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(Expr::open(_partition_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_order_ctxs, state));
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        RETURN_IF_ERROR(Expr::open(_agg_expr_ctxs[i], state));
        RETURN_IF_ERROR(_evaluate_const_columns(i));
    }

    _has_udaf = std::any_of(_fns.begin(), _fns.end(),
                            [](const auto& ctx) { return ctx.binary_type == TFunctionBinaryType::SRJAR; });

    auto create_fn_states = [this]() {
        for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
            if (_fns[i].binary_type == TFunctionBinaryType::SRJAR) {
                const auto& fn = _fns[i];
                auto st = window_init_jvm_context(fn.fid, fn.hdfs_location, fn.checksum, fn.aggregate_fn.symbol,
                                                  _agg_fn_ctxs[i]);
                RETURN_IF_ERROR(st);
            }
        }
        AggDataPtr agg_states = _mem_pool->allocate_aligned(_agg_states_total_size, _max_agg_state_align_size);
        _managed_fn_states.emplace_back(std::make_unique<ManagedFunctionStates>(&_agg_fn_ctxs, agg_states, this));
        return Status::OK();
    };

    if (_has_udaf) {
        auto promise_st = call_function_in_pthread(state, create_fn_states);
        RETURN_IF_ERROR(promise_st->get_future().get());
    } else {
        RETURN_IF_ERROR(create_fn_states());
    }

    return Status::OK();
}

void Analytor::close(RuntimeState* state) {
    if (_is_closed) {
        return;
    }

    while (!_buffer.empty()) {
        _buffer.pop();
    }
    _input_chunks.clear();
    _is_closed = true;

    auto agg_close = [this, state]() {
        // Note: we must free agg_states before _mem_pool free_all;
        _managed_fn_states.clear();
        _managed_fn_states.shrink_to_fit();

        if (_mem_pool != nullptr) {
            _mem_pool->free_all();
        }

        Expr::close(_order_ctxs, state);
        Expr::close(_partition_ctxs, state);
        for (const auto& i : _agg_expr_ctxs) {
            Expr::close(i, state);
        }
        return Status::OK();
    };

    if (_has_udaf) {
        auto promise_st = call_function_in_pthread(state, agg_close);
        (void)promise_st->get_future().get();
    } else {
        (void)agg_close();
    }
}

Status Analytor::process(RuntimeState* state, const ChunkPtr& chunk) {
    _remove_unused_rows(state);

    RETURN_IF_ERROR(_add_chunk(chunk));
    RETURN_IF_ERROR((this->*_process_impl)(state));

    return _check_has_error();
}

Status Analytor::finish_process(RuntimeState* state) {
    _input_eos = true;
    RETURN_IF_ERROR((this->*_process_impl)(state));
    _is_sink_complete.store(true, std::memory_order_release);
    return Status::OK();
}

std::string Analytor::debug_string() const {
    std::stringstream ss;
    ss << std::boolalpha;

    FrameRange frame = _get_frame_range();
    ss << "current_row_position=" << _get_global_position(_current_row_position) << ", partition=("
       << _get_global_position(_partition.start) << ", " << _get_global_position(_partition.end) << "/"
       << _partition.is_real << "), peer_group=(" << _get_global_position(_peer_group.start) << ", "
       << _get_global_position(_peer_group.end) << "/" << _peer_group.is_real << ")"
       << ", frame=[" << frame.start << ", " << frame.end << ")"
       << ", input_chunks_size=" << _input_chunks.size() << ", output_chunk_index=" << _output_chunk_index
       << ", removed_from_buffer_rows=" << _removed_from_buffer_rows
       << ", removed_chunk_index=" << _removed_chunk_index;

    return ss.str();
}

Status Analytor::_prepare_processing_mode(RuntimeState* state, RuntimeProfile* runtime_profile) {
    TAnalyticWindow window = _tnode.analytic_node.window;
    _process_impl = &Analytor::_materializing_process;
    std::stringstream process_mode;
    process_mode << (_need_partition_materializing ? "Materializing/" : "Streaming/");
    process_mode << (_use_removable_cumulative_process ? "RemovableCumulative"
                                                       : (_is_unbounded_preceding ? "Cumulative" : "ByDefinition"));
    runtime_profile->add_info_string("ProcessMode", process_mode.str());
    if (!_tnode.analytic_node.__isset.window) {
        _materializing_process_impl = &Analytor::_materializing_process_for_unbounded_frame;
    } else if (window.type == TAnalyticWindowType::RANGE) {
        // RANGE windows must have UNBOUNDED PRECEDING
        // RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING
        DCHECK(!window.__isset.window_start);
        DCHECK(!window.__isset.window_end || window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW);
        if (!window.__isset.window_end) {
            // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            _materializing_process_impl = &Analytor::_materializing_process_for_unbounded_frame;
        } else {
            // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            DCHECK_EQ(window.window_end.type, TAnalyticWindowBoundaryType::CURRENT_ROW);
            if (_need_partition_materializing) {
                _materializing_process_impl = &Analytor::_materializing_process_for_half_unbounded_range_frame;
            } else {
                _process_impl = &Analytor::_streaming_process_for_half_bounded_range_frame;
                _materializing_process_impl = nullptr;
            }
        }
    } else {
        if (!window.__isset.window_start && !window.__isset.window_end) {
            // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            _materializing_process_impl = &Analytor::_materializing_process_for_unbounded_frame;
        } else if (!window.__isset.window_start) {
            // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW or
            // ROWS BETWEEN UNBOUNDED PRECEDING AND N PRECEDING or
            // ROWS BETWEEN UNBOUNDED PRECEDING AND M FOLLOWING
            if (_need_partition_materializing) {
                _materializing_process_impl = &Analytor::_materializing_process_for_half_unbounded_rows_frame;
            } else {
                _process_impl = &Analytor::_streaming_process_for_half_unbounded_rows_frame;
                _materializing_process_impl = nullptr;
            }
        } else {
            // ROWS BETWEEN N PRECEDING AND M PRECEDING or
            // ROWS BETWEEN N FOLLOWING AND M FOLLOWING or
            // ROWS BETWEEN N PRECEDING AND M FOLLOWING or
            // ROWS BETWEEN N PRECEDING AND CURRENT ROW or
            // ROWS BETWEEN CURRENT ROW AND M FOLLOWING
            if (_need_partition_materializing) {
                _materializing_process_impl = &Analytor::_materializing_process_for_sliding_frame;
            } else {
                _process_impl = &Analytor::_streaming_process_for_sliding_frame;
                _materializing_process_impl = nullptr;
            }
        }
    }

    return Status::OK();
}

Status Analytor::_evaluate_const_columns(int i) {
    if (i >= _agg_fn_ctxs.size() || _agg_fn_ctxs[i] == nullptr) {
        // Only agg fn has this context.
        return Status::OK();
    }
    std::vector<ColumnPtr> const_columns;
    const_columns.reserve(_agg_expr_ctxs[i].size());
    for (auto& j : _agg_expr_ctxs[i]) {
        ASSIGN_OR_RETURN(auto col, j->root()->evaluate_const(j));
        const_columns.emplace_back(std::move(col));
    }
    _agg_fn_ctxs[i]->set_constant_columns(const_columns);
    return Status::OK();
}

Status Analytor::_check_has_error() {
    for (const auto* ctx : _agg_fn_ctxs) {
        if (ctx != nullptr) {
            if (ctx->has_error()) {
                return Status::RuntimeError(ctx->error_msg());
            }
        }
    }
    return Status::OK();
}

void Analytor::_remove_unused_rows(RuntimeState* state) {
    const size_t chunk_num = config::pipeline_analytic_removable_chunk_num;
    // Keep at least one chunk, because the process of _find_partition_end() may access the end position of
    // the last chunk.
    if (_removed_chunk_index + chunk_num + 1 >= _input_chunk_first_row_positions.size()) {
        return;
    }

    const int64_t remove_end_position = _input_chunk_first_row_positions[_removed_chunk_index + chunk_num];
    if (_need_partition_materializing) {
        if (_get_global_position(_partition.start) <= remove_end_position) {
            return;
        }
    } else if (_use_removable_cumulative_process || !_is_unbounded_preceding) {
        // Both cumulative process or sliding process need to access position around range.start
        const auto frame = _get_frame_range();
        if (_get_global_position(frame.start - 1) <= remove_end_position) {
            return;
        }
    } else {
        // Cumulative process only access the position around the frame.end
        const auto frame = _get_frame_range();
        if (_get_global_position(std::min(_current_row_position, frame.end)) <= remove_end_position) {
            return;
        }
    }

    const int64_t remove_rows = remove_end_position - _removed_from_buffer_rows;
    COUNTER_ADD(_peak_buffered_rows, -remove_rows);
    COUNTER_UPDATE(_remove_unused_rows_cnt, 1);
    COUNTER_UPDATE(_remove_unused_total_rows, remove_rows);

    {
        SCOPED_TIMER(_column_resize_timer);
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            for (size_t j = 0; j < _agg_expr_ctxs[i].size(); j++) {
                _agg_intput_columns[i][j]->remove_first_n_values(remove_rows);
            }
        }
        for (size_t i = 0; i < _partition_ctxs.size(); i++) {
            _partition_columns[i]->remove_first_n_values(remove_rows);
        }
        for (size_t i = 0; i < _order_ctxs.size(); i++) {
            _order_columns[i]->remove_first_n_values(remove_rows);
        }
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            _agg_functions[i]->reset_state_for_contraction(
                    _agg_fn_ctxs[i], _managed_fn_states[0]->mutable_data() + _agg_states_offsets[i], remove_rows);
        }
    }

    _current_row_position -= remove_rows;
    _partition.remove_first_n(remove_rows);
    _peer_group.remove_first_n(remove_rows);
    int32_t candidate_partition_end_size = _candidate_partition_ends.size();
    while (--candidate_partition_end_size >= 0) {
        auto peek = _candidate_partition_ends.front();
        _candidate_partition_ends.pop();
        _candidate_partition_ends.push(peek - remove_rows);
    }
    int32_t candidate_peer_group_end_size = _candidate_peer_group_ends.size();
    while (--candidate_peer_group_end_size >= 0) {
        auto peek = _candidate_peer_group_ends.front();
        _candidate_peer_group_ends.pop();
        _candidate_peer_group_ends.push(peek - remove_rows);
    }
    _removed_chunk_index += chunk_num;
    _removed_from_buffer_rows += remove_rows;

    DCHECK_GE(_current_row_position, 0);
    DCHECK_GE(_partition.end, 0);
}

Status Analytor::_add_chunk(const ChunkPtr& chunk) {
    DCHECK(chunk != nullptr && !chunk->is_empty());
    const size_t chunk_size = chunk->num_rows();

    {
        auto check_if_overflow = [](Column* column) {
            std::string msg;
            if (column->capacity_limit_reached(&msg)) {
                return Status::InternalError(msg);
            }
            return Status::OK();
        };
        SCOPED_TIMER(_column_resize_timer);
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            for (size_t j = 0; j < _agg_expr_ctxs[i].size(); j++) {
                ASSIGN_OR_RETURN(ColumnPtr column, _agg_expr_ctxs[i][j]->evaluate(chunk.get()));

                // When chunk's column is const, maybe need to unpack it.
                TRY_CATCH_BAD_ALLOC(_append_column(chunk_size, _agg_intput_columns[i][j].get(), column));

                RETURN_IF_ERROR(check_if_overflow(_agg_intput_columns[i][j].get()));
            }
        }

        for (size_t i = 0; i < _partition_ctxs.size(); i++) {
            ASSIGN_OR_RETURN(ColumnPtr column, _partition_ctxs[i]->evaluate(chunk.get()));
            TRY_CATCH_BAD_ALLOC(_append_column(chunk_size, _partition_columns[i].get(), column));
            RETURN_IF_ERROR(check_if_overflow(_partition_columns[i].get()));
        }

        for (size_t i = 0; i < _order_ctxs.size(); i++) {
            ASSIGN_OR_RETURN(ColumnPtr column, _order_ctxs[i]->evaluate(chunk.get()));
            TRY_CATCH_BAD_ALLOC(_append_column(chunk_size, _order_columns[i].get(), column));
            RETURN_IF_ERROR(check_if_overflow(_order_columns[i].get()));
        }
    }

    _input_chunk_first_row_positions.emplace_back(_input_rows);
    _input_rows += chunk_size;
    _input_chunks.emplace_back(chunk);
    COUNTER_ADD(_peak_buffered_rows, chunk_size);

    return Status::OK();
}

void Analytor::_append_column(size_t chunk_size, Column* dst_column, ColumnPtr& src_column) {
    DCHECK(!(src_column->is_constant() && dst_column->is_constant() && (!dst_column->empty()) &&
             (!src_column->empty()) && (src_column->compare_at(0, 0, *dst_column, 1) != 0)));
    if (src_column->only_null()) {
        static_cast<void>(dst_column->append_nulls(chunk_size));
    } else if (src_column->is_constant() && !dst_column->is_constant()) {
        // Unpack const column, then append it to dst.
        auto* const_column = down_cast<ConstColumn*>(src_column.get());
        const_column->data_column()->assign(chunk_size, 0);
        dst_column->append(*const_column->data_column(), 0, chunk_size);
    } else {
        // Most cases.
        dst_column->append(*src_column, 0, chunk_size);
    }
}

Status Analytor::_materializing_process(RuntimeState* state) {
    PRE_PROCESSING();

    while (_has_output()) {
        if (reached_limit() || state->is_cancelled()) {
            return Status::OK();
        }

        _find_partition_end();
        // Only process after all the data in a partition is reached.
        if (!_partition.is_real) {
            return Status::OK();
        }

        _init_window_result_columns();

        // Process at most one chunk
        (this->*_materializing_process_impl)(state);

        // Chunk may contains multiply partitions, so the chunk need to be reprocessed.
        if (_is_current_chunk_finished_eval()) {
            ChunkPtr chunk;
            RETURN_IF_ERROR(_output_result_chunk(&chunk));
            offer_chunk_to_buffer(chunk);
        }

        if (_current_row_position == _partition.end) {
            _reset_state_for_next_partition();
        }
    }
    return Status::OK();
}

Status Analytor::_streaming_process_for_half_unbounded_rows_frame(RuntimeState* state) {
    PRE_PROCESSING();

    do {
        if (reached_limit() || state->is_cancelled()) {
            return Status::OK();
        }

        // One iteration process one chunk, and a chunk may be processed more than once for window clause like
        // `ROWS BETWEEN UNBOUNDED PRECEDING AND M FOLLOWING`.
        auto remain_size = _current_chunk_size() - _window_result_position();
        _init_window_result_columns();
        _find_partition_end();

        while (_current_row_position < _partition.end && remain_size > 0) {
            const FrameRange frame = _get_frame_range();
            const bool is_n_following_frame = _rows_end_offset > 0;

            // For window clause like `ROWS BETWEEN UNBOUNDED PRECEDING AND M FOLLOWING`,
            // if the current chunk has not reach the partition boundary, it may need more data.
            if (is_n_following_frame && !_partition.is_real && frame.end > _partition.end) {
                return Status::OK();
            }

            // For window clause like `ROWS BETWEEN UNBOUNDED PRECEDING AND M FOLLOWING`,
            // extra update is needed for the first row.
            if (is_n_following_frame && _current_row_position == _partition.start) {
                _update_window_batch(_partition.start, _partition.end, _partition.start, frame.end - 1);
            }

            // The real frame is [frame.start, frame.end), but in streaming mode, which means the agg state will be
            // updated in acumulative manner, so the frame for current evaluation is [frame.end-1, frame.end).
            _update_window_batch(_partition.start, _partition.end, frame.end - 1, frame.end);

            _get_window_function_result(_window_result_position(), _window_result_position() + 1);
            _update_current_row_position(1);
            remain_size--;
        }

        if (_partition.is_real && _current_row_position == _partition.end) {
            _reset_state_for_next_partition();
        }

        if (_is_current_chunk_finished_eval()) {
            ChunkPtr chunk;
            RETURN_IF_ERROR(_output_result_chunk(&chunk));
            offer_chunk_to_buffer(chunk);
        }
    } while (_has_output());

    return Status::OK();
}

Status Analytor::_streaming_process_for_half_bounded_range_frame(RuntimeState* state) {
    PRE_PROCESSING();

    bool has_finish_current_partition = true;
    while (_has_output()) {
        if (reached_limit() || state->is_cancelled()) {
            return Status::OK();
        }
        if (has_finish_current_partition) {
            _find_partition_end();
        }
        _find_peer_group_end();

        // We cannot evaluate if peer group end is not reached.
        if (!_peer_group.is_real) {
            DCHECK(!_partition.is_real);
            break;
        }

        if (_current_row_position < _peer_group.end) {
            // The real frame is [_partition_start, _peer_group_end), but in streaming mode, which means the agg state will be
            // updated in acumulative manner, so the frame for current evaluation is [_peer_group_start, _peer_group_end).
            _update_window_batch(_peer_group.start, _peer_group.end, _peer_group.start, _peer_group.end);
        }
        while (_current_row_position < _peer_group.end) {
            const auto chunk_size = static_cast<int64_t>(_current_chunk_size());

            _init_window_result_columns();

            int64_t base = _first_global_position_of_current_chunk();
            // Why use current_row_position to evaluate start here?
            // Because the peer group may cross multiply chunks, we only need to update from the start of remaining part.
            int64_t start = _get_global_position(_current_row_position) - base;
            int64_t end = _get_global_position(_peer_group.end) - base;
            if (end > chunk_size) {
                end = chunk_size;
            }
            DCHECK_GE(start, 0);
            DCHECK_GT(end, start);

            _get_window_function_result(start, end);
            _update_current_row_position(end - start);

            if (_is_current_chunk_finished_eval()) {
                ChunkPtr chunk;
                RETURN_IF_ERROR(_output_result_chunk(&chunk));
                offer_chunk_to_buffer(chunk);
            }
            if (reached_limit() || state->is_cancelled()) {
                return Status::OK();
            }
        }

        if (_partition.is_real && _current_row_position == _partition.end) {
            has_finish_current_partition = true;
            _reset_state_for_next_partition();
        } else {
            has_finish_current_partition = false;
        }
    }

    return Status::OK();
}

Status Analytor::_streaming_process_for_sliding_frame(RuntimeState* state) {
    PRE_PROCESSING();

    do {
        if (reached_limit() || state->is_cancelled()) {
            return Status::OK();
        }

        // One iteration process one chunk, and a chunk may be processed more than once for window clause like
        // `ROWS BETWEEN N PRECEDING AND M FOLLOWING`.
        auto remain_size = _current_chunk_size() - _window_result_position();
        _init_window_result_columns();
        _find_partition_end();

        while (_current_row_position < _partition.end && remain_size > 0) {
            const FrameRange frame = _get_frame_range();
            const bool is_n_following_frame = _rows_end_offset > 0;

            // For window clause like `ROWS BETWEEN N PRECEDING AND M FOLLOWING`,
            // if the current chunk has not reach the partition boundary, it may need more data.
            if (is_n_following_frame && !_partition.is_real && frame.end > _partition.end) {
                return Status::OK();
            }

            if (_use_removable_cumulative_process) {
                _update_window_batch_removable_cumulatively();
            } else {
                // Update agg state in batch manner for each row.
                _reset_window_state();
                const FrameRange range = _get_frame_range();
                _update_window_batch(_partition.start, _partition.end, range.start, range.end);
            }

            _get_window_function_result(_window_result_position(), _window_result_position() + 1);
            _update_current_row_position(1);
            remain_size--;
        }

        if (_partition.is_real && _current_row_position == _partition.end) {
            _reset_state_for_next_partition();
        }

        if (_is_current_chunk_finished_eval()) {
            ChunkPtr chunk;
            RETURN_IF_ERROR(_output_result_chunk(&chunk));
            offer_chunk_to_buffer(chunk);
        }
    } while (_has_output());

    return Status::OK();
}

void Analytor::_materializing_process_for_unbounded_frame(RuntimeState* state) {
    if (_current_row_position == _partition.start) {
        // Update agg state in batch manner.
        _update_window_batch(_partition.start, _partition.end, _partition.start, _partition.end);
    }
    const auto chunk_size = _current_chunk_size();
    int64_t base = _first_global_position_of_current_chunk();
    int64_t start = _get_global_position(_current_row_position) - base;
    int64_t end = std::min<int64_t>(_current_row_position + chunk_size, _partition.end);
    end = std::min<int64_t>((_get_global_position(end) - base), chunk_size);

    _get_window_function_result(start, end);
    _update_current_row_position(end - start);
}

void Analytor::_materializing_process_for_half_unbounded_rows_frame(RuntimeState* state) {
    while (_current_row_position < _partition.end && !_is_current_chunk_finished_eval()) {
        const FrameRange frame = _get_frame_range();
        const bool is_n_following_frame = _rows_end_offset > 0;

        // For window clause like `ROWS BETWEEN UNBOUNDED PRECEDING AND M FOLLOWING`,
        // extra update is needed for the first row.
        if (is_n_following_frame && _current_row_position == _partition.start) {
            _update_window_batch(_partition.start, _partition.end, _partition.start, frame.end - 1);
        }

        // The real frame is [frame.start, frame.end), but in streaming mode, which means the agg state will be
        // updated in acumulative manner, so the frame for current evaluation is [frame.end-1, frame.end).
        _update_window_batch(_partition.start, _partition.end, frame.end - 1, frame.end);

        _get_window_function_result(_window_result_position(), _window_result_position() + 1);
        _update_current_row_position(1);
    }
}

void Analytor::_materializing_process_for_half_unbounded_range_frame(RuntimeState* state) {
    if (_should_set_partition_size) {
        _set_partition_size_for_function();
    }
    const auto chunk_size = _current_chunk_size();
    while (_current_row_position < _partition.end && !_is_current_chunk_finished_eval()) {
        _find_peer_group_end();
        DCHECK(_peer_group.is_real);
        // One peer group may across multiply chunks, so we need to avoid duplicate update.
        if (_current_row_position == _peer_group.start) {
            // The real frame is [_partition_start, _peer_group_end), but the agg state will be updated in
            // acumulative manner, so the frame for current evaluation is [_peer_group_start, _peer_group_end).
            _update_window_batch(_peer_group.start, _peer_group.end, _peer_group.start, _peer_group.end);
        }

        int64_t base = _first_global_position_of_current_chunk();
        // Why use current_row_position to evaluate start here?
        // Because the peer group may cross multiply chunks, we only need to update from the start of remaining part.
        int64_t start = _get_global_position(_current_row_position) - base;
        int64_t end = _get_global_position(_peer_group.end) - base;
        if (end > chunk_size) {
            end = chunk_size;
        }
        DCHECK_GE(start, 0);
        DCHECK_GT(end, start);

        _get_window_function_result(start, end);
        _update_current_row_position(end - start);
    }
}

void Analytor::_materializing_process_for_sliding_frame(RuntimeState* state) {
    if (_use_removable_cumulative_process) {
        while (_current_row_position < _partition.end && !_is_current_chunk_finished_eval()) {
            _update_window_batch_removable_cumulatively();

            _get_window_function_result(_window_result_position(), _window_result_position() + 1);
            _update_current_row_position(1);
        }
    } else {
        while (_current_row_position < _partition.end && !_is_current_chunk_finished_eval()) {
            // Update agg state in batch manner for each row.
            _reset_window_state();
            const FrameRange range = _get_frame_range();
            _update_window_batch(_partition.start, _partition.end, range.start, range.end);

            _get_window_function_result(_window_result_position(), _window_result_position() + 1);
            _update_current_row_position(1);
        }
    }
}

void Analytor::_update_window_batch(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                    int64_t frame_end) {
    // DO NOT put timer here because this function will be used frequently,
    // timer will cause a sharp drop in performance.
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        size_t column_size = _agg_intput_columns[i].size();
        const Column* data_columns[column_size];
        for (size_t j = 0; j < column_size; j++) {
            data_columns[j] = _agg_intput_columns[i][j].get();
        }

        // For lead/lag function, it uses the relationship between the frame_start and frame_end to determine
        // whether NULL value should be generated, so the frame should not be normalized.
        if (!_is_lead_lag_functions[i]) {
            frame_start = std::max<int64_t>(frame_start, _partition.start);
            // For half unounded window, we have not found the partition end, _found_partition_end.second refers to the next position.
            // And for others, _found_partition_end.second is identical to _partition.end, so we can always use _found_partition_end
            // instead of _partition.end to refer to the current right boundary.
            frame_end = std::min<int64_t>(frame_end, _partition.end);
        }
        _agg_functions[i]->update_batch_single_state_with_frame(
                _agg_fn_ctxs[i], _managed_fn_states[0]->mutable_data() + _agg_states_offsets[i], data_columns,
                partition_start, partition_end, frame_start, frame_end);
    }
}

void Analytor::_update_window_batch_removable_cumulatively() {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        const Column* agg_column = _agg_intput_columns[i][0].get();
        _agg_functions[i]->update_state_removable_cumulatively(
                _agg_fn_ctxs[i], _managed_fn_states[0]->mutable_data() + _agg_states_offsets[i], &agg_column,
                _current_row_position, _partition.start, _partition.end, _rows_start_offset, _rows_end_offset, false,
                false);
    }
}

Status Analytor::_output_result_chunk(ChunkPtr* chunk) {
    ChunkPtr output_chunk = std::move(_input_chunks[_output_chunk_index]);
    for (size_t i = 0; i < _result_window_columns.size(); i++) {
        output_chunk->append_column(_result_window_columns[i], _result_tuple_desc->slots()[i]->id());
    }

    _num_rows_returned += output_chunk->num_rows();

    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        output_chunk->set_num_rows(output_chunk->num_rows() - num_rows_over);
        *chunk = output_chunk;
        _output_chunk_index++;
        return Status::OK();
    }

    *chunk = output_chunk;
    _output_chunk_index++;
    return Status::OK();
}

void Analytor::_reset_state_for_next_partition() {
    _partition_statistics.update(_partition.end - _partition.start);
    _peer_group_statistics.reset();

    _partition.start = _partition.end;
    _current_row_position = _partition.start;
    _reset_window_state();
    DCHECK_GE(_current_row_position, 0);
}

void Analytor::_reset_window_state() {
    // DO NOT put timer here because this function will be used frequently,
    // timer will cause a sharp drop in performance.
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->reset(_agg_fn_ctxs[i], _agg_intput_columns[i],
                                 _managed_fn_states[0]->mutable_data() + _agg_states_offsets[i]);
    }
}

void Analytor::_init_window_result_columns() {
    if (_window_result_position() != 0) {
        return;
    }
    const auto chunk_size = _current_chunk_size();
    _result_window_columns.resize(_agg_fn_types.size());
    for (size_t i = 0; i < _agg_fn_types.size(); ++i) {
        _result_window_columns[i] =
                ColumnHelper::create_column(_agg_fn_types[i].result_type, _agg_fn_types[i].has_nullable_child);
        // Binary column cound't call resize method like Numeric Column,
        // so we only reserve it.
        if (_agg_fn_types[i].result_type.type == LogicalType::TYPE_CHAR ||
            _agg_fn_types[i].result_type.type == LogicalType::TYPE_VARCHAR ||
            _agg_fn_types[i].result_type.type == LogicalType::TYPE_JSON ||
            _agg_fn_types[i].result_type.type == LogicalType::TYPE_ARRAY ||
            _agg_fn_types[i].result_type.type == LogicalType::TYPE_MAP ||
            _agg_fn_types[i].result_type.type == LogicalType::TYPE_STRUCT) {
            _result_window_columns[i]->reserve(chunk_size);
        } else {
            _result_window_columns[i]->resize(chunk_size);
        }
    }
}

void Analytor::_find_partition_end() {
    // Current partition data don't consume finished.
    if (_partition.is_real && _current_row_position < _partition.end) {
        return;
    }

    if (_partition_columns.empty() || _input_rows == 0) {
        _partition.end = (_input_rows - _removed_from_buffer_rows);
        _partition.is_real = _input_eos;
        return;
    }

    while (!_candidate_partition_ends.empty()) {
        int64_t peek = _candidate_partition_ends.front();
        _candidate_partition_ends.pop();
        if (peek > _partition.end) {
            _partition.end = peek;
            _partition.is_real = true;
            return;
        }
    }

    const int64_t start = _partition.end;
    const int64_t target = (_partition.is_real || _partition.end == 0) ? _partition.end : _partition.end - 1;
    _partition.end = static_cast<int64_t>(_partition_columns[0]->size());
    {
        SCOPED_TIMER(_partition_search_timer);
        if (start < _partition.end) {
            if (_use_hash_based_partition) {
                _partition.end = _find_first_not_equal_for_hash_based_partition(target, start, _partition.end);
            } else {
                for (auto& column : _partition_columns) {
                    _partition.end = _find_first_not_equal(column.get(), target, start, _partition.end);
                }
            }
        }
    }

    if (_partition.end < static_cast<int64_t>(_partition_columns[0]->size())) {
        _partition.is_real = true;
        _find_candidate_partition_ends();
        return;
    }

    // Genuine partition end may be existed in the incoming chunks if _input_eos = false.
    DCHECK_EQ(_partition.end, _partition_columns[0]->size());
    _partition.is_real = _input_eos;
}

void Analytor::_find_peer_group_end() {
    // Current peer group data don't output finished.
    if (_peer_group.is_real && _current_row_position < _peer_group.end) {
        return;
    }

    while (!_candidate_peer_group_ends.empty()) {
        int64_t peek = _candidate_peer_group_ends.front();
        _candidate_peer_group_ends.pop();
        if (peek > _peer_group.end) {
            _peer_group.start = _peer_group.end;
            _peer_group.end = peek;
            _peer_group.is_real = true;

            _peer_group_statistics.update(_peer_group.end - _peer_group.start);
            return;
        }
    }

    if (_peer_group.is_real) {
        _peer_group.start = _peer_group.end;
    }
    _peer_group.end = _partition.end;
    DCHECK(!_order_columns.empty());

    {
        SCOPED_TIMER(_peer_group_search_timer);
        if (_peer_group.start < _peer_group.end) {
            for (auto& column : _order_columns) {
                _peer_group.end =
                        _find_first_not_equal(column.get(), _peer_group.start, _peer_group.start, _peer_group.end);
            }
        }
    }

    if (_peer_group.end < _partition.end) {
        _peer_group_statistics.update(_peer_group.end - _peer_group.start);
        _peer_group.is_real = true;
        _find_candidate_peer_group_ends();
        return;
    }

    DCHECK_EQ(_peer_group.end, _partition.end);
    if (_partition.is_real) {
        // _found_peer_group_end is the genuine partition boundary.
        _peer_group.is_real = true;
        return;
    }

    _peer_group.is_real = false;
}

int64_t Analytor::_find_first_not_equal(Column* column, int64_t target, int64_t start, int64_t end) {
    while (start + 1 < end) {
        int64_t mid = start + (end - start) / 2;
        if (column->compare_at(target, mid, *column, 1) == 0) {
            start = mid;
        } else {
            end = mid;
        }
    }
    if (column->compare_at(target, end - 1, *column, 1) == 0) {
        return end;
    }
    return end - 1;
}

int64_t Analytor::_find_first_not_equal_for_hash_based_partition(int64_t target, int64_t start, int64_t end) {
    // In this case, we cannot compare each column one by one like Analytor::_find_first_not_equal does,
    // and we must compare all the partition columns for one comparation.
    auto compare = [this](size_t left, size_t right) {
        for (auto& column : _partition_columns) {
            auto res = column->compare_at(left, right, *column, 1);
            if (res != 0) {
                return res;
            }
        }
        return 0;
    };
    while (start + 1 < end) {
        int64_t mid = start + (end - start) / 2;
        if (compare(target, mid) == 0) {
            start = mid;
        } else {
            end = mid;
        }
    }
    if (compare(target, end - 1) == 0) {
        return end;
    }
    return end - 1;
}

void Analytor::_find_candidate_partition_ends() {
    if (!_partition_statistics.is_high_cardinality()) {
        return;
    }

    SCOPED_TIMER(_partition_search_timer);
    for (size_t i = _partition.end + 1; i < _partition_columns[0]->size(); ++i) {
        for (auto& column : _partition_columns) {
            auto cmp = column->compare_at(i - 1, i, *column, 1);
            if (cmp != 0) {
                _candidate_partition_ends.push(i);
                break;
            }
        }
    }
}

void Analytor::_find_candidate_peer_group_ends() {
    if (!_peer_group_statistics.is_high_cardinality()) {
        return;
    }

    SCOPED_TIMER(_peer_group_search_timer);
    for (size_t i = _peer_group.end + 1; i < _partition.end; ++i) {
        for (auto& column : _order_columns) {
            auto cmp = column->compare_at(i - 1, i, *column, 1);
            if (cmp != 0) {
                _candidate_peer_group_ends.push(i);
                break;
            }
        }
    }
}

void Analytor::_get_window_function_result(size_t frame_start, size_t frame_end) {
    // DO NOT put timer here because this function will be used frequently,
    // timer will cause a sharp drop in performance.
    DCHECK_GT(frame_end, frame_start);
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        Column* agg_column = _result_window_columns[i].get();
        _agg_functions[i]->get_values(_agg_fn_ctxs[i], _managed_fn_states[0]->data() + _agg_states_offsets[i],
                                      agg_column, frame_start, frame_end);
    }
}

void Analytor::_set_partition_size_for_function() {
    for (auto i : _partition_size_required_function_index) {
        auto& state = *reinterpret_cast<CumeDistState*>(_managed_fn_states[0]->mutable_data() + _agg_states_offsets[i]);
        state.count = _partition.end - _partition.start;
    }
}

AnalytorPtr AnalytorFactory::create(int i) {
    if (!_analytors[i]) {
        _analytors[i] =
                std::make_shared<Analytor>(_tnode, _child_row_desc, _result_tuple_desc, _use_hash_based_partition);
    }
    return _analytors[i];
}
} // namespace starrocks
