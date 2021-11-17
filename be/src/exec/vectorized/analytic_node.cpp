// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/analytic_node.h"

#include <algorithm>
#include <cmath>
#include <memory>

#include "column/chunk.h"
#include "column/column_helper.h"
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
          _result_tuple_desc(descs.get_tuple_descriptor(tnode.analytic_node.output_tuple_id)) {
    if (tnode.analytic_node.__isset.buffered_tuple_id) {
        _buffered_tuple_id = tnode.analytic_node.buffered_tuple_id;
    }

    TAnalyticWindow window = tnode.analytic_node.window;
    FrameType frame_type = FrameType::Unbounded;
    if (!tnode.analytic_node.__isset.window) {
        _get_next = &AnalyticNode::_get_next_for_unbounded_frame;
    } else if (tnode.analytic_node.window.type == TAnalyticWindowType::RANGE) {
        // RANGE windows must have UNBOUNDED PRECEDING
        // RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING
        if (!window.__isset.window_end) {
            frame_type = FrameType::Unbounded;
            _get_next = &AnalyticNode::_get_next_for_unbounded_frame;
        } else {
            frame_type = FrameType::UnboundedPrecedingRange;
            _get_next = &AnalyticNode::_get_next_for_unbounded_preceding_range_frame;
        }
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

        if (window.__isset.window_end) {
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

        if (!window.__isset.window_start && !window.__isset.window_end) {
            frame_type = FrameType::Unbounded;
            _get_next = &AnalyticNode::_get_next_for_unbounded_frame;
        } else if (!window.__isset.window_start && window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW) {
            frame_type = FrameType::UnboundedPrecedingRows;
            _get_next = &AnalyticNode::_get_next_for_unbounded_preceding_rows_frame;
        } else {
            frame_type = FrameType::Sliding;
            _get_next = &AnalyticNode::_get_next_for_sliding_frame;
            if (!window.__isset.window_start) {
                _get_sliding_frame_range = &AnalyticNode::_get_sliding_frame_range_no_start;
            } else {
                _get_sliding_frame_range = &AnalyticNode::_get_sliding_frame_range_with_start;
            }
        }
    }

    VLOG_ROW << "frame_type " << frame_type << " _rows_start_offset " << _rows_start_offset << " "
             << " _rows_end_offset " << _rows_end_offset;
}

Status AnalyticNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(_conjunct_ctxs.empty());
    const TAnalyticNode& analytic_node = tnode.analytic_node;

    size_t agg_size = analytic_node.analytic_functions.size();
    _agg_fn_ctxs.resize(agg_size);
    _agg_functions.resize(agg_size);
    _agg_expr_ctxs.resize(agg_size);
    _agg_intput_columns.resize(agg_size);
    _agg_fn_types.resize(agg_size);
    _agg_states_offsets.resize(agg_size);

    bool has_outer_join_child = analytic_node.__isset.has_outer_join_child && analytic_node.has_outer_join_child;

    bool has_lead_lag_function = false;
    for (int i = 0; i < agg_size; ++i) {
        const TExpr& desc = analytic_node.analytic_functions[i];
        const TFunction& fn = desc.nodes[0].fn;
        VLOG_ROW << fn.name.function_name << " is arg nullable " << desc.nodes[0].has_nullable_child;
        VLOG_ROW << fn.name.function_name << " is result nullable " << desc.nodes[0].is_nullable;

        _agg_intput_columns[i].resize(desc.nodes[0].num_children);

        int node_idx = 0;
        for (int j = 0; j < desc.nodes[0].num_children; ++j) {
            ++node_idx;
            Expr* expr = nullptr;
            ExprContext* ctx = nullptr;
            RETURN_IF_ERROR(Expr::create_tree_from_thrift(_pool, desc.nodes, nullptr, &node_idx, &expr, &ctx));
            _agg_expr_ctxs[i].emplace_back(ctx);
        }

        bool is_input_nullable = false;
        if (fn.name.function_name == "count" || fn.name.function_name == "row_number" ||
            fn.name.function_name == "rank" || fn.name.function_name == "dense_rank") {
            is_input_nullable = !fn.arg_types.empty() && (desc.nodes[0].has_nullable_child || has_outer_join_child);
            auto* func = get_aggregate_function(fn.name.function_name, TYPE_BIGINT, TYPE_BIGINT, is_input_nullable);
            _agg_functions[i] = func;
            _agg_fn_types[i] = {TypeDescriptor(TYPE_BIGINT), false, false};
            // count(*) no input column, we manually resize it to 1 to process count(*)
            // like other agg function.
            _agg_intput_columns[i].resize(1);
        } else {
            const TypeDescriptor return_type = TypeDescriptor::from_thrift(fn.ret_type);
            const TypeDescriptor arg_type = TypeDescriptor::from_thrift(fn.arg_types[0]);

            auto return_typedesc = AnyValUtil::column_type_to_type_desc(return_type);
            // collect arg_typedescs for aggregate function.
            std::vector<FunctionContext::TypeDesc> arg_typedescs;
            for (auto& type : fn.arg_types) {
                arg_typedescs.push_back(AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_thrift(type)));
            }

            _agg_fn_ctxs[i] = FunctionContextImpl::create_context(state, _mem_pool.get(), return_typedesc,
                                                                  arg_typedescs, 0, false);
            state->obj_pool()->add(_agg_fn_ctxs[i]);

            // For nullable aggregate function(sum, max, min, avg),
            // we should always use nullable aggregate function.
            is_input_nullable = true;
            VLOG_ROW << "try get function " << fn.name.function_name << " arg_type.type " << arg_type.type
                     << " return_type.type " << return_type.type;
            auto* func =
                    get_aggregate_function(fn.name.function_name, arg_type.type, return_type.type, is_input_nullable);
            if (func == nullptr) {
                return Status::InternalError(
                        strings::Substitute("Invalid window function plan: $0", fn.name.function_name));
            }
            _agg_functions[i] = func;
            _agg_fn_types[i] = {return_type, is_input_nullable, desc.nodes[0].is_nullable};
        }

        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); ++j) {
            // Currently, only lead and lag window function have multi args.
            // For performance, we do this special handle.
            // In future, if need, we could remove this if else easily.
            if (j == 0) {
                _agg_intput_columns[i][j] =
                        ColumnHelper::create_column(_agg_expr_ctxs[i][j]->root()->type(), is_input_nullable);
            } else {
                _agg_intput_columns[i][j] = ColumnHelper::create_column(_agg_expr_ctxs[i][j]->root()->type(),
                                                                        _agg_expr_ctxs[i][j]->root()->is_nullable(),
                                                                        _agg_expr_ctxs[i][j]->root()->is_constant(), 0);
            }
            _agg_intput_columns[i][j]->reserve(config::vector_chunk_size * BUFFER_CHUNK_NUMBER);
        }

        DCHECK(_agg_functions[i] != nullptr);
        VLOG_ROW << "get agg function " << _agg_functions[i]->get_name();
        if (_agg_functions[i]->get_name() == "lead-lag") {
            has_lead_lag_function = true;
        }
    }

    if (has_lead_lag_function) {
        _update_window_batch = &AnalyticNode::_update_window_batch_lead_lag;
    } else {
        _update_window_batch = &AnalyticNode::_update_window_batch_normal;
    }

    // compute agg state total size and offsets
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

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, analytic_node.partition_exprs, &_partition_ctxs));
    _partition_columns.resize(_partition_ctxs.size());
    for (size_t i = 0; i < _partition_ctxs.size(); i++) {
        _partition_columns[i] = ColumnHelper::create_column(
                _partition_ctxs[i]->root()->type(), _partition_ctxs[i]->root()->is_nullable() | has_outer_join_child,
                _partition_ctxs[i]->root()->is_constant(), 0);
        _partition_columns[i]->reserve(config::vector_chunk_size * BUFFER_CHUNK_NUMBER);
    }

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, analytic_node.order_by_exprs, &_order_ctxs));
    _order_columns.resize(_order_ctxs.size());
    for (size_t i = 0; i < _order_ctxs.size(); i++) {
        _order_columns[i] = ColumnHelper::create_column(_order_ctxs[i]->root()->type(),
                                                        _order_ctxs[i]->root()->is_nullable() | has_outer_join_child,
                                                        _order_ctxs[i]->root()->is_constant(), 0);
        _order_columns[i]->reserve(config::vector_chunk_size * BUFFER_CHUNK_NUMBER);
    }

    return Status::OK();
}

Status AnalyticNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    DCHECK(child(0)->row_desc().is_prefix_of(row_desc()));
    _mem_pool.reset(new MemPool(mem_tracker()));

    _compute_timer = ADD_TIMER(runtime_profile(), "ComputeTime");
    DCHECK_EQ(_result_tuple_desc->slots().size(), _agg_functions.size());

    SCOPED_TIMER(_compute_timer);
    for (const auto& ctx : _agg_expr_ctxs) {
        Expr::prepare(ctx, state, child(0)->row_desc(), expr_mem_tracker());
    }

    if (!_partition_ctxs.empty() || !_order_ctxs.empty()) {
        vector<TTupleId> tuple_ids;
        tuple_ids.push_back(child(0)->row_desc().tuple_descriptors()[0]->id());
        tuple_ids.push_back(_buffered_tuple_id);
        RowDescriptor cmp_row_desc(state->desc_tbl(), tuple_ids, vector<bool>(2, false));
        if (!_partition_ctxs.empty()) {
            RETURN_IF_ERROR(Expr::prepare(_partition_ctxs, state, cmp_row_desc, expr_mem_tracker()));
        }
        if (!_order_ctxs.empty()) {
            RETURN_IF_ERROR(Expr::prepare(_order_ctxs, state, cmp_row_desc, expr_mem_tracker()));
        }
    }

    AggDataPtr agg_states = _mem_pool->allocate_aligned(_agg_states_total_size, _max_agg_state_align_size);
    _managed_fn_states.emplace_back(std::make_unique<ManagedFunctionStates>(agg_states, this));

    return Status::OK();
}

Status AnalyticNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));
    RETURN_IF_ERROR(Expr::open(_partition_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_order_ctxs, state));
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        RETURN_IF_ERROR(Expr::open(_agg_expr_ctxs[i], state));
    }
    return Status::OK();
}

Status AnalyticNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Vector query engine don't support row_batch");
}

Status AnalyticNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);

    if (reached_limit() || (_input_eos && _output_chunk_index == _input_chunks.size())) {
        *eos = true;
        return Status::OK();
    }

    _remove_unused_buffer_values();

    RETURN_IF_ERROR((this->*_get_next)(state, chunk, eos));
    if (*eos) {
        return Status::OK();
    }

    if (_input_rows > 0 && (_input_rows & memory_check_batch_size) < config::vector_chunk_size) {
        int64_t cur_memory_usage = _compute_memory_usage();
        int64_t delta_memory_usage = cur_memory_usage - _last_memory_usage;
        mem_tracker()->consume(delta_memory_usage);
        _last_memory_usage = cur_memory_usage;
        RETURN_IF_ERROR(state->check_query_state("Analytic Node"));
    }

    DCHECK(!(*chunk)->has_const_column());
    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

size_t AnalyticNode::_compute_memory_usage() {
    size_t memory_usage = 0;
    for (size_t i = 0; i < _partition_columns.size(); ++i) {
        memory_usage += _partition_columns[i]->memory_usage();
    }

    for (size_t i = 0; i < _order_columns.size(); ++i) {
        memory_usage += _order_columns[i]->memory_usage();
    }

    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); j++) {
            memory_usage += _agg_intput_columns[i][j]->memory_usage();
        }
    }
    return memory_usage;
}

Status AnalyticNode::_get_next_for_unbounded_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_input_eos || _output_chunk_index < _input_chunks.size()) {
        int64_t found_partition_end = 0;
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state, &found_partition_end));
        if (_input_eos && _input_rows == 0) {
            *eos = true;
            return Status::OK();
        }
        SCOPED_TIMER(_compute_timer);

        bool is_new_partition = _is_new_partition(found_partition_end);
        if (is_new_partition) {
            _reset_state_for_new_partition(found_partition_end);
        }

        size_t chunk_size = _input_chunks[_output_chunk_index]->num_rows();
        _create_agg_result_columns(chunk_size);

        if (is_new_partition) {
            (this->*_update_window_batch)(_partition_start, _partition_end, _partition_start, _partition_end);
        }

        int64_t first_chunk_row_position = input_chunk_first_row_positions[_output_chunk_index];
        int64_t get_value_start = _get_total_position(_current_row_position) - first_chunk_row_position;
        int64_t get_value_end = std::min<int64_t>(_current_row_position + chunk_size, _partition_end);
        _window_result_position =
                std::min<int64_t>((_get_total_position(get_value_end) - first_chunk_row_position), chunk_size);

        _get_window_function_result(get_value_start, _window_result_position);
        _current_row_position += (_window_result_position - get_value_start);

        if (_window_result_position == _input_chunks[_output_chunk_index]->num_rows()) {
            return _output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_unbounded_preceding_range_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_input_eos || _output_chunk_index < _input_chunks.size()) {
        int64_t found_partition_end = 0;
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state, &found_partition_end));
        if (_input_eos && _input_rows == 0) {
            *eos = true;
            return Status::OK();
        }

        SCOPED_TIMER(_compute_timer);

        bool is_new_partition = _is_new_partition(found_partition_end);
        if (is_new_partition) {
            _reset_state_for_new_partition(found_partition_end);
        }

        size_t chunk_size = _input_chunks[_output_chunk_index]->num_rows();
        _create_agg_result_columns(chunk_size);

        while (_current_row_position < _partition_end && _window_result_position < chunk_size) {
            if (_current_row_position >= _peer_group_end) {
                _find_peer_group_end();
                DCHECK_GE(_peer_group_end, _peer_group_start);
                (this->*_update_window_batch)(_peer_group_start, _peer_group_end, _peer_group_start, _peer_group_end);
            }

            int64_t first_chunk_row_position = input_chunk_first_row_positions[_output_chunk_index];
            int64_t get_value_start = _get_total_position(_current_row_position) - first_chunk_row_position;
            _window_result_position =
                    std::min<int64_t>((_get_total_position(_peer_group_end) - first_chunk_row_position), chunk_size);

            DCHECK_GE(get_value_start, 0);
            DCHECK_GT(_window_result_position, get_value_start);

            _get_window_function_result(get_value_start, _window_result_position);
            _current_row_position += (_window_result_position - get_value_start);
        }

        if (_window_result_position == _input_chunks[_output_chunk_index]->num_rows()) {
            return _output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_sliding_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_input_eos || _output_chunk_index < _input_chunks.size()) {
        int64_t found_partition_end = 0;
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state, &found_partition_end));
        if (_input_eos && _input_rows == 0) {
            *eos = true;
            return Status::OK();
        }
        SCOPED_TIMER(_compute_timer);

        bool is_new_partition = _is_new_partition(found_partition_end);
        if (is_new_partition) {
            _reset_state_for_new_partition(found_partition_end);
        }

        size_t chunk_size = _input_chunks[_output_chunk_index]->num_rows();
        _create_agg_result_columns(chunk_size);

        while (_current_row_position < _partition_end && _window_result_position < chunk_size) {
            _reset_window_state();
            FrameRange range = (this->*_get_sliding_frame_range)();
            (this->*_update_window_batch)(_partition_start, _partition_end, range.start, range.end);
            _window_result_position++;
            int64_t result_start =
                    _get_total_position(_current_row_position) - input_chunk_first_row_positions[_output_chunk_index];
            DCHECK_GE(result_start, 0);
            _get_window_function_result(result_start, _window_result_position);
            _current_row_position++;
        }

        if (_window_result_position == _input_chunks[_output_chunk_index]->num_rows()) {
            return _output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

Status AnalyticNode::_get_next_for_unbounded_preceding_rows_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    while (!_input_eos || _output_chunk_index < _input_chunks.size()) {
        int64_t found_partition_end = 0;
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state, &found_partition_end));
        if (_input_eos && _input_rows == 0) {
            *eos = true;
            return Status::OK();
        }

        SCOPED_TIMER(_compute_timer);

        bool is_new_partition = _is_new_partition(found_partition_end);
        if (is_new_partition) {
            _reset_state_for_new_partition(found_partition_end);
        }

        size_t chunk_size = _input_chunks[_output_chunk_index]->num_rows();
        _create_agg_result_columns(chunk_size);

        while (_current_row_position < _partition_end && _window_result_position < chunk_size) {
            (this->*_update_window_batch)(_partition_start, _partition_end, _current_row_position,
                                          _current_row_position + 1);

            _window_result_position++;
            int64_t frame_start =
                    _get_total_position(_current_row_position) - input_chunk_first_row_positions[_output_chunk_index];

            DCHECK_GE(frame_start, 0);
            _get_window_function_result(frame_start, _window_result_position);
            _current_row_position++;
        }

        if (_window_result_position == _input_chunks[_output_chunk_index]->num_rows()) {
            return _output_result_chunk(chunk);
        }
    }
    return Status::OK();
}

bool AnalyticNode::_need_fetch_next_chunk(int64_t found_partition_end) {
    // current partition data don't consume finished
    if (_input_eos | (_current_row_position < _partition_end)) {
        return false;
    }

    // no partition or hasn't fecth one chunk
    if ((_partition_ctxs.empty() & !_input_eos) | (found_partition_end == 0)) {
        return true;
    }

    // partition end not found
    if (!_partition_ctxs.empty() && found_partition_end == _partition_columns[0]->size() && !_input_eos) {
        return true;
    }
    return false;
}

bool AnalyticNode::_is_new_partition(int64_t found_partition_end) {
    // _current_row_position >= _partition_end : current partition data has consumed finished
    // _partition_end == 0 : the first partition
    return ((_current_row_position >= _partition_end) &
            ((_partition_end == 0) | (_partition_end != found_partition_end)));
}

int64_t AnalyticNode::_find_partition_end() {
    // current partition data don't consume finished
    if (_current_row_position < _partition_end) {
        return _partition_end;
    }

    if (_partition_columns.empty() | (_input_rows == 0)) {
        return _input_rows;
    }

    int64_t found_partition_end = _partition_columns[0]->size();
    for (size_t i = 0; i < _partition_columns.size(); ++i) {
        Column* column = _partition_columns[i].get();
        found_partition_end = _find_first_not_equal(column, _partition_end, found_partition_end);
    }
    return found_partition_end;
}

int64_t AnalyticNode::_find_first_not_equal(Column* column, int64_t start, int64_t end) {
    int64_t target = start;
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

void AnalyticNode::_find_peer_group_end() {
    // current peer group data don't output finished
    if (_current_row_position < _peer_group_end) {
        return;
    }

    _peer_group_start = _peer_group_end;
    _peer_group_end = _partition_end;
    DCHECK(!_order_columns.empty());

    for (size_t i = 0; i < _order_columns.size(); ++i) {
        Column* column = _order_columns[i].get();
        _peer_group_end = _find_first_not_equal(column, _peer_group_start, _peer_group_end);
    }
}

void AnalyticNode::_reset_state_for_new_partition(int64_t found_partition_end) {
    _partition_start = _partition_end;
    _partition_end = found_partition_end;
    _current_row_position = _partition_start;
    _reset_window_state();
    DCHECK_GE(_current_row_position, 0);
}

Status AnalyticNode::_try_fetch_next_partition_data(RuntimeState* state, int64_t* partition_end) {
    *partition_end = _find_partition_end();
    while (_need_fetch_next_chunk(*partition_end)) {
        RETURN_IF_ERROR(_fetch_next_chunk(state));
        *partition_end = _find_partition_end();
    }
    return Status::OK();
}

void AnalyticNode::_append_column(size_t chunk_size, Column* dst_column, ColumnPtr& src_column) {
    if (src_column->only_null()) {
        dst_column->append_nulls(chunk_size);
    } else if (src_column->is_constant()) {
        ConstColumn* const_column = static_cast<ConstColumn*>(src_column.get());
        const_column->data_column()->assign(chunk_size, 0);
        dst_column->append(*const_column->data_column(), 0, chunk_size);
    } else {
        dst_column->append(*src_column, 0, chunk_size);
    }
}

Status AnalyticNode::_fetch_next_chunk(RuntimeState* state) {
    ChunkPtr child_chunk;
    RETURN_IF_CANCELLED(state);
    do {
        RETURN_IF_ERROR(_children[0]->get_next(state, &child_chunk, &_input_eos));
    } while (!_input_eos && child_chunk->is_empty());
    SCOPED_TIMER(_compute_timer);
    if (_input_eos) {
        return Status::OK();
    }

    input_chunk_first_row_positions.emplace_back(_input_rows);
    size_t chunk_size = child_chunk->num_rows();
    _input_rows += chunk_size;

    {
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            for (size_t j = 0; j < _agg_expr_ctxs[i].size(); j++) {
                ColumnPtr column = _agg_expr_ctxs[i][j]->evaluate(child_chunk.get());
                // Currently, only lead and lag window function have multi args.
                // For performance, we do this special handle.
                // In future, if need, we could remove this if else easily.
                if (j == 0) {
                    _append_column(chunk_size, _agg_intput_columns[i][j].get(), column);
                } else {
                    _agg_intput_columns[i][j]->append(*column, 0, column->size());
                }
            }
        }

        for (size_t i = 0; i < _partition_ctxs.size(); i++) {
            ColumnPtr column = _partition_ctxs[i]->evaluate(child_chunk.get());
            _append_column(chunk_size, _partition_columns[i].get(), column);
        }

        for (size_t i = 0; i < _order_ctxs.size(); i++) {
            ColumnPtr column = _order_ctxs[i]->evaluate(child_chunk.get());
            _append_column(chunk_size, _order_columns[i].get(), column);
        }
    }

    _input_chunks.emplace_back(std::move(child_chunk));
    return Status::OK();
}

void AnalyticNode::_get_window_function_result(int32_t start, int32_t end) {
    DCHECK_GT(end, start);
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        Column* agg_column = _result_window_columns[i].get();
        _agg_functions[i]->get_values(_agg_fn_ctxs[i], _managed_fn_states[0]->data() + _agg_states_offsets[i],
                                      agg_column, start, end);
    }
}

Status AnalyticNode::_output_result_chunk(ChunkPtr* chunk) {
    ChunkPtr output_chunk = std::move(_input_chunks[_output_chunk_index]);
    for (size_t i = 0; i < _result_window_columns.size(); i++) {
        output_chunk->append_column(_result_window_columns[i], _result_tuple_desc->slots()[i]->id());
    }

    _num_rows_returned += output_chunk->num_rows();

    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        output_chunk->set_num_rows(output_chunk->num_rows() - num_rows_over);
        COUNTER_SET(_rows_returned_counter, _limit);
        *chunk = output_chunk;
        return Status::OK();
    }

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    *chunk = output_chunk;
    _output_chunk_index++;
    _window_result_position = 0;
    return Status::OK();
}

void AnalyticNode::_remove_unused_buffer_values() {
    if (_input_chunks.size() <= _output_chunk_index ||
        input_chunk_first_row_positions[_output_chunk_index] - _removed_from_buffer_rows <
                config::vector_chunk_size * BUFFER_CHUNK_NUMBER) {
        return;
    }

    int64_t remove_end_position = input_chunk_first_row_positions[_removed_chunk_index + BUFFER_CHUNK_NUMBER];
    if (_partition_start <= remove_end_position) {
        return;
    }

    int64_t remove_count = remove_end_position - _removed_from_buffer_rows;
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); j++) {
            _agg_intput_columns[i][j]->remove_first_n_values(remove_count);
        }
    }
    for (size_t i = 0; i < _partition_ctxs.size(); i++) {
        _partition_columns[i]->remove_first_n_values(remove_count);
    }
    for (size_t i = 0; i < _order_ctxs.size(); i++) {
        _order_columns[i]->remove_first_n_values(remove_count);
    }

    _removed_from_buffer_rows += remove_count;
    _partition_start -= remove_count;
    _partition_end -= remove_count;
    _current_row_position -= remove_count;
    _peer_group_start -= remove_count;
    _peer_group_end -= remove_count;

    _removed_chunk_index += BUFFER_CHUNK_NUMBER;

    DCHECK_GE(_current_row_position, 0);
}

int64_t AnalyticNode::_get_total_position(int64_t local_position) {
    return _removed_from_buffer_rows + local_position;
}

FrameRange AnalyticNode::_get_sliding_frame_range_no_start() {
    return {_partition_start, _current_row_position + _rows_end_offset + 1};
}

FrameRange AnalyticNode::_get_sliding_frame_range_with_start() {
    return {_current_row_position + _rows_start_offset, _current_row_position + _rows_end_offset + 1};
}

void AnalyticNode::_update_window_batch_lead_lag(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                                 int64_t frame_end) {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        const Column* agg_column = _agg_intput_columns[i][0].get();
        _agg_functions[i]->update_batch_single_state(
                _agg_fn_ctxs[i], _managed_fn_states[0]->mutable_data() + _agg_states_offsets[i], &agg_column,
                peer_group_start, peer_group_end, frame_start, frame_end);
    }
}

void AnalyticNode::_update_window_batch_normal(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                               int64_t frame_end) {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        const Column* agg_column = _agg_intput_columns[i][0].get();
        frame_start = std::max<int64_t>(frame_start, _partition_start);
        frame_end = std::min<int64_t>(frame_end, _partition_end);
        _agg_functions[i]->update_batch_single_state(
                _agg_fn_ctxs[i], _managed_fn_states[0]->mutable_data() + _agg_states_offsets[i], &agg_column,
                peer_group_start, peer_group_end, frame_start, frame_end);
    }
}

void AnalyticNode::_reset_window_state() {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->reset(_agg_fn_ctxs[i], _agg_intput_columns[i],
                                 _managed_fn_states[0]->mutable_data() + _agg_states_offsets[i]);
    }
}

void AnalyticNode::_create_agg_result_columns(int64_t chunk_size) {
    if (_window_result_position == 0) {
        _result_window_columns.resize(_agg_fn_types.size());
        for (size_t i = 0; i < _agg_fn_types.size(); ++i) {
            _result_window_columns[i] =
                    ColumnHelper::create_column(_agg_fn_types[i].result_type, _agg_fn_types[i].has_nullable_child);
            // binary column cound't call resize method like Numeric Column,
            // so we only reserve it.
            if (_agg_fn_types[i].result_type.type == PrimitiveType::TYPE_CHAR ||
                _agg_fn_types[i].result_type.type == PrimitiveType::TYPE_VARCHAR) {
                _result_window_columns[i]->reserve(chunk_size);
            } else {
                _result_window_columns[i]->resize(chunk_size);
            }
        }
    }
}

Status AnalyticNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    for (auto* ctx : _agg_fn_ctxs) {
        if (ctx != nullptr && ctx->impl()) {
            ctx->impl()->close();
        }
    }

    // Note: we must free agg_states before _mem_pool free_all;
    _managed_fn_states.clear();
    _managed_fn_states.shrink_to_fit();

    // Note: we must explicit free memory before ExecNode::close!
    if (_mem_pool != nullptr) {
        _mem_pool->free_all();
    }

    mem_tracker()->release(_last_memory_usage);

    Expr::close(_order_ctxs, state);
    Expr::close(_partition_ctxs, state);
    for (const auto& i : _agg_expr_ctxs) {
        Expr::close(i, state);
    }

    return ExecNode::close(state);
}

} // namespace starrocks::vectorized
