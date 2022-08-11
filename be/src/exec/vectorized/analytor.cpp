// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/analytor.h"

#include <algorithm>
#include <cmath>
#include <memory>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "exprs/agg/count.h"
#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "udf/java/utils.h"
#include "udf/udf.h"
#include "util/runtime_profile.h"

namespace starrocks {
namespace vectorized {
Status window_init_jvm_context(int64_t fid, const std::string& url, const std::string& checksum,
                               const std::string& symbol, starrocks_udf::FunctionContext* context);
} // namespace vectorized

Analytor::Analytor(const TPlanNode& tnode, const RowDescriptor& child_row_desc,
                   const TupleDescriptor* result_tuple_desc)
        : _tnode(tnode), _child_row_desc(child_row_desc), _result_tuple_desc(result_tuple_desc) {
    if (tnode.analytic_node.__isset.buffered_tuple_id) {
        _buffered_tuple_id = tnode.analytic_node.buffered_tuple_id;
    }

    TAnalyticWindow window = tnode.analytic_node.window;
    FrameType frame_type = FrameType::Unbounded;
    if (!tnode.analytic_node.__isset.window) {
    } else if (tnode.analytic_node.window.type == TAnalyticWindowType::RANGE) {
        // RANGE windows must have UNBOUNDED PRECEDING
        // RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING
        if (!window.__isset.window_end) {
            frame_type = FrameType::Unbounded;
        } else {
            frame_type = FrameType::UnboundedPrecedingRange;
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
        } else if (!window.__isset.window_start && window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW) {
            frame_type = FrameType::UnboundedPrecedingRows;
        } else {
            frame_type = FrameType::Sliding;
            _is_range_with_start = window.__isset.window_start;
        }
    }

    VLOG_ROW << "frame_type " << frame_type << " _rows_start_offset " << _rows_start_offset << " "
             << " _rows_end_offset " << _rows_end_offset;
}

Status Analytor::prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile) {
    _state = state;

    _pool = pool;
    _runtime_profile = runtime_profile;
    _limit = _tnode.limit;
    // add profile attributes
    if (_tnode.analytic_node.__isset.sql_partition_keys) {
        _runtime_profile->add_info_string("PartitionKeys", _tnode.analytic_node.sql_partition_keys);
    }
    if (_tnode.analytic_node.__isset.sql_aggregate_functions) {
        _runtime_profile->add_info_string("AggregateFunctions", _tnode.analytic_node.sql_aggregate_functions);
    }
    _rows_returned_counter = ADD_COUNTER(_runtime_profile, "RowsReturned", TUnit::UNIT);
    _mem_pool = std::make_unique<MemPool>();

    const TAnalyticNode& analytic_node = _tnode.analytic_node;

    size_t agg_size = analytic_node.analytic_functions.size();
    _agg_fn_ctxs.resize(agg_size);
    _agg_functions.resize(agg_size);
    _agg_expr_ctxs.resize(agg_size);
    _agg_intput_columns.resize(agg_size);
    _agg_fn_types.resize(agg_size);
    _agg_states_offsets.resize(agg_size);

    bool has_outer_join_child = analytic_node.__isset.has_outer_join_child && analytic_node.has_outer_join_child;

    _has_lead_lag_function = false;
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

        if (fn.name.function_name == "ntile") {
            if (!state->enable_pipeline_engine()) {
                return Status::NotSupported("The NTILE window function is only supported by the pipeline engine.");
            }

            _need_partition_boundary_for_unbounded_preceding_rows_frame = true;
        }

        bool is_input_nullable = false;
        if (fn.name.function_name == "count" || fn.name.function_name == "row_number" ||
            fn.name.function_name == "rank" || fn.name.function_name == "dense_rank" ||
            fn.name.function_name == "ntile") {
            is_input_nullable = !fn.arg_types.empty() && (desc.nodes[0].has_nullable_child || has_outer_join_child);
            auto* func = vectorized::get_window_function(fn.name.function_name, TYPE_BIGINT, TYPE_BIGINT,
                                                         is_input_nullable, fn.binary_type, state->func_version());
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
            auto* func = vectorized::get_window_function(fn.name.function_name, arg_type.type, return_type.type,
                                                         is_input_nullable, fn.binary_type, state->func_version());
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
                _agg_intput_columns[i][j] = vectorized::ColumnHelper::create_column(
                        _agg_expr_ctxs[i][j]->root()->type(), is_input_nullable);
            } else {
                _agg_intput_columns[i][j] = vectorized::ColumnHelper::create_column(
                        _agg_expr_ctxs[i][j]->root()->type(), _agg_expr_ctxs[i][j]->root()->is_nullable(),
                        _agg_expr_ctxs[i][j]->root()->is_constant(), 0);
            }
        }

        DCHECK(_agg_functions[i] != nullptr);
        VLOG_ROW << "get agg function " << _agg_functions[i]->get_name();
        if (_agg_functions[i]->get_name() == "lead-lag") {
            _has_lead_lag_function = true;
        }
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
        _partition_columns[i] = vectorized::ColumnHelper::create_column(
                _partition_ctxs[i]->root()->type(), _partition_ctxs[i]->root()->is_nullable() | has_outer_join_child,
                _partition_ctxs[i]->root()->is_constant(), 0);
    }

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, analytic_node.order_by_exprs, &_order_ctxs));
    _order_columns.resize(_order_ctxs.size());
    for (size_t i = 0; i < _order_ctxs.size(); i++) {
        _order_columns[i] = vectorized::ColumnHelper::create_column(
                _order_ctxs[i]->root()->type(), _order_ctxs[i]->root()->is_nullable() | has_outer_join_child,
                _order_ctxs[i]->root()->is_constant(), 0);
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _compute_timer = ADD_TIMER(_runtime_profile, "ComputeTime");
    DCHECK_EQ(_result_tuple_desc->slots().size(), _agg_functions.size());

    SCOPED_TIMER(_compute_timer);
    for (const auto& ctx : _agg_expr_ctxs) {
        Expr::prepare(ctx, state);
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

    // save TFunction object
    _fns.reserve(_agg_fn_ctxs.size());
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        _fns.emplace_back(_tnode.analytic_node.analytic_functions[i].nodes[0].fn);
    }

    return Status::OK();
}

Status Analytor::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(Expr::open(_partition_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_order_ctxs, state));
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        RETURN_IF_ERROR(Expr::open(_agg_expr_ctxs[i], state));
    }

    _has_udaf = std::any_of(_fns.begin(), _fns.end(),
                            [](const auto& ctx) { return ctx.binary_type == TFunctionBinaryType::SRJAR; });

    auto create_fn_states = [this]() {
        for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
            if (_fns[i].binary_type == TFunctionBinaryType::SRJAR) {
                const auto& fn = _fns[i];
                auto st = vectorized::window_init_jvm_context(fn.fid, fn.hdfs_location, fn.checksum,
                                                              fn.aggregate_fn.symbol, _agg_fn_ctxs[i]);
                RETURN_IF_ERROR(st);
            }
        }
        vectorized::AggDataPtr agg_states =
                _mem_pool->allocate_aligned(_agg_states_total_size, _max_agg_state_align_size);
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
        for (auto* ctx : _agg_fn_ctxs) {
            if (ctx != nullptr && ctx->impl()) {
                ctx->impl()->close();
            }
        }

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
        promise_st->get_future().get();
    } else {
        agg_close();
    }
}

bool Analytor::is_chunk_buffer_empty() {
    std::lock_guard<std::mutex> l(_buffer_mutex);
    return _buffer.empty();
}

vectorized::ChunkPtr Analytor::poll_chunk_buffer() {
    std::lock_guard<std::mutex> l(_buffer_mutex);
    if (_buffer.empty()) {
        return nullptr;
    }
    vectorized::ChunkPtr chunk = _buffer.front();
    _buffer.pop();
    return chunk;
}

void Analytor::offer_chunk_to_buffer(const vectorized::ChunkPtr& chunk) {
    std::lock_guard<std::mutex> l(_buffer_mutex);
    _buffer.push(chunk);
}

FrameRange Analytor::get_sliding_frame_range() {
    if (_is_range_with_start) {
        return {_current_row_position + _rows_start_offset, _current_row_position + _rows_end_offset + 1};
    } else {
        return {_partition_start, _current_row_position + _rows_end_offset + 1};
    }
}

void Analytor::update_window_batch(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                   int64_t frame_end) {
    if (_has_lead_lag_function) {
        _update_window_batch_lead_lag(peer_group_start, peer_group_end, frame_start, frame_end);
    } else {
        _update_window_batch_normal(peer_group_start, peer_group_end, frame_start, frame_end);
    }
}

void Analytor::reset_window_state() {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->reset(_agg_fn_ctxs[i], _agg_intput_columns[i],
                                 _managed_fn_states[0]->mutable_data() + _agg_states_offsets[i]);
    }
}

void Analytor::get_window_function_result(size_t start, size_t end) {
    DCHECK_GT(end, start);
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        vectorized::Column* agg_column = _result_window_columns[i].get();
        _agg_functions[i]->get_values(_agg_fn_ctxs[i], _managed_fn_states[0]->data() + _agg_states_offsets[i],
                                      agg_column, start, end);
    }
}

bool Analytor::is_partition_finished() {
    if (_input_eos) {
        return true;
    }

    // There is no partition, or it hasn't fetched any chunk.
    if (_partition_ctxs.empty() || _found_partition_end == 0) {
        return false;
    }

    // If found_partition_end == _partition_columns[0]->size(),
    // the next chunk maybe also belongs to the current partition.
    return _found_partition_end != _partition_columns[0]->size();
}

Status Analytor::output_result_chunk(vectorized::ChunkPtr* chunk) {
    vectorized::ChunkPtr output_chunk = std::move(_input_chunks[_output_chunk_index]);
    for (size_t i = 0; i < _result_window_columns.size(); i++) {
        output_chunk->append_column(_result_window_columns[i], _result_tuple_desc->slots()[i]->id());
    }

    _num_rows_returned += output_chunk->num_rows();

    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        output_chunk->set_num_rows(output_chunk->num_rows() - num_rows_over);
        COUNTER_SET(_rows_returned_counter, _limit);
        *chunk = output_chunk;
        _output_chunk_index++;
        return Status::OK();
    }

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    *chunk = output_chunk;
    _output_chunk_index++;
    _window_result_position = 0;
    return Status::OK();
}

void Analytor::create_agg_result_columns(int64_t chunk_size) {
    if (_window_result_position == 0) {
        _result_window_columns.resize(_agg_fn_types.size());
        for (size_t i = 0; i < _agg_fn_types.size(); ++i) {
            _result_window_columns[i] = vectorized::ColumnHelper::create_column(_agg_fn_types[i].result_type,
                                                                                _agg_fn_types[i].has_nullable_child);
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

void Analytor::append_column(size_t chunk_size, vectorized::Column* dst_column, vectorized::ColumnPtr& src_column) {
    if (src_column->only_null()) {
        dst_column->append_nulls(chunk_size);
    } else if (src_column->is_constant()) {
        vectorized::ConstColumn* const_column = static_cast<vectorized::ConstColumn*>(src_column.get());
        const_column->data_column()->assign(chunk_size, 0);
        dst_column->append(*const_column->data_column(), 0, chunk_size);
    } else {
        dst_column->append(*src_column, 0, chunk_size);
    }
}

bool Analytor::is_new_partition() {
    // _current_row_position >= _partition_end : current partition data has been processed
    // _partition_end == 0 : the first partition
    return ((_current_row_position >= _partition_end) &
            ((_partition_end == 0) | (_partition_end != _found_partition_end)));
}

int64_t Analytor::get_total_position(int64_t local_position) {
    return _removed_from_buffer_rows + local_position;
}

void Analytor::find_partition_end() {
    // current partition data don't consume finished
    if (_current_row_position < _partition_end) {
        _found_partition_end = _partition_end;
        return;
    }

    if (_partition_columns.empty() || _input_rows == 0) {
        _found_partition_end = _input_rows;
        return;
    }

    int64_t start = _found_partition_end;
    _found_partition_end = static_cast<int64_t>(_partition_columns[0]->size());
    for (auto& column : _partition_columns) {
        _found_partition_end = _find_first_not_equal(column.get(), _partition_end, start, _found_partition_end);
    }
}

bool Analytor::find_and_check_partition_end() {
    if (_partition_columns.empty() || _input_rows == 0) {
        _found_partition_end = _input_rows;
        return false;
    }

    int64_t start = _found_partition_end;
    _found_partition_end = static_cast<int64_t>(_partition_columns[0]->size());
    for (auto& column : _partition_columns) {
        _found_partition_end = _find_first_not_equal(column.get(), _partition_end, start, _found_partition_end);
    }
    return _found_partition_end != static_cast<int64_t>(_partition_columns[0]->size());
}

void Analytor::find_peer_group_end() {
    // current peer group data don't output finished
    if (_current_row_position < _peer_group_end) {
        return;
    }

    _peer_group_start = _peer_group_end;
    _peer_group_end = _partition_end;
    DCHECK(!_order_columns.empty());

    for (auto& column : _order_columns) {
        _peer_group_end = _find_first_not_equal(column.get(), _peer_group_start, _peer_group_start, _peer_group_end);
    }
}

void Analytor::reset_state_for_cur_partition() {
    _partition_start = _partition_end;
    _partition_end = _found_partition_end;
    _current_row_position = _partition_start;
    reset_window_state();
    DCHECK_GE(_current_row_position, 0);
}

void Analytor::reset_state_for_next_partition() {
    _partition_end = _found_partition_end;
    _partition_start = _partition_end;
    _current_row_position = _partition_start;
    reset_window_state();
    DCHECK_GE(_current_row_position, 0);
}

void Analytor::remove_unused_buffer_values(RuntimeState* state) {
    if (_input_chunks.size() <= _output_chunk_index ||
        _input_chunk_first_row_positions[_output_chunk_index] - _removed_from_buffer_rows <
                state->chunk_size() * BUFFER_CHUNK_NUMBER) {
        return;
    }

    int64_t remove_end_position = _input_chunk_first_row_positions[_removed_chunk_index + BUFFER_CHUNK_NUMBER];
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
    _found_partition_end -= remove_count;
    _current_row_position -= remove_count;
    _peer_group_start -= remove_count;
    _peer_group_end -= remove_count;

    _removed_chunk_index += BUFFER_CHUNK_NUMBER;

    DCHECK_GE(_current_row_position, 0);
}

<<<<<<< HEAD
=======
std::string Analytor::debug_string() const {
    std::stringstream ss;
    ss << std::boolalpha;

    ss << "current_row_position=" << _current_row_position << ", partition=(" << _partition_start << ", "
       << _partition_end << ", " << _found_partition_end.second << "/" << _found_partition_end.first
       << "), peer_group=(" << _peer_group_start << ", " << _peer_group_end << ")"
       << ", frame=(" << _rows_start_offset << ", " << _rows_end_offset << ")";

    return ss.str();
}

Status Analytor::check_has_error() {
    for (const auto* ctx : _agg_fn_ctxs) {
        if (ctx != nullptr) {
            if (ctx->has_error()) {
                return Status::RuntimeError(ctx->error_msg());
            }
        }
    }
    return Status::OK();
}

>>>>>>> f392656e9 ([BugFix] Fix user-defined function get a wrong result (#9887))
void Analytor::_update_window_batch_lead_lag(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                             int64_t frame_end) {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        const vectorized::Column* agg_column = _agg_intput_columns[i][0].get();
        _agg_functions[i]->update_batch_single_state(
                _agg_fn_ctxs[i], _managed_fn_states[0]->mutable_data() + _agg_states_offsets[i], &agg_column,
                peer_group_start, peer_group_end, frame_start, frame_end);
    }
}

void Analytor::_update_window_batch_normal(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                           int64_t frame_end) {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        const vectorized::Column* agg_column = _agg_intput_columns[i][0].get();
        frame_start = std::max<int64_t>(frame_start, _partition_start);
        // for rows betweend unbounded preceding and current row, we have not found the partition end, for others,
        // _found_partition_end = _partition_end, so we use _found_partition_end instead of _partition_end
        frame_end = std::min<int64_t>(frame_end, _found_partition_end);
        _agg_functions[i]->update_batch_single_state(
                _agg_fn_ctxs[i], _managed_fn_states[0]->mutable_data() + _agg_states_offsets[i], &agg_column,
                peer_group_start, peer_group_end, frame_start, frame_end);
    }
}

int64_t Analytor::_find_first_not_equal(vectorized::Column* column, int64_t target, int64_t start, int64_t end) {
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

AnalytorPtr AnalytorFactory::create(int i) {
    if (!_analytors[i]) {
        _analytors[i] = std::make_shared<Analytor>(_tnode, _child_row_desc, _result_tuple_desc);
    }
    return _analytors[i];
}
} // namespace starrocks
