// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "aggregator.h"

#include <algorithm>

#include "column/chunk.h"
#include "common/status.h"
#include "exec/pipeline/operator.h"
#include "exprs/anyval_util.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "udf/java/utils.h"

namespace starrocks {
namespace vectorized {

Status init_udaf_context(int64_t fid, const std::string& url, const std::string& checksum, const std::string& symbol,
                         starrocks_udf::FunctionContext* context);

} // namespace vectorized
Aggregator::Aggregator(const TPlanNode& tnode) : _tnode(tnode) {}

Status Aggregator::open(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::open(_group_by_expr_ctxs, state));
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        RETURN_IF_ERROR(Expr::open(_agg_expr_ctxs[i], state));
        RETURN_IF_ERROR(_evaluate_const_columns(i));
    }
    for (int i = 0; i < _intermediate_agg_expr_ctxs.size(); ++i) {
        RETURN_IF_ERROR(Expr::open(_intermediate_agg_expr_ctxs[i], state));
    }
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));

    // init function context
    _has_udaf = std::any_of(_fns.begin(), _fns.end(),
                            [](const auto& ctx) { return ctx.binary_type == TFunctionBinaryType::SRJAR; });
    if (_has_udaf) {
        auto promise_st = call_function_in_pthread(state, [this]() {
            for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
                if (_fns[i].binary_type == TFunctionBinaryType::SRJAR) {
                    const auto& fn = _fns[i];
                    auto st = vectorized::init_udaf_context(fn.fid, fn.hdfs_location, fn.checksum,
                                                            fn.aggregate_fn.symbol, _agg_fn_ctxs[i]);
                    RETURN_IF_ERROR(st);
                }
            }
            return Status::OK();
        });
        RETURN_IF_ERROR(promise_st->get_future().get());
    }

    // AggregateFunction::create needs to call create in JNI,
    // but prepare is executed in bthread, which will cause the JNI code to crash

    if (_group_by_expr_ctxs.empty()) {
        _single_agg_state = _mem_pool->allocate_aligned(_agg_states_total_size, _max_agg_state_align_size);
        RETURN_IF_UNLIKELY_NULL(_single_agg_state, Status::MemoryAllocFailed("alloc single agg state failed"));
        auto call_agg_create = [this]() {
            for (int i = 0; i < _agg_functions.size(); i++) {
                _agg_functions[i]->create(_agg_fn_ctxs[0], _single_agg_state + _agg_states_offsets[i]);
            }
            return Status::OK();
        };
        if (_has_udaf) {
            auto promise_st = call_function_in_pthread(state, call_agg_create);
            promise_st->get_future().get();
        } else {
            call_agg_create();
        }

        if (_agg_expr_ctxs.empty()) {
            return Status::InternalError("Invalid agg query plan");
        }
    }

    // For SQL: select distinct id from table or select id from from table group by id;
    // we don't need to allocate memory for agg states.
    if (_is_only_group_by_columns) {
        TRY_CATCH_BAD_ALLOC(_init_agg_hash_variant(_hash_set_variant));
    } else {
        TRY_CATCH_BAD_ALLOC(_init_agg_hash_variant(_hash_map_variant));
    }

    RETURN_IF_ERROR(check_has_error());

    return Status::OK();
}

Status Aggregator::prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile,
                           MemTracker* mem_tracker) {
    _state = state;

    _pool = pool;
    _runtime_profile = runtime_profile;
    _mem_tracker = mem_tracker;

    _limit = _tnode.limit;
    _needs_finalize = _tnode.agg_node.need_finalize;
    _streaming_preaggregation_mode = _tnode.agg_node.streaming_preaggregation_mode;
    _intermediate_tuple_id = _tnode.agg_node.intermediate_tuple_id;
    _output_tuple_id = _tnode.agg_node.output_tuple_id;

    _rows_returned_counter = ADD_COUNTER(_runtime_profile, "RowsReturned", TUnit::UNIT);

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, _tnode.conjuncts, &_conjunct_ctxs));
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, _tnode.agg_node.grouping_exprs, &_group_by_expr_ctxs));
    // add profile attributes
    if (_tnode.agg_node.__isset.sql_grouping_keys) {
        _runtime_profile->add_info_string("GroupingKeys", _tnode.agg_node.sql_grouping_keys);
    }
    if (_tnode.agg_node.__isset.sql_aggregate_functions) {
        _runtime_profile->add_info_string("AggregateFunctions", _tnode.agg_node.sql_aggregate_functions);
    }

    bool has_outer_join_child = _tnode.agg_node.__isset.has_outer_join_child && _tnode.agg_node.has_outer_join_child;
    VLOG_ROW << "has_outer_join_child " << has_outer_join_child;

    size_t group_by_size = _group_by_expr_ctxs.size();
    _group_by_columns.resize(group_by_size);
    _group_by_types.resize(group_by_size);
    for (size_t i = 0; i < group_by_size; ++i) {
        TExprNode expr = _tnode.agg_node.grouping_exprs[i].nodes[0];
        _group_by_types[i].result_type = TypeDescriptor::from_thrift(expr.type);
        _group_by_types[i].is_nullable = expr.is_nullable || has_outer_join_child;
        _has_nullable_key = _has_nullable_key || _group_by_types[i].is_nullable;
        VLOG_ROW << "group by column " << i << " result_type " << _group_by_types[i].result_type << " is_nullable "
                 << expr.is_nullable;
    }
    VLOG_ROW << "has_nullable_key " << _has_nullable_key;

    _tmp_agg_states.resize(_state->chunk_size());

    size_t agg_size = _tnode.agg_node.aggregate_functions.size();
    _agg_fn_ctxs.resize(agg_size);
    _agg_functions.resize(agg_size);
    _agg_expr_ctxs.resize(agg_size);
    _agg_input_columns.resize(agg_size);
    _agg_input_raw_columns.resize(agg_size);
    _agg_fn_types.resize(agg_size);
    _agg_states_offsets.resize(agg_size);
    _is_merge_funcs.resize(agg_size);

    for (int i = 0; i < agg_size; ++i) {
        const TExpr& desc = _tnode.agg_node.aggregate_functions[i];
        const TFunction& fn = desc.nodes[0].fn;
        _is_merge_funcs[i] = _tnode.agg_node.aggregate_functions[i].nodes[0].agg_expr.is_merge_agg;
        VLOG_ROW << fn.name.function_name << " is arg nullable " << desc.nodes[0].has_nullable_child;
        VLOG_ROW << fn.name.function_name << " is result nullable " << desc.nodes[0].is_nullable;
        if (fn.name.function_name == "count") {
            {
                bool is_input_nullable =
                        !fn.arg_types.empty() && (has_outer_join_child || desc.nodes[0].has_nullable_child);
                auto* func = vectorized::get_aggregate_function("count", TYPE_BIGINT, TYPE_BIGINT, is_input_nullable);
                _agg_functions[i] = func;
            }
            std::vector<FunctionContext::TypeDesc> arg_typedescs;
            _agg_fn_types[i] = {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT), arg_typedescs, false, false};
        } else {
            TypeDescriptor return_type = TypeDescriptor::from_thrift(fn.ret_type);
            TypeDescriptor serde_type = TypeDescriptor::from_thrift(fn.aggregate_fn.intermediate_type);

            // collect arg_typedescs for aggregate function.
            std::vector<FunctionContext::TypeDesc> arg_typedescs;
            for (auto& type : fn.arg_types) {
                arg_typedescs.push_back(AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_thrift(type)));
            }

            TypeDescriptor arg_type = TypeDescriptor::from_thrift(fn.arg_types[0]);
            // Because intersect_count has more two input types.
            // intersect_count's first argument's type is alwasy Bitmap,
            // So we get its second arguments type as input.
            if (fn.name.function_name == "intersect_count" || fn.name.function_name == "max_by") {
                arg_type = TypeDescriptor::from_thrift(fn.arg_types[1]);
            }

            // Because windowfunnel have more two input types.
            // functions registry use 2th args(datetime/date).
            if (fn.name.function_name == "window_funnel") {
                arg_type = TypeDescriptor::from_thrift(fn.arg_types[1]);
            }

            if (fn.name.function_name == "exchange_bytes" || fn.name.function_name == "exchange_speed") {
                arg_type = TypeDescriptor(TYPE_BIGINT);
            }

            bool is_input_nullable = has_outer_join_child || desc.nodes[0].has_nullable_child;
            auto* func = vectorized::get_aggregate_function(fn.name.function_name, arg_type.type, return_type.type,
                                                            is_input_nullable, fn.binary_type, state->func_version());
            if (func == nullptr) {
                return Status::InternalError(
                        strings::Substitute("Invalid agg function plan: $0", fn.name.function_name));
            }
            VLOG_ROW << "get agg function " << func->get_name() << " serde_type " << serde_type << " return_type "
                     << return_type;
            _agg_functions[i] = func;
            _agg_fn_types[i] = {return_type, serde_type, arg_typedescs, is_input_nullable, desc.nodes[0].is_nullable};
        }

        int node_idx = 0;
        for (int j = 0; j < desc.nodes[0].num_children; ++j) {
            ++node_idx;
            Expr* expr = nullptr;
            ExprContext* ctx = nullptr;
            RETURN_IF_ERROR(Expr::create_tree_from_thrift(_pool, desc.nodes, nullptr, &node_idx, &expr, &ctx));
            _agg_expr_ctxs[i].emplace_back(ctx);
        }

        // It is very critical, because for a count(*) or count(1) aggregation function, when it first be applied to
        // input data, the agg function needs no input columns; but when it is parted into two parts when query cache
        // enabled, the latter part after cache operator must always handle intermediate types, so the agg function
        // need at least one input column to store intermediate result.
        auto num_args = std::max<size_t>(1UL, desc.nodes[0].num_children);
        _agg_input_columns[i].resize(num_args);
        _agg_input_raw_columns[i].resize(num_args);
    }

    if (_tnode.agg_node.__isset.intermediate_aggr_exprs) {
        auto& aggr_exprs = _tnode.agg_node.intermediate_aggr_exprs;
        _intermediate_agg_expr_ctxs.resize(agg_size);
        for (int i = 0; i < agg_size; ++i) {
            int node_idx = 0;
            Expr* expr = nullptr;
            ExprContext* ctx = nullptr;
            RETURN_IF_ERROR(Expr::create_tree_from_thrift(_pool, aggr_exprs[i].nodes, nullptr, &node_idx, &expr, &ctx));
            _intermediate_agg_expr_ctxs[i].emplace_back(ctx);
        }
    }

    _mem_pool = std::make_unique<MemPool>();
    // TODO: use hashtable key size as align
    // reserve size for hash table key
    _agg_states_total_size = 16;
    // compute agg state total size and offsets
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
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
    // we need to allocate contiguous memory, so we need some alignment operations
    _max_agg_state_align_size = std::max(_max_agg_state_align_size, HashTableKeyAllocator::aligned);
    _agg_states_total_size = (_agg_states_total_size + _max_agg_state_align_size - 1) / _max_agg_state_align_size *
                             _max_agg_state_align_size;
    _state_allocator.aggregate_key_size = _agg_states_total_size;
    _state_allocator.pool = _mem_pool.get();

    _is_only_group_by_columns = _agg_expr_ctxs.empty() && !_group_by_expr_ctxs.empty();

    _get_results_timer = ADD_TIMER(_runtime_profile, "GetResultsTime");
    _iter_timer = ADD_TIMER(_runtime_profile, "ResultIteratorTime");
    _agg_append_timer = ADD_TIMER(_runtime_profile, "ResultAggAppendTime");
    _group_by_append_timer = ADD_TIMER(_runtime_profile, "ResultGroupByAppendTime");
    //TODO: split agg_compute_timer to cunstruct_ht_time + agg_func_compute_time
    _agg_compute_timer = ADD_TIMER(_runtime_profile, "AggComputeTime");
    _streaming_timer = ADD_TIMER(_runtime_profile, "StreamingTime");
    _expr_compute_timer = ADD_TIMER(_runtime_profile, "ExprComputeTime");
    _expr_release_timer = ADD_TIMER(_runtime_profile, "ExprReleaseTime");

    _input_row_count = ADD_COUNTER(_runtime_profile, "InputRowCount", TUnit::UNIT);
    _hash_table_size = ADD_COUNTER(_runtime_profile, "HashTableSize", TUnit::UNIT);
    _pass_through_row_count = ADD_COUNTER(_runtime_profile, "PassThroughRowCount", TUnit::UNIT);

    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());

    RETURN_IF_ERROR(Expr::prepare(_group_by_expr_ctxs, state));

    for (const auto& ctx : _agg_expr_ctxs) {
        RETURN_IF_ERROR(Expr::prepare(ctx, state));
    }

    for (const auto& ctx : _intermediate_agg_expr_ctxs) {
        RETURN_IF_ERROR(Expr::prepare(ctx, state));
    }

    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));

    // Initial for FunctionContext of every aggregate functions
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        _agg_fn_ctxs[i] = FunctionContextImpl::create_context(
                state, _mem_pool.get(), AnyValUtil::column_type_to_type_desc(_agg_fn_types[i].result_type),
                _agg_fn_types[i].arg_typedescs, 0, false);
        state->obj_pool()->add(_agg_fn_ctxs[i]);
    }

    // save TFunction object
    _fns.reserve(_agg_fn_ctxs.size());
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        _fns.emplace_back(_tnode.agg_node.aggregate_functions[i].nodes[0].fn);
    }

    return Status::OK();
}

Status Aggregator::reset_state(starrocks::RuntimeState* state, const std::vector<vectorized::ChunkPtr>& refill_chunks,
                               pipeline::Operator* refill_op) {
    RETURN_IF_ERROR(_reset_state(state));
    // begin_pending_reset_state just tells the Aggregator, the chunks are intermediate type, it should call
    // merge method of agg functions to process these chunks.
    begin_pending_reset_state();
    for (const auto& chunk : refill_chunks) {
        if (chunk == nullptr || chunk->is_empty()) {
            continue;
        }
        RETURN_IF_ERROR(refill_op->push_chunk(state, chunk));
    }
    end_pending_reset_state();
    return Status::OK();
}

Status Aggregator::_reset_state(RuntimeState* state) {
    _is_ht_eos = false;
    _num_input_rows = 0;
    _is_sink_complete = false;
    _it_hash.reset();
    _num_rows_processed = 0;

    {
        typeof(_buffer) empty_buffer;
        _buffer.swap(empty_buffer);
    }
    _tmp_agg_states.assign(_tmp_agg_states.size(), nullptr);
    _streaming_selection.assign(_streaming_selection.size(), 0);

    DCHECK(_mem_pool != nullptr);
    // Note: we must free agg_states object before _mem_pool free_all;
    if (_group_by_expr_ctxs.empty()) {
        for (int i = 0; i < _agg_functions.size(); i++) {
            _agg_functions[i]->destroy(_agg_fn_ctxs[0], _single_agg_state + _agg_states_offsets[i]);
        }
    } else if (!_is_only_group_by_columns) {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                     \
    else if (_hash_map_variant.type == vectorized::AggHashMapVariant::Type::NAME) \
            _release_agg_memory<decltype(_hash_map_variant.NAME)::element_type>(_hash_map_variant.NAME.get());
        APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    }
    _mem_pool->free_all();

    if (_group_by_expr_ctxs.empty()) {
        _single_agg_state = _mem_pool->allocate_aligned(_agg_states_total_size, _max_agg_state_align_size);
        for (int i = 0; i < _agg_functions.size(); i++) {
            _agg_functions[i]->create(_agg_fn_ctxs[0], _single_agg_state + _agg_states_offsets[i]);
        }
    } else if (_is_only_group_by_columns) {
        TRY_CATCH_BAD_ALLOC(_init_agg_hash_variant(_hash_set_variant));
    } else {
        TRY_CATCH_BAD_ALLOC(_init_agg_hash_variant(_hash_map_variant));
    }
    // _state_allocator holds the entries of the hash_map/hash_set, when iterating a hash_map/set, the _state_allocator
    // is used to access these entries, so we must reset the _state_allocator along with the hash_map/hash_set.
    _state_allocator.reset();
    return Status::OK();
}

void Aggregator::close(RuntimeState* state) {
    if (_is_closed) {
        return;
    }

    _is_closed = true;
    // Clear the buffer
    while (!_buffer.empty()) {
        _buffer.pop();
    }

    auto agg_close = [this, state]() {
        // _mem_pool is nullptr means prepare phase failed
        if (_mem_pool != nullptr) {
            // Note: we must free agg_states object before _mem_pool free_all;
            if (_single_agg_state != nullptr) {
                for (int i = 0; i < _agg_functions.size(); i++) {
                    _agg_functions[i]->destroy(_agg_fn_ctxs[0], _single_agg_state + _agg_states_offsets[i]);
                }
            } else if (!_is_only_group_by_columns) {
                if (false) {
                }
#define HASH_MAP_METHOD(NAME)                                                     \
    else if (_hash_map_variant.type == vectorized::AggHashMapVariant::Type::NAME) \
            _release_agg_memory<decltype(_hash_map_variant.NAME)::element_type>(_hash_map_variant.NAME.get());
                APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
            }

            _mem_pool->free_all();
        }

        // AggregateFunction::destroy depends FunctionContext.
        // so we close function context after destroy stage
        for (auto ctx : _agg_fn_ctxs) {
            if (ctx != nullptr && ctx->impl()) {
                ctx->impl()->close();
            }
        }

        Expr::close(_group_by_expr_ctxs, state);
        for (const auto& i : _agg_expr_ctxs) {
            Expr::close(i, state);
        }
        Expr::close(_conjunct_ctxs, state);
        return Status::OK();
    };
    if (_has_udaf) {
        auto promise_st = call_function_in_pthread(state, agg_close);
        promise_st->get_future().get();
    } else {
        agg_close();
    }
}

bool Aggregator::is_chunk_buffer_empty() {
    std::lock_guard<std::mutex> l(_buffer_mutex);
    return _buffer.empty();
}

vectorized::ChunkPtr Aggregator::poll_chunk_buffer() {
    std::lock_guard<std::mutex> l(_buffer_mutex);
    if (_buffer.empty()) {
        return nullptr;
    }
    vectorized::ChunkPtr chunk = _buffer.front();
    _buffer.pop();
    return chunk;
}

void Aggregator::offer_chunk_to_buffer(const vectorized::ChunkPtr& chunk) {
    std::lock_guard<std::mutex> l(_buffer_mutex);
    _buffer.push(chunk);
}

bool Aggregator::should_expand_preagg_hash_tables(size_t prev_row_returned, size_t input_chunk_size, int64_t ht_mem,
                                                  int64_t ht_rows) const {
    // Need some rows in tables to have valid statistics.
    if (ht_rows == 0) {
        return true;
    }

    // Find the appropriate reduction factor in our table for the current hash table sizes.
    int cache_level = 0;
    while (cache_level + 1 < STREAMING_HT_MIN_REDUCTION_SIZE &&
           ht_mem >= STREAMING_HT_MIN_REDUCTION[cache_level + 1].min_ht_mem) {
        cache_level++;
    }

    // Compare the number of rows in the hash table with the number of input rows that
    // were aggregated into it. Exclude passed through rows from this calculation since
    // they were not in hash tables.
    const int64_t input_rows = prev_row_returned - input_chunk_size;
    const int64_t aggregated_input_rows = input_rows - _num_rows_returned;
    double current_reduction = static_cast<double>(aggregated_input_rows) / ht_rows;

    // inaccurate, which could lead to a divide by zero below.
    if (aggregated_input_rows <= 0) {
        return true;
    }
    // Extrapolate the current reduction factor (r) using the formula
    // R = 1 + (N / n) * (r - 1), where R is the reduction factor over the full input data
    // set, N is the number of input rows, excluding passed-through rows, and n is the
    // number of rows inserted or merged into the hash tables. This is a very rough
    // approximation but is good enough to be useful.
    double min_reduction = STREAMING_HT_MIN_REDUCTION[cache_level].streaming_ht_min_reduction;
    return current_reduction > min_reduction;
}

void Aggregator::compute_single_agg_state(size_t chunk_size) {
    bool use_intermediate = _use_intermediate_as_input();
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        if (!_is_merge_funcs[i] && !use_intermediate) {
            _agg_functions[i]->update_batch_single_state(_agg_fn_ctxs[i], chunk_size, _agg_input_raw_columns[i].data(),
                                                         _single_agg_state + _agg_states_offsets[i]);
        } else {
            DCHECK_GE(_agg_input_columns[i].size(), 1);
            _agg_functions[i]->merge_batch_single_state(_agg_fn_ctxs[i], chunk_size, _agg_input_columns[i][0].get(),
                                                        _single_agg_state + _agg_states_offsets[i]);
        }
    }
}

void Aggregator::compute_batch_agg_states(size_t chunk_size) {
    bool use_intermediate = _use_intermediate_as_input();
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        if (!_is_merge_funcs[i] && !use_intermediate) {
            _agg_functions[i]->update_batch(_agg_fn_ctxs[i], chunk_size, _agg_states_offsets[i],
                                            _agg_input_raw_columns[i].data(), _tmp_agg_states.data());
        } else {
            DCHECK_GE(_agg_input_columns[i].size(), 1);
            _agg_functions[i]->merge_batch(_agg_fn_ctxs[i], _agg_input_columns[i][0]->size(), _agg_states_offsets[i],
                                           _agg_input_columns[i][0].get(), _tmp_agg_states.data());
        }
    }
}

void Aggregator::compute_batch_agg_states_with_selection(size_t chunk_size) {
    bool use_intermediate = _use_intermediate_as_input();
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        if (!_is_merge_funcs[i] && !use_intermediate) {
            _agg_functions[i]->update_batch_selectively(_agg_fn_ctxs[i], chunk_size, _agg_states_offsets[i],
                                                        _agg_input_raw_columns[i].data(), _tmp_agg_states.data(),
                                                        _streaming_selection);
        } else {
            DCHECK_GE(_agg_input_columns[i].size(), 1);
            _agg_functions[i]->merge_batch_selectively(_agg_fn_ctxs[i], _agg_input_columns[i][0]->size(),
                                                       _agg_states_offsets[i], _agg_input_columns[i][0].get(),
                                                       _tmp_agg_states.data(), _streaming_selection);
        }
    }
}

Status Aggregator::_evaluate_const_columns(int i) {
    // used for const columns.
    std::vector<ColumnPtr> const_columns;
    const_columns.reserve(_agg_expr_ctxs[i].size());
    for (int j = 0; j < _agg_expr_ctxs[i].size(); ++j) {
        ASSIGN_OR_RETURN(auto col, _agg_expr_ctxs[i][j]->root()->evaluate_const(_agg_expr_ctxs[i][j]));
        const_columns.emplace_back(std::move(col));
    }
    _agg_fn_ctxs[i]->impl()->set_constant_columns(const_columns);
    return Status::OK();
}

void Aggregator::convert_to_chunk_no_groupby(vectorized::ChunkPtr* chunk) {
    // TODO(kks): we should approve memory allocate here
    vectorized::Columns agg_result_column = _create_agg_result_columns(1);
    auto use_intermediate = _use_intermediate_as_output();
    if (!use_intermediate) {
        _finalize_to_chunk(_single_agg_state, agg_result_column);
    } else {
        _serialize_to_chunk(_single_agg_state, agg_result_column);
    }

    // For agg function column is non-nullable and table is empty
    // sum(zero_row) should be null, not 0.
    if (UNLIKELY(_num_input_rows == 0 && _group_by_expr_ctxs.empty() && !use_intermediate)) {
        for (size_t i = 0; i < _agg_fn_types.size(); i++) {
            if (_agg_fn_types[i].is_nullable) {
                agg_result_column[i] = vectorized::ColumnHelper::create_column(_agg_fn_types[i].result_type, true);
                agg_result_column[i]->append_default();
            }
        }
    }

    TupleDescriptor* tuple_desc = use_intermediate ? _intermediate_tuple_desc : _output_tuple_desc;

    vectorized::ChunkPtr result_chunk = std::make_shared<vectorized::Chunk>();
    for (size_t i = 0; i < agg_result_column.size(); i++) {
        result_chunk->append_column(std::move(agg_result_column[i]), tuple_desc->slots()[i]->id());
    }
    ++_num_rows_returned;
    ++_num_rows_processed;
    *chunk = std::move(result_chunk);
    _is_ht_eos = true;
}

void Aggregator::process_limit(vectorized::ChunkPtr* chunk) {
    if (_reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
        COUNTER_SET(_rows_returned_counter, _limit);
        _is_ht_eos = true;
        LOG(INFO) << "Aggregate Node ReachedLimit " << _limit;
    }
}

Status Aggregator::evaluate_exprs(vectorized::Chunk* chunk) {
    _set_passthrough(chunk->owner_info().is_passthrough());
    _reset_exprs();
    return _evaluate_exprs(chunk);
}

void Aggregator::output_chunk_by_streaming(vectorized::ChunkPtr* chunk) {
    // The input chunk is already intermediate-typed, so there is no need to convert it again.
    // Only when the input chunk is input-typed, we should convert it into intermediate-typed chunk.
    // is_passthrough is on indicate that the chunk is input-typed.
    auto use_intermediate = _use_intermediate_as_input();
    const auto& slots = _intermediate_tuple_desc->slots();

    vectorized::ChunkPtr result_chunk = std::make_shared<vectorized::Chunk>();
    for (size_t i = 0; i < _group_by_columns.size(); i++) {
        result_chunk->append_column(_group_by_columns[i], slots[i]->id());
    }

    if (!_agg_fn_ctxs.empty()) {
        DCHECK(!_group_by_columns.empty());
        const auto num_rows = _group_by_columns[0]->size();
        vectorized::Columns agg_result_column = _create_agg_result_columns(num_rows);
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            size_t id = _group_by_columns.size() + i;
            auto slot_id = slots[id]->id();
            if (use_intermediate) {
                DCHECK(i < _agg_input_columns.size() && _agg_input_columns[i].size() >= 1);
                result_chunk->append_column(std::move(_agg_input_columns[i][0]), slot_id);
            } else {
                _agg_functions[i]->convert_to_serialize_format(_agg_fn_ctxs[i], _agg_input_columns[i],
                                                               result_chunk->num_rows(), &agg_result_column[i]);
                result_chunk->append_column(std::move(agg_result_column[i]), slot_id);
            }
        }
    }

    _num_pass_through_rows += result_chunk->num_rows();
    _num_rows_returned += result_chunk->num_rows();
    _num_rows_processed += result_chunk->num_rows();
    *chunk = std::move(result_chunk);
    COUNTER_SET(_pass_through_row_count, _num_pass_through_rows);
}

void Aggregator::output_chunk_by_streaming_with_selection(vectorized::ChunkPtr* chunk) {
    // Streaming aggregate at least has one group by column
    size_t chunk_size = _group_by_columns[0]->size();
    for (auto& _group_by_column : _group_by_columns) {
        // Multi GroupColumn may be have the same SharedPtr
        // If ColumnSize and ChunkSize are not equal,
        // indicating that the Filter has been executed in previous GroupByColumn
        // e.g.: select c1, cast(c1 as int) from t1 group by c1, cast(c1 as int);

        // At present, the type of problem cannot be completely solved,
        // and a new solution needs to be designed to solve it completely
        if (_group_by_column->size() == chunk_size) {
            _group_by_column->filter(_streaming_selection);
        }
    }
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        for (auto& agg_input_column : _agg_input_columns[i]) {
            // AggColumn and GroupColumn may be the same SharedPtr,
            // If ColumnSize and ChunkSize are not equal,
            // indicating that the Filter has been executed in GroupByColumn
            // e.g.: select c1, count(distinct c1) from t1 group by c1;

            // At present, the type of problem cannot be completely solved,
            // and a new solution needs to be designed to solve it completely
            if (agg_input_column != nullptr && agg_input_column->size() == chunk_size) {
                agg_input_column->filter(_streaming_selection);
            }
        }
    }
    output_chunk_by_streaming(chunk);
}

#define CONVERT_TO_TWO_LEVEL_MAP(DST, SRC)                                                                             \
    if (_hash_map_variant.type == vectorized::AggHashMapVariant::Type::SRC) {                                          \
        _hash_map_variant.DST = std::make_unique<decltype(_hash_map_variant.DST)::element_type>(_state->chunk_size()); \
        _hash_map_variant.DST->hash_map.reserve(_hash_map_variant.SRC->hash_map.capacity());                           \
        _hash_map_variant.DST->hash_map.insert(_hash_map_variant.SRC->hash_map.begin(),                                \
                                               _hash_map_variant.SRC->hash_map.end());                                 \
        _hash_map_variant.type = vectorized::AggHashMapVariant::Type::DST;                                             \
        _hash_map_variant.SRC.reset();                                                                                 \
        return;                                                                                                        \
    }

#define CONVERT_TO_TWO_LEVEL_SET(DST, SRC)                                                                             \
    if (_hash_set_variant.type == vectorized::AggHashSetVariant::Type::SRC) {                                          \
        _hash_set_variant.DST = std::make_unique<decltype(_hash_set_variant.DST)::element_type>(_state->chunk_size()); \
        _hash_set_variant.DST->hash_set.reserve(_hash_set_variant.SRC->hash_set.capacity());                           \
        _hash_set_variant.DST->hash_set.insert(_hash_set_variant.SRC->hash_set.begin(),                                \
                                               _hash_set_variant.SRC->hash_set.end());                                 \
        _hash_set_variant.type = vectorized::AggHashSetVariant::Type::DST;                                             \
        _hash_set_variant.SRC.reset();                                                                                 \
        return;                                                                                                        \
    }

void Aggregator::try_convert_to_two_level_map() {
    if (_mem_tracker->consumption() > two_level_memory_threshold) {
        CONVERT_TO_TWO_LEVEL_MAP(phase1_slice_two_level, phase1_slice);
        CONVERT_TO_TWO_LEVEL_MAP(phase2_slice_two_level, phase2_slice);
    }
}

void Aggregator::try_convert_to_two_level_set() {
    if (_mem_tracker->consumption() > two_level_memory_threshold) {
        CONVERT_TO_TWO_LEVEL_SET(phase1_slice_two_level, phase1_slice);
        CONVERT_TO_TWO_LEVEL_SET(phase2_slice_two_level, phase2_slice);
    }
}

Status Aggregator::check_has_error() {
    for (const auto* ctx : _agg_fn_ctxs) {
        if (ctx->has_error()) {
            return Status::RuntimeError(ctx->error_msg());
        }
    }
    return Status::OK();
}

#undef CONVERT_TO_TWO_LEVEL

// When need finalize, create column by result type
// otherwise, create column by serde type
vectorized::Columns Aggregator::_create_agg_result_columns(size_t num_rows) {
    vectorized::Columns agg_result_columns(_agg_fn_types.size());
    auto use_intermediate = _use_intermediate_as_output();

    if (!use_intermediate) {
        for (size_t i = 0; i < _agg_fn_types.size(); ++i) {
            // For count, count distinct, bitmap_union_int such as never return null function,
            // we need to create a not-nullable column.
            agg_result_columns[i] = vectorized::ColumnHelper::create_column(
                    _agg_fn_types[i].result_type, _agg_fn_types[i].has_nullable_child & _agg_fn_types[i].is_nullable);
            agg_result_columns[i]->reserve(num_rows);
        }
    } else {
        for (size_t i = 0; i < _agg_fn_types.size(); ++i) {
            agg_result_columns[i] = vectorized::ColumnHelper::create_column(_agg_fn_types[i].serde_type,
                                                                            _agg_fn_types[i].has_nullable_child);
            agg_result_columns[i]->reserve(num_rows);
        }
    }
    return agg_result_columns;
}

vectorized::Columns Aggregator::_create_group_by_columns(size_t num_rows) {
    vectorized::Columns group_by_columns(_group_by_types.size());
    for (size_t i = 0; i < _group_by_types.size(); ++i) {
        group_by_columns[i] =
                vectorized::ColumnHelper::create_column(_group_by_types[i].result_type, _group_by_types[i].is_nullable);
        group_by_columns[i]->reserve(num_rows);
    }
    return group_by_columns;
}

void Aggregator::_serialize_to_chunk(vectorized::ConstAggDataPtr __restrict state,
                                     const vectorized::Columns& agg_result_columns) {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->serialize_to_column(_agg_fn_ctxs[i], state + _agg_states_offsets[i],
                                               agg_result_columns[i].get());
    }
}

void Aggregator::_finalize_to_chunk(vectorized::ConstAggDataPtr __restrict state,
                                    const vectorized::Columns& agg_result_columns) {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->finalize_to_column(_agg_fn_ctxs[i], state + _agg_states_offsets[i],
                                              agg_result_columns[i].get());
    }
}

void Aggregator::_reset_exprs() {
    SCOPED_TIMER(_expr_release_timer);
    for (size_t i = 0; i < _group_by_columns.size(); i++) {
        _group_by_columns[i] = nullptr;
    }

    DCHECK(_agg_input_columns.size() == _agg_fn_ctxs.size());
    for (size_t i = 0; i < _agg_input_columns.size(); i++) {
        for (size_t j = 0; j < _agg_input_columns[i].size(); j++) {
            _agg_input_columns[i][j] = nullptr;
            _agg_input_raw_columns[i][j] = nullptr;
        }
    }
}

Status Aggregator::_evaluate_exprs(vectorized::Chunk* chunk) {
    SCOPED_TIMER(_expr_compute_timer);
    // Compute group by columns
    for (size_t i = 0; i < _group_by_expr_ctxs.size(); i++) {
        ASSIGN_OR_RETURN(_group_by_columns[i], _group_by_expr_ctxs[i]->evaluate(chunk));
        DCHECK(_group_by_columns[i] != nullptr);
        if (_group_by_columns[i]->is_constant()) {
            // If group by column is constant, we disable streaming aggregate.
            // Because we don't want to send const column to exchange node
            _streaming_preaggregation_mode = TStreamingPreaggregationMode::FORCE_PREAGGREGATION;
            // All hash table could handle only null, and we don't know the real data
            // type for only null column, so we don't unpack it.
            if (!_group_by_columns[i]->only_null()) {
                vectorized::ConstColumn* const_column =
                        static_cast<vectorized::ConstColumn*>(_group_by_columns[i].get());
                const_column->data_column()->assign(chunk->num_rows(), 0);
                _group_by_columns[i] = const_column->data_column();
            }
        }
        // Scalar function compute will return non-nullable column
        // for nullable column when the real whole chunk data all not-null.
        if (_group_by_types[i].is_nullable && !_group_by_columns[i]->is_nullable()) {
            // TODO: optimized the memory usage
            _group_by_columns[i] = vectorized::NullableColumn::create(
                    _group_by_columns[i], vectorized::NullColumn::create(_group_by_columns[i]->size(), 0));
        }
    }

    // Compute agg function columns
    auto use_intermediate = _use_intermediate_as_input();
    auto& agg_expr_ctxs = use_intermediate ? _intermediate_agg_expr_ctxs : _agg_expr_ctxs;
    DCHECK(agg_expr_ctxs.size() == _agg_input_columns.size());
    DCHECK(agg_expr_ctxs.size() == _agg_fn_ctxs.size());
    for (size_t i = 0; i < agg_expr_ctxs.size(); i++) {
        for (size_t j = 0; j < agg_expr_ctxs[i].size(); j++) {
            // For simplicity and don't change the overall processing flow,
            // We handle const column as normal data column
            // TODO(kks): improve const column aggregate later
            if (j == 0) {
                ASSIGN_OR_RETURN(auto&& col, agg_expr_ctxs[i][j]->evaluate(chunk));
                _agg_input_columns[i][j] =
                        vectorized::ColumnHelper::unpack_and_duplicate_const_column(chunk->num_rows(), std::move(col));
            } else {
                ASSIGN_OR_RETURN(auto&& col, agg_expr_ctxs[i][j]->evaluate(chunk));
                _agg_input_columns[i][j] = std::move(col);
            }
            _agg_input_raw_columns[i][j] = _agg_input_columns[i][j].get();
        }
    }

    return Status::OK();
}

bool is_group_columns_fixed_size(std::vector<ExprContext*>& group_by_expr_ctxs, std::vector<ColumnType>& group_by_types,
                                 size_t* max_size, bool* has_null) {
    size_t size = 0;
    *has_null = false;

    for (size_t i = 0; i < group_by_expr_ctxs.size(); i++) {
        ExprContext* ctx = group_by_expr_ctxs[i];
        if (group_by_types[i].is_nullable) {
            *has_null = true;
            size += 1; // 1 bytes for  null flag.
        }
        PrimitiveType ptype = ctx->root()->type().type;
        size_t byte_size = get_size_of_fixed_length_type(ptype);
        if (byte_size == 0) return false;
        size += byte_size;
    }
    *max_size = size;
    return true;
}

#define CHECK_AGGR_PHASE_DEFAULT()                                                                                    \
    {                                                                                                                 \
        type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice : HashVariantType::Type::phase2_slice; \
        break;                                                                                                        \
    }

template <typename HashVariantType>
void Aggregator::_init_agg_hash_variant(HashVariantType& hash_variant) {
    auto type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice : HashVariantType::Type::phase2_slice;
    if (_has_nullable_key) {
        switch (_group_by_expr_ctxs.size()) {
        case 0:
            break;
        case 1: {
            auto group_by_expr = _group_by_expr_ctxs[0];
            switch (group_by_expr->root()->type().type) {
#define CHECK_AGGR_PHASE(TYPE, VALUE)                                                  \
    case TYPE: {                                                                       \
        type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_null_##VALUE  \
                                         : HashVariantType::Type::phase2_null_##VALUE; \
        break;                                                                         \
    }
                CHECK_AGGR_PHASE(TYPE_BOOLEAN, uint8);
                CHECK_AGGR_PHASE(TYPE_TINYINT, int8);
                CHECK_AGGR_PHASE(TYPE_SMALLINT, int16);
                CHECK_AGGR_PHASE(TYPE_INT, int32);
                CHECK_AGGR_PHASE(TYPE_DECIMAL32, decimal32);
                CHECK_AGGR_PHASE(TYPE_BIGINT, int64);
                CHECK_AGGR_PHASE(TYPE_DECIMAL64, decimal64);
                CHECK_AGGR_PHASE(TYPE_DATE, date);
                CHECK_AGGR_PHASE(TYPE_DATETIME, timestamp);
                CHECK_AGGR_PHASE(TYPE_DECIMAL128, decimal128);
                CHECK_AGGR_PHASE(TYPE_LARGEINT, int128);
                CHECK_AGGR_PHASE(TYPE_CHAR, string);
                CHECK_AGGR_PHASE(TYPE_VARCHAR, string);

#undef CHECK_AGGR_PHASE
            default:
                CHECK_AGGR_PHASE_DEFAULT();
            }
        } break;
        default:
            CHECK_AGGR_PHASE_DEFAULT();
        }
    } else {
        switch (_group_by_expr_ctxs.size()) {
        case 0:
            break;
        case 1: {
            auto group_by_expr = _group_by_expr_ctxs[0];
            switch (group_by_expr->root()->type().type) {
#define CHECK_AGGR_PHASE(TYPE, VALUE)                                             \
    case TYPE: {                                                                  \
        type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_##VALUE  \
                                         : HashVariantType::Type::phase2_##VALUE; \
        break;                                                                    \
    }
                CHECK_AGGR_PHASE(TYPE_BOOLEAN, uint8);
                CHECK_AGGR_PHASE(TYPE_TINYINT, int8);
                CHECK_AGGR_PHASE(TYPE_SMALLINT, int16);
                CHECK_AGGR_PHASE(TYPE_INT, int32);
                CHECK_AGGR_PHASE(TYPE_DECIMAL32, decimal32);
                CHECK_AGGR_PHASE(TYPE_BIGINT, int64);
                CHECK_AGGR_PHASE(TYPE_DECIMAL64, decimal64);
                CHECK_AGGR_PHASE(TYPE_DATE, date);
                CHECK_AGGR_PHASE(TYPE_DATETIME, timestamp);
                CHECK_AGGR_PHASE(TYPE_LARGEINT, int128);
                CHECK_AGGR_PHASE(TYPE_DECIMAL128, decimal128);
                CHECK_AGGR_PHASE(TYPE_CHAR, string);
                CHECK_AGGR_PHASE(TYPE_VARCHAR, string);

#undef CHECK_AGGR_PHASE

            default:
                CHECK_AGGR_PHASE_DEFAULT();
            }
        } break;
        default:
            CHECK_AGGR_PHASE_DEFAULT();
        }
    }

    bool has_null_column = false;
    int fixed_byte_size = 0;
    // this optimization don't need to be limited to multi-column group by.
    // single column like float/double/decimal/largeint could also be applied to.
    if (type == HashVariantType::Type::phase1_slice || type == HashVariantType::Type::phase2_slice) {
        size_t max_size = 0;
        if (is_group_columns_fixed_size(_group_by_expr_ctxs, _group_by_types, &max_size, &has_null_column)) {
            // we need reserve a byte for serialization length for nullable columns
            if (max_size < 4 || (!has_null_column && max_size == 4)) {
                type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice_fx4
                                                 : HashVariantType::Type::phase2_slice_fx4;
            } else if (max_size < 8 || (!has_null_column && max_size == 8)) {
                type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice_fx8
                                                 : HashVariantType::Type::phase2_slice_fx8;
            } else if (max_size < 16 || (!has_null_column && max_size == 16)) {
                type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice_fx16
                                                 : HashVariantType::Type::phase2_slice_fx16;
            }
            if (!has_null_column) {
                fixed_byte_size = max_size;
            }
        }
    }
    VLOG_ROW << "hash type is "
             << static_cast<typename std::underlying_type<typename HashVariantType::Type>::type>(type);
    hash_variant.init(_state, type);

#define SET_FIXED_SLICE_HASH_MAP_FIELD(TYPE)                  \
    if (type == HashVariantType::Type::TYPE) {                \
        hash_variant.TYPE->has_null_column = has_null_column; \
        hash_variant.TYPE->fixed_byte_size = fixed_byte_size; \
    }
    SET_FIXED_SLICE_HASH_MAP_FIELD(phase1_slice_fx4);
    SET_FIXED_SLICE_HASH_MAP_FIELD(phase1_slice_fx8);
    SET_FIXED_SLICE_HASH_MAP_FIELD(phase1_slice_fx16);
    SET_FIXED_SLICE_HASH_MAP_FIELD(phase2_slice_fx4);
    SET_FIXED_SLICE_HASH_MAP_FIELD(phase2_slice_fx8);
    SET_FIXED_SLICE_HASH_MAP_FIELD(phase2_slice_fx16);
#undef SET_FIXED_SLICE_HASH_MAP_FIELD

} // namespace starrocks
} // namespace starrocks
