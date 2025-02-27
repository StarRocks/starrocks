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

#include "exec/pipeline/sort/local_partition_topn_context.h"

#include <exec/partition/chunks_partitioner.h>

#include <utility>

#include "exec/chunks_sorter_topn.h"

namespace starrocks::pipeline {

LocalPartitionTopnContext::LocalPartitionTopnContext(const std::vector<TExpr>& t_partition_exprs, bool enable_pre_agg,
                                                     const std::vector<TExpr>& t_pre_agg_exprs,
                                                     const std::vector<TSlotId>& t_pre_agg_output_slot_id,
                                                     const std::vector<ExprContext*>& sort_exprs,
                                                     std::vector<bool> is_asc_order, std::vector<bool> is_null_first,
                                                     std::string sort_keys, int64_t offset, int64_t partition_limit,
                                                     const TTopNType::type topn_type)
        : _t_partition_exprs(t_partition_exprs),
          _enable_pre_agg(enable_pre_agg),
          _sort_exprs(sort_exprs),
          _is_asc_order(std::move(is_asc_order)),
          _is_null_first(std::move(is_null_first)),
          _sort_keys(std::move(sort_keys)),
          _offset(offset),
          _partition_limit(partition_limit),
          _topn_type(topn_type) {
    _pre_agg = std::make_unique<PreAggState>(t_pre_agg_exprs, t_pre_agg_output_slot_id);
}

Status LocalPartitionTopnContext::prepare(RuntimeState* state, RuntimeProfile* runtime_profile) {
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_partition_exprs, &_partition_exprs, state));
    RETURN_IF_ERROR(Expr::prepare(_partition_exprs, state));
    RETURN_IF_ERROR(Expr::open(_partition_exprs, state));
    for (auto& expr : _partition_exprs) {
        auto& type_desc = expr->root()->type();
        if (!type_desc.support_groupby()) {
            return Status::NotSupported(fmt::format("partition by type {} is not supported", type_desc.debug_string()));
        }
    }
    auto partition_size = _t_partition_exprs.size();
    _partition_types.resize(partition_size);
    for (auto i = 0; i < partition_size; ++i) {
        TExprNode expr = _t_partition_exprs[i].nodes[0];
        _partition_types[i].result_type = TypeDescriptor::from_thrift(expr.type);
        _partition_types[i].is_nullable = expr.is_nullable;
        _has_nullable_key = _has_nullable_key || _partition_types[i].is_nullable;
    }

    _mem_pool = std::make_unique<MemPool>();
    if (_enable_pre_agg) {
        RETURN_IF_ERROR(prepare_pre_agg(state));
    }

    _chunks_partitioner =
            std::make_unique<ChunksPartitioner>(_has_nullable_key, _partition_exprs, _partition_types, _mem_pool.get());
    return _chunks_partitioner->prepare(state, runtime_profile, _enable_pre_agg);
}

Status LocalPartitionTopnContext::prepare_pre_agg(RuntimeState* state) {
    size_t agg_size = _pre_agg->_t_pre_agg_exprs.size();
    _pre_agg->_agg_fn_ctxs.resize(agg_size);
    _pre_agg->_agg_functions.resize(agg_size);
    _pre_agg->_agg_expr_ctxs.resize(agg_size);
    _pre_agg->_agg_input_columns.resize(agg_size);
    _pre_agg->_agg_input_raw_columns.resize(agg_size);
    _pre_agg->_agg_fn_types.resize(agg_size);
    _pre_agg->_agg_states_offsets.resize(agg_size);
    for (int i = 0; i < agg_size; ++i) {
        const TExpr& desc = _pre_agg->_t_pre_agg_exprs[i];
        const TFunction& fn = desc.nodes[0].fn;

        auto num_args = desc.nodes[0].num_children;
        _pre_agg->_agg_input_columns[i].resize(num_args);
        _pre_agg->_agg_input_raw_columns[i].resize(num_args);
        int node_idx = 0;
        for (int j = 0; j < desc.nodes[0].num_children; ++j) {
            ++node_idx;
            Expr* expr = nullptr;
            ExprContext* ctx = nullptr;
            RETURN_IF_ERROR(Expr::create_tree_from_thrift_with_jit(state->obj_pool(), desc.nodes, nullptr, &node_idx,
                                                                   &expr, &ctx, state));
            _pre_agg->_agg_expr_ctxs[i].emplace_back(ctx);
        }

        TypeDescriptor return_type = TypeDescriptor::from_thrift(fn.ret_type);
        TypeDescriptor serde_type = TypeDescriptor::from_thrift(fn.aggregate_fn.intermediate_type);

        TypeDescriptor arg_type = TypeDescriptor::from_thrift(fn.arg_types[0]);

        // Collect arg_typedescs for aggregate function.
        std::vector<FunctionContext::TypeDesc> arg_typedescs;
        for (auto& type : fn.arg_types) {
            arg_typedescs.push_back(TypeDescriptor::from_thrift(type));
        }

        _pre_agg->_agg_fn_ctxs[i] = FunctionContext::create_context(state, _mem_pool.get(), return_type, arg_typedescs);
        state->obj_pool()->add(_pre_agg->_agg_fn_ctxs[i]);

        bool is_input_nullable = false;
        const AggregateFunction* func = nullptr;
        if (fn.name.function_name == "count") {
            return_type.type = TYPE_BIGINT;
            arg_type.type = TYPE_BIGINT;
            serde_type.type = TYPE_BIGINT;
            is_input_nullable = !fn.arg_types.empty() && (desc.nodes[0].has_nullable_child);
            _pre_agg->_agg_fn_types[i] = {serde_type, false, false};
        } else {
            // For nullable aggregate function(sum, max, min, avg),
            // we should always use nullable aggregate function.
            is_input_nullable = true;
            _pre_agg->_agg_fn_types[i] = {serde_type, is_input_nullable, desc.nodes[0].is_nullable};
        }

        func = get_window_function(fn.name.function_name, arg_type.type, return_type.type, is_input_nullable,
                                   fn.binary_type, state->func_version());
        if (func == nullptr) {
            return Status::InternalError(strings::Substitute("Invalid window function plan: ($0, $1, $2, $3, $4, $5)",
                                                             fn.name.function_name, arg_type.type, return_type.type,
                                                             is_input_nullable, fn.binary_type, state->func_version()));
        }
        _pre_agg->_agg_functions[i] = func;
    }

    // Compute agg state total size and offsets.
    for (int i = 0; i < agg_size; ++i) {
        _pre_agg->_agg_states_offsets[i] = _pre_agg->_agg_states_total_size;
        _pre_agg->_agg_states_total_size += _pre_agg->_agg_functions[i]->size();
        _pre_agg->_max_agg_state_align_size =
                std::max(_pre_agg->_max_agg_state_align_size, _pre_agg->_agg_functions[i]->alignof_size());

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < _pre_agg->_agg_fn_ctxs.size()) {
            size_t next_state_align_size = _pre_agg->_agg_functions[i + 1]->alignof_size();
            // Extend total_size to next alignment requirement
            // Add padding by rounding up '_agg_states_total_size' to be a multiplier of next_state_align_size.
            _pre_agg->_agg_states_total_size = (_pre_agg->_agg_states_total_size + next_state_align_size - 1) /
                                               next_state_align_size * next_state_align_size;
        }
    }

    for (const auto& ctx : _pre_agg->_agg_expr_ctxs) {
        RETURN_IF_ERROR(Expr::prepare(ctx, state));
        RETURN_IF_ERROR(Expr::open(ctx, state));
    }

    return Status::OK();
}

Status LocalPartitionTopnContext::compute_agg_state(Chunk* chunk, size_t partition_idx) {
    DCHECK(_enable_pre_agg);

    RETURN_IF_ERROR(_evaluate_agg_input_columns(chunk));

    for (size_t i = 0; i < _pre_agg->_agg_fn_ctxs.size(); i++) {
        _pre_agg->_agg_functions[i]->update_batch_single_state(
                _pre_agg->_agg_fn_ctxs[i], chunk->num_rows(), _pre_agg->_agg_input_raw_columns[i].data(),
                _pre_agg->_managed_fn_states[partition_idx]->mutable_data() + _pre_agg->_agg_states_offsets[i]);
    }

    return Status::OK();
}

Status LocalPartitionTopnContext::push_one_chunk_to_partitioner(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_chunks_partitioner->offer<true>(
            chunk,
            [this, state](size_t partition_idx) {
                _chunks_sorters.emplace_back(std::make_shared<ChunksSorterTopn>(
                        state, &_sort_exprs, &_is_asc_order, &_is_null_first, _sort_keys, _offset, _partition_limit,
                        _topn_type, ChunksSorterTopn::kDefaultMaxBufferRows, ChunksSorterTopn::kDefaultMaxBufferBytes,
                        ChunksSorterTopn::tunning_buffered_chunks(_partition_limit)));
                // create agg state for new partition
                if (_enable_pre_agg) {
                    AggDataPtr agg_states = _mem_pool->allocate_aligned(_pre_agg->_agg_states_total_size,
                                                                        _pre_agg->_max_agg_state_align_size);
                    _pre_agg->_managed_fn_states.emplace_back(std::make_unique<ManagedFunctionStates<PreAggState>>(
                            &_pre_agg->_agg_fn_ctxs, agg_states, _pre_agg.get()));
                }
            },
            [this, state](size_t partition_idx, const ChunkPtr& chunk) {
                (void)_chunks_sorters[partition_idx]->update(state, chunk);
                if (_enable_pre_agg) {
                    (void)compute_agg_state(chunk.get(), partition_idx);
                }
            }));
    if (_chunks_partitioner->is_passthrough()) {
        RETURN_IF_ERROR(transfer_all_chunks_from_partitioner_to_sorters(state));
    }
    return Status::OK();
}

void LocalPartitionTopnContext::sink_complete() {
    // when enable_pre_agg, mempool will allocate agg state which is used for source op
    if (!_enable_pre_agg) {
        _mem_pool.reset();
    }
    _is_sink_complete = true;
}

Status LocalPartitionTopnContext::transfer_all_chunks_from_partitioner_to_sorters(RuntimeState* state) {
    if (_is_transfered) {
        return Status::OK();
    }

    _partition_num = _chunks_partitioner->num_partitions();
    RETURN_IF_ERROR(
            _chunks_partitioner->consume_from_hash_map([this, state](int32_t partition_idx, const ChunkPtr& chunk) {
                (void)_chunks_sorters[partition_idx]->update(state, chunk);
                if (_enable_pre_agg) {
                    (void)compute_agg_state(chunk.get(), partition_idx);
                }
                return true;
            }));

    for (auto& chunks_sorter : _chunks_sorters) {
        RETURN_IF_ERROR(chunks_sorter->done(state));
    }

    _is_transfered = true;
    return Status::OK();
}

bool LocalPartitionTopnContext::has_output() {
    if (_chunks_partitioner->is_passthrough() && _is_transfered) {
        return _sorter_index < _chunks_sorters.size() || !_chunks_partitioner->is_passthrough_buffer_empty();
    }
    return _is_sink_complete && _sorter_index < _chunks_sorters.size();
}

bool LocalPartitionTopnContext::is_finished() {
    if (!_is_sink_complete) {
        return false;
    }
    return !has_output();
}

StatusOr<ChunkPtr> LocalPartitionTopnContext::pull_one_chunk() {
    ChunkPtr chunk = nullptr;
    if (_sorter_index < _chunks_sorters.size()) {
        ASSIGN_OR_RETURN(chunk, pull_one_chunk_from_sorters());
        if (chunk != nullptr) {
            return chunk;
        }
    }
    chunk = _chunks_partitioner->consume_from_passthrough_buffer();
    if (_enable_pre_agg && chunk != nullptr) {
        RETURN_IF_ERROR(output_agg_streaming(chunk.get()));
    }
    return chunk;
}

StatusOr<ChunkPtr> LocalPartitionTopnContext::pull_one_chunk_from_sorters() {
    auto& chunks_sorter = _chunks_sorters[_sorter_index];
    ChunkPtr chunk = nullptr;
    bool eos = false;
    RETURN_IF_ERROR(chunks_sorter->get_next(&chunk, &eos));

    if (_enable_pre_agg) {
        output_agg_result(chunk.get(), eos, _pre_agg->_is_first_chunk_of_current_sorter);
    }
    _pre_agg->_is_first_chunk_of_current_sorter = false;

    if (eos) {
        _pre_agg->_is_first_chunk_of_current_sorter = true;
        // Current sorter has no output, try to get chunk from next sorter
        _sorter_index++;
    }
    return chunk;
}

Columns LocalPartitionTopnContext::_create_agg_result_columns(size_t num_rows) {
    Columns agg_result_columns(_pre_agg->_agg_fn_types.size());
    for (size_t i = 0; i < _pre_agg->_agg_fn_types.size(); ++i) {
        // For count, count distinct, bitmap_union_int such as never return null function,
        // we need to create a not-nullable column.
        agg_result_columns[i] = ColumnHelper::create_column(_pre_agg->_agg_fn_types[i].result_type,
                                                            _pre_agg->_agg_fn_types[i].is_nullable);
        agg_result_columns[i]->reserve(num_rows);
    }
    return agg_result_columns;
}

void LocalPartitionTopnContext::output_agg_result(Chunk* chunk, bool eos, bool is_first_chunk) {
    // when eos, chunk is nullptr, just do nothing
    if (eos || chunk == nullptr || chunk->num_rows() < 1) return;

    auto agg_state = _pre_agg->_managed_fn_states[_sorter_index]->mutable_data();

    Columns agg_result_columns = _create_agg_result_columns(chunk->num_rows());

    if (is_first_chunk) {
        for (size_t i = 0; i < _pre_agg->_agg_fn_ctxs.size(); i++) {
            // add agg result into first row
            _pre_agg->_agg_functions[i]->serialize_to_column(_pre_agg->_agg_fn_ctxs[i],
                                                             agg_state + _pre_agg->_agg_states_offsets[i],
                                                             agg_result_columns[i].get());
            // reset agg result, after first row, we only append 'reset' result
            _pre_agg->_agg_functions[i]->reset(_pre_agg->_agg_fn_ctxs[i], _pre_agg->_agg_input_columns[i],
                                               agg_state + _pre_agg->_agg_states_offsets[i]);
        }
    }

    size_t num_default_rows = is_first_chunk ? chunk->num_rows() - 1 : chunk->num_rows();

    for (size_t i = 0; i < _pre_agg->_agg_fn_types.size(); ++i) {
        // add 'reset' rows
        for (size_t j = 0; j < num_default_rows; j++) {
            _pre_agg->_agg_functions[i]->serialize_to_column(_pre_agg->_agg_fn_ctxs[i],
                                                             agg_state + _pre_agg->_agg_states_offsets[i],
                                                             agg_result_columns[i].get());
        }
    }

    for (size_t i = 0; i < _pre_agg->_agg_fn_ctxs.size(); i++) {
        chunk->append_column(std::move(agg_result_columns[i]), _pre_agg->_t_pre_agg_output_slot_id[i]);
    }
}

Status LocalPartitionTopnContext::output_agg_streaming(Chunk* chunk) {
    RETURN_IF_ERROR(_evaluate_agg_input_columns(chunk));
    Columns agg_result_column = _create_agg_result_columns(chunk->num_rows());
    for (size_t i = 0; i < _pre_agg->_agg_fn_ctxs.size(); i++) {
        auto slot_id = _pre_agg->_t_pre_agg_output_slot_id[i];
        _pre_agg->_agg_functions[i]->convert_to_serialize_format(
                _pre_agg->_agg_fn_ctxs[i], _pre_agg->_agg_input_columns[i], chunk->num_rows(), &agg_result_column[i]);
        chunk->append_column(std::move(agg_result_column[i]), slot_id);
    }
    return Status::OK();
}

Status LocalPartitionTopnContext::_evaluate_agg_input_columns(Chunk* chunk) {
    for (size_t i = 0; i < _pre_agg->_agg_fn_ctxs.size(); i++) {
        for (size_t j = 0; j < _pre_agg->_agg_expr_ctxs[i].size(); j++) {
            // For simplicity and don't change the overall processing flow,
            // We handle const column as normal data column
            // TODO(kks): improve const column aggregate later
            ASSIGN_OR_RETURN(auto&& col, _pre_agg->_agg_expr_ctxs[i][j]->evaluate(chunk));
            // if first column is const, we have to unpack it. Most agg function only has one arg, and treat it as non-const column
            if (j == 0) {
                _pre_agg->_agg_input_columns[i][j] =
                        ColumnHelper::unpack_and_duplicate_const_column(chunk->num_rows(), col);
            } else {
                // if function has at least two argument, unpack const column selectively
                // for function like corr, FE forbid second args to be const, we will always unpack const column for it
                // for function like percentile_disc, the second args is const, do not unpack it
                if (_pre_agg->_agg_expr_ctxs[i][j]->root()->is_constant()) {
                    _pre_agg->_agg_input_columns[i][j] = std::move(col);
                } else {
                    _pre_agg->_agg_input_columns[i][j] =
                            ColumnHelper::unpack_and_duplicate_const_column(chunk->num_rows(), col);
                }
            }
            _pre_agg->_agg_input_raw_columns[i][j] = _pre_agg->_agg_input_columns[i][j].get();
        }
    }
    return Status::OK();
}

LocalPartitionTopnContextFactory::LocalPartitionTopnContextFactory(
        RuntimeState*, const TTopNType::type topn_type, bool is_merging, const std::vector<ExprContext*>& sort_exprs,
        std::vector<bool> is_asc_order, std::vector<bool> is_null_first, const std::vector<TExpr>& t_partition_exprs,
        bool enable_pre_agg, const std::vector<TExpr>& t_pre_agg_exprs,
        const std::vector<TSlotId>& t_pre_agg_output_slot_id, int64_t offset, int64_t limit, std::string sort_keys,
        const std::vector<OrderByType>& order_by_types, const std::vector<RuntimeFilterBuildDescriptor*>&)
        : _topn_type(topn_type),
          _sort_exprs(sort_exprs),
          _is_asc_order(std::move(is_asc_order)),
          _is_null_first(std::move(is_null_first)),
          _t_partition_exprs(t_partition_exprs),
          enable_pre_agg(enable_pre_agg),
          _t_pre_agg_exprs(t_pre_agg_exprs),
          _t_pre_agg_output_slot_id(t_pre_agg_output_slot_id),
          _offset(offset),
          _partition_limit(limit),
          _sort_keys(std::move(sort_keys)) {}

LocalPartitionTopnContext* LocalPartitionTopnContextFactory::create(int32_t driver_sequence) {
    if (auto it = _ctxs.find(driver_sequence); it != _ctxs.end()) {
        return it->second.get();
    }

    auto ctx = std::make_shared<LocalPartitionTopnContext>(
            _t_partition_exprs, enable_pre_agg, _t_pre_agg_exprs, _t_pre_agg_output_slot_id, _sort_exprs, _is_asc_order,
            _is_null_first, _sort_keys, _offset, _partition_limit, _topn_type);
    auto* ctx_raw_ptr = ctx.get();
    _ctxs.emplace(driver_sequence, std::move(ctx));
    return ctx_raw_ptr;
}

Status LocalPartitionTopnContextFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_sort_exprs, state));
    RETURN_IF_ERROR(Expr::open(_sort_exprs, state));
    return Status::OK();
}

} // namespace starrocks::pipeline
