// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/sort/local_partition_topn_context.h"

#include <exec/vectorized/partition/chunks_partitioner.h>

#include "exec/vectorized/chunks_sorter_topn.h"

namespace starrocks::pipeline {

LocalPartitionTopnContext::LocalPartitionTopnContext(
        const std::vector<TExpr>& t_partition_exprs, SortExecExprs& sort_exec_exprs, std::vector<bool> is_asc_order,
        std::vector<bool> is_null_first, const std::string& sort_keys, int64_t offset, int64_t limit,
        const std::vector<OrderByType>& order_by_types, TupleDescriptor* materialized_tuple_desc,
        const RowDescriptor& parent_node_row_desc, const RowDescriptor& parent_node_child_row_desc)
        : _t_partition_exprs(t_partition_exprs),
          _sort_exec_exprs(sort_exec_exprs),
          _is_asc_order(is_asc_order),
          _is_null_first(is_null_first),
          _sort_keys(sort_keys),
          _offset(offset),
          _limit(limit),
          _order_by_types(order_by_types),
          _materialized_tuple_desc(materialized_tuple_desc),
          _parent_node_row_desc(parent_node_row_desc),
          _parent_node_child_row_desc(parent_node_child_row_desc) {}

Status LocalPartitionTopnContext::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_partition_exprs, &_partition_exprs));
    auto partition_size = _t_partition_exprs.size();
    _partition_types.resize(partition_size);
    for (auto i = 0; i < partition_size; ++i) {
        TExprNode expr = _t_partition_exprs[i].nodes[0];
        _partition_types[i].result_type = TypeDescriptor::from_thrift(expr.type);
        _partition_types[i].is_nullable = expr.is_nullable;
        _has_nullable_key = _has_nullable_key || _partition_types[i].is_nullable;
    }

    _chunks_partitioner =
            std::make_unique<vectorized::ChunksPartitioner>(_has_nullable_key, _partition_exprs, _partition_types);
    return _chunks_partitioner->prepare(state);
}

Status LocalPartitionTopnContext::push_one_chunk_to_partitioner(const vectorized::ChunkPtr& chunk) {
    return _chunks_partitioner->offer(chunk);
}

void LocalPartitionTopnContext::sink_complete() {
    _is_sink_complete = true;
}

Status LocalPartitionTopnContext::transfer_all_chunks_from_partitioner_to_sorters(RuntimeState* state) {
    const auto num_partitions = _chunks_partitioner->num_partitions();
    _chunks_sorters.resize(num_partitions);
    for (int i = 0; i < num_partitions; ++i) {
        _chunks_sorters[i] = std::make_shared<vectorized::ChunksSorterTopn>(
                state, &_sort_exec_exprs.lhs_ordering_expr_ctxs(), &_is_asc_order, &_is_null_first, _sort_keys, _offset,
                _limit, vectorized::ChunksSorterTopn::tunning_buffered_chunks(_limit));
    }
    RETURN_IF_ERROR(
            _chunks_partitioner->accept([this, state](int32_t partition_idx, const vectorized::ChunkPtr& chunk) {
                _chunks_sorters[partition_idx]->update(state, chunk);
                return true;
            }));

    for (auto& chunks_sorter : _chunks_sorters) {
        RETURN_IF_ERROR(chunks_sorter->done(state));
    }

    return Status::OK();
}

bool LocalPartitionTopnContext::has_output() {
    return _is_sink_complete && _sorter_index < _chunks_sorters.size();
}

bool LocalPartitionTopnContext::is_finished() {
    if (!_is_sink_complete) {
        return false;
    }
    return !has_output();
}

StatusOr<vectorized::ChunkPtr> LocalPartitionTopnContext::pull_one_chunk_from_sorters() {
    auto& chunks_sorter = _chunks_sorters[_sorter_index];
    vectorized::ChunkPtr chunk;
    if (chunks_sorter->pull_chunk(&chunk)) {
        // Current sorter has no output, try to get chunk from next sorter
        _sorter_index++;
    }
    return chunk;
}

LocalPartitionTopnContextFactory::LocalPartitionTopnContextFactory(
        const int32_t degree_of_parallelism, const std::vector<TExpr>& t_partition_exprs,
        SortExecExprs& sort_exec_exprs, std::vector<bool> is_asc_order, std::vector<bool> is_null_first,
        const std::string& sort_keys, int64_t offset, int64_t limit, const std::vector<OrderByType>& order_by_types,
        TupleDescriptor* materialized_tuple_desc, const RowDescriptor& parent_node_row_desc,
        const RowDescriptor& parent_node_child_row_desc)
        : _ctxs(degree_of_parallelism),
          _t_partition_exprs(t_partition_exprs),
          _sort_exec_exprs(sort_exec_exprs),
          _is_asc_order(is_asc_order),
          _is_null_first(is_null_first),
          _sort_keys(sort_keys),
          _offset(offset),
          _limit(limit),
          _order_by_types(order_by_types),
          _materialized_tuple_desc(materialized_tuple_desc),
          _parent_node_row_desc(parent_node_row_desc),
          _parent_node_child_row_desc(parent_node_child_row_desc) {}

LocalPartitionTopnContext* LocalPartitionTopnContextFactory::create(int32_t driver_sequence) {
    DCHECK_LT(driver_sequence, _ctxs.size());

    if (_ctxs[driver_sequence] == nullptr) {
        _ctxs[driver_sequence] = std::make_shared<LocalPartitionTopnContext>(
                _t_partition_exprs, _sort_exec_exprs, _is_asc_order, _is_null_first, _sort_keys, _offset, _limit,
                _order_by_types, _materialized_tuple_desc, _parent_node_row_desc, _parent_node_child_row_desc);
    }

    return _ctxs[driver_sequence].get();
}
} // namespace starrocks::pipeline
