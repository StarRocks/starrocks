// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/sort/sort_context.h"
#include "exec/sort_exec_exprs.h"
#include "exec/vectorized/chunks_sorter.h"

namespace starrocks {
class BufferControlBlock;
class ExprContext;
class ResultWriter;
class ExecNode;

namespace vectorized {
class ChunksSorter;
}

namespace pipeline {
using namespace vectorized;

/*
 * Partiton Sort Operator is almost like Sort Operator,
 * except that it is used to sort for partial data, 
 * thus through multiple instances to provide data parallelism.
 */
class PartitionSortSinkOperator final : public Operator {
public:
    PartitionSortSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                              std::shared_ptr<vectorized::ChunksSorter> chunks_sorter, SortExecExprs sort_exec_exprs,
                              const std::vector<OrderByType>& order_by_types, TupleDescriptor* materialized_tuple_desc,
                              const RowDescriptor& parent_node_row_desc,
                              const RowDescriptor& parent_node_child_row_desc, SortContext* sort_context)
            : Operator(factory, id, "partition_sort_sink", plan_node_id, driver_sequence),
              _chunks_sorter(std::move(chunks_sorter)),
              _sort_exec_exprs(std::move(sort_exec_exprs)),
              _order_by_types(order_by_types),
              _materialized_tuple_desc(materialized_tuple_desc),
              _parent_node_row_desc(parent_node_row_desc),
              _parent_node_child_row_desc(parent_node_child_row_desc),
              _sort_context(sort_context) {
        _sort_context->ref();
    }

    ~PartitionSortSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override { return !is_finished(); }

    bool is_finished() const override { return _is_finished || _sort_context->is_finished(); }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    Status set_finishing(RuntimeState* state) override;

private:
    bool _is_finished = false;

    std::shared_ptr<vectorized::ChunksSorter> _chunks_sorter;

    // from topn
    // _sort_exec_exprs contains the ordering expressions
    SortExecExprs _sort_exec_exprs;
    const std::vector<OrderByType>& _order_by_types;

    // Cached descriptor for the materialized tuple. Assigned in Prepare().
    TupleDescriptor* _materialized_tuple_desc;

    // Used to get needed data from TopNNode.
    const RowDescriptor& _parent_node_row_desc;
    const RowDescriptor& _parent_node_child_row_desc;
    SortContext* _sort_context;
};

class PartitionSortSinkOperatorFactory final : public OperatorFactory {
public:
    PartitionSortSinkOperatorFactory(
            int32_t id, int32_t plan_node_id, std::shared_ptr<SortContextFactory> sort_context_factory,
            SortExecExprs& sort_exec_exprs, std::vector<bool> is_asc_order, std::vector<bool> is_null_first,
            const std::string& sort_keys, int64_t offset, int64_t limit, const TTopNType::type topn_type,
            const std::vector<OrderByType>& order_by_types, TupleDescriptor* materialized_tuple_desc,
            const RowDescriptor& parent_node_row_desc, const RowDescriptor& parent_node_child_row_desc,
            const std::vector<ExprContext*>& analytic_partition_exprs)
            : OperatorFactory(id, "partition_sort_sink", plan_node_id),
              _sort_context_factory(sort_context_factory),
              _sort_exec_exprs(sort_exec_exprs),
              _is_asc_order(is_asc_order),
              _is_null_first(is_null_first),
              _sort_keys(sort_keys),
              _offset(offset),
              _limit(limit),
              _topn_type(topn_type),
              _order_by_types(order_by_types),
              _materialized_tuple_desc(materialized_tuple_desc),
              _parent_node_row_desc(parent_node_row_desc),
              _parent_node_child_row_desc(parent_node_child_row_desc),
              _analytic_partition_exprs(analytic_partition_exprs) {}

    ~PartitionSortSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::shared_ptr<SortContextFactory> _sort_context_factory;
    // _sort_exec_exprs contains the ordering expressions
    SortExecExprs& _sort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
    const std::string _sort_keys;
    int64_t _offset;
    int64_t _limit;
    const TTopNType::type _topn_type;
    const std::vector<OrderByType>& _order_by_types;

    // Cached descriptor for the materialized tuple. Assigned in Prepare().
    TupleDescriptor* _materialized_tuple_desc;

    // Used to get needed data from TopNNode.
    const RowDescriptor& _parent_node_row_desc;
    const RowDescriptor& _parent_node_child_row_desc;
    std::vector<ExprContext*> _analytic_partition_exprs;
};

} // namespace pipeline
} // namespace starrocks
