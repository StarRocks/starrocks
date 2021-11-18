// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/sort/sort_context.h"
#include "exec/sort_exec_exprs.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"

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
    PartitionSortSinkOperator(int32_t id, int32_t plan_node_id, std::shared_ptr<vectorized::ChunksSorter> chunks_sorter,
                              SortExecExprs sort_exec_exprs, const std::vector<OrderByType>& order_by_types,
                              TupleDescriptor* materialized_tuple_desc, const RowDescriptor& parent_node_row_desc,
                              const RowDescriptor& parent_node_child_row_desc, SortContext* sort_context)
            : Operator(id, "partition_sort_sink", plan_node_id),
              _chunks_sorter(std::move(chunks_sorter)),
              _sort_exec_exprs(std::move(sort_exec_exprs)),
              _order_by_types(order_by_types),
              _materialized_tuple_desc(materialized_tuple_desc),
              _parent_node_row_desc(parent_node_row_desc),
              _parent_node_child_row_desc(parent_node_child_row_desc),
              _sort_context(sort_context) {
        _sort_context->create_one_operator();
    }

    ~PartitionSortSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override { return !is_finished(); }

    bool is_finished() const override { return _is_finished || _sort_context->is_finished(); }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    void set_finishing(RuntimeState* state) override;

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
    PartitionSortSinkOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<SortContext> sort_context,
                                     SortExecExprs& sort_exec_exprs, std::vector<bool> is_asc_order,
                                     std::vector<bool> is_null_first, int64_t offset, int64_t limit,
                                     const std::vector<OrderByType>& order_by_types,
                                     TupleDescriptor* materialized_tuple_desc,
                                     const RowDescriptor& parent_node_row_desc,
                                     const RowDescriptor& parent_node_child_row_desc)
            : OperatorFactory(id, "partition_sort_sink", plan_node_id),
              _sort_context(sort_context),
              _sort_exec_exprs(sort_exec_exprs),
              _is_asc_order(is_asc_order),
              _is_null_first(is_null_first),
              _offset(offset),
              _limit(limit),
              _order_by_types(order_by_types),
              _materialized_tuple_desc(materialized_tuple_desc),
              _parent_node_row_desc(parent_node_row_desc),
              _parent_node_child_row_desc(parent_node_child_row_desc) {}

    ~PartitionSortSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        static const uint SIZE_OF_CHUNK_FOR_TOPN = 3000;
        static const uint SIZE_OF_CHUNK_FOR_FULL_SORT = 5000;

        std::shared_ptr<ChunksSorter> chunks_sorter;
        if (_limit >= 0) {
            chunks_sorter = std::make_unique<vectorized::ChunksSorterTopn>(&(_sort_exec_exprs.lhs_ordering_expr_ctxs()),
                                                                           &_is_asc_order, &_is_null_first, _offset,
                                                                           _limit, SIZE_OF_CHUNK_FOR_TOPN);
        } else {
            chunks_sorter = std::make_unique<vectorized::ChunksSorterFullSort>(
                    &(_sort_exec_exprs.lhs_ordering_expr_ctxs()), &_is_asc_order, &_is_null_first,
                    SIZE_OF_CHUNK_FOR_FULL_SORT);
        }

        _sort_context->add_partition_chunks_sorter(chunks_sorter);
        auto ope = std::make_shared<PartitionSortSinkOperator>(
                _id, _plan_node_id, chunks_sorter, _sort_exec_exprs, _order_by_types, _materialized_tuple_desc,
                _parent_node_row_desc, _parent_node_child_row_desc, _sort_context.get());
        return ope;
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::shared_ptr<SortContext> _sort_context;
    // _sort_exec_exprs contains the ordering expressions
    SortExecExprs& _sort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
    int64_t _offset;
    int64_t _limit;
    const std::vector<OrderByType>& _order_by_types;

    // Cached descriptor for the materialized tuple. Assigned in Prepare().
    TupleDescriptor* _materialized_tuple_desc;

    // Used to get needed data from TopNNode.
    const RowDescriptor& _parent_node_row_desc;
    const RowDescriptor& _parent_node_child_row_desc;
};

} // namespace pipeline
} // namespace starrocks
