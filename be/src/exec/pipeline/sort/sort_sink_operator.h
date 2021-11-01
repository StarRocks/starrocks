// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator.h"
#include "exec/sort_exec_exprs.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/mysql_result_writer.h"
#include "util/stack_util.h"

namespace starrocks {
class BufferControlBlock;
class ExprContext;
class ResultWriter;
class ExecNode;

namespace vectorized {
class ChunksSorter;
}

namespace pipeline {
class SortSinkOperator final : public Operator {
public:
    SortSinkOperator(int32_t id, int32_t plan_node_id, std::shared_ptr<vectorized::ChunksSorter> chunks_sorter,
                     SortExecExprs sort_exec_exprs, const std::vector<OrderByType>& order_by_types,
                     TupleDescriptor* materialized_tuple_desc, const RowDescriptor& parent_node_row_desc,
                     const RowDescriptor& parent_node_child_row_desc)
            : Operator(id, "sort_sink", plan_node_id),
              _chunks_sorter(std::move(chunks_sorter)),
              _sort_exec_exprs(std::move(sort_exec_exprs)),
              _order_by_types(order_by_types),
              _materialized_tuple_desc(materialized_tuple_desc),
              _parent_node_row_desc(parent_node_row_desc),
              _parent_node_child_row_desc(parent_node_child_row_desc) {}

    ~SortSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished; }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    void finish(RuntimeState* state) override;

private:
    // This method is the same as topn node.
    vectorized::ChunkPtr _materialize_chunk_before_sort(vectorized::Chunk* chunk);
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
};

class SortSinkOperatorFactory final : public OperatorFactory {
public:
    SortSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                            const std::shared_ptr<vectorized::ChunksSorter>& chunks_sorter,
                            SortExecExprs& sort_exec_exprs, const std::vector<OrderByType>& order_by_types,
                            TupleDescriptor* materialized_tuple_desc, const RowDescriptor& parent_node_row_desc,
                            const RowDescriptor& parent_node_child_row_desc)
            : OperatorFactory(id, "sort_sink", plan_node_id),
              _chunks_sorter(std::move(chunks_sorter)),
              _sort_exec_exprs(sort_exec_exprs),
              _order_by_types(order_by_types),
              _materialized_tuple_desc(materialized_tuple_desc),
              _parent_node_row_desc(parent_node_row_desc),
              _parent_node_child_row_desc(parent_node_child_row_desc) {}

    ~SortSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        auto ope = std::make_shared<SortSinkOperator>(_id, _plan_node_id, _chunks_sorter, _sort_exec_exprs,
                                                      _order_by_types, _materialized_tuple_desc, _parent_node_row_desc,
                                                      _parent_node_child_row_desc);
        return ope;
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::shared_ptr<vectorized::ChunksSorter> _chunks_sorter;

    // _sort_exec_exprs contains the ordering expressions
    SortExecExprs& _sort_exec_exprs;
    const std::vector<OrderByType>& _order_by_types;

    // Cached descriptor for the materialized tuple. Assigned in Prepare().
    TupleDescriptor* _materialized_tuple_desc;

    // Used to get needed data from TopNNode.
    const RowDescriptor& _parent_node_row_desc;
    const RowDescriptor& _parent_node_child_row_desc;
};

} // namespace pipeline
} // namespace starrocks
