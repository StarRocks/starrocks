// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "exec/exec_node.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

// UNION ALL operator has three kinds of sub-node as follows:
// 1. Passthrough.
//    The src column from sub-node is projected to the dest column without expressions.
//    A src column may be projected to the multiple dest columns.
//    *UnionPassthroughOperator* is used for this case.
// 2. Materialize.
//    The src column is projected to the dest column with expressions.
//    *ProjectOperator* is used for this case.
// 3. Const.
//    Use the evaluation result of const expressions WITHOUT sub-node as the dest column.
//    Each expression is projected to the one dest row.
//    *UnionConstSourceOperator* is used for this case.

// UnionConstSourceOperator is for the Const kind of sub-node.
class UnionConstSourceOperator final : public SourceOperator {
public:
    UnionConstSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                             const std::vector<SlotDescriptor*>& dst_slots,
                             const std::vector<ExprContext*>* const const_expr_lists, const size_t rows_total)
            : SourceOperator(factory, id, "union_const_source", plan_node_id, driver_sequence),
              _dst_slots(dst_slots),
              _const_expr_lists(DCHECK_NOTNULL(const_expr_lists)),
              _rows_total(rows_total) {}

    bool has_output() const override { return _next_processed_row_index < _rows_total; }

    bool is_finished() const override { return !has_output(); };

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    const std::vector<SlotDescriptor*>& _dst_slots;

    // The evaluation of each const expr_list is projected to ONE dest row.
    // It references to the part of the UnionConstSourceOperatorFactory::_const_expr_lists.
    const std::vector<ExprContext*>* const _const_expr_lists;
    const size_t _rows_total;
    size_t _next_processed_row_index = 0;
};

class UnionConstSourceOperatorFactory final : public SourceOperatorFactory {
public:
    UnionConstSourceOperatorFactory(int32_t id, int32_t plan_node_id, const std::vector<SlotDescriptor*>& dst_slots,
                                    const std::vector<std::vector<ExprContext*>>& const_expr_lists)
            : SourceOperatorFactory(id, "union_const_source", plan_node_id),
              _dst_slots(dst_slots),
              _const_expr_lists(const_expr_lists) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        // Divide _const_expr_lists into *degree_of_parallelism* parts,
        // each of which contains *num_rows_per_driver* continuous rows except the last part.
        size_t rows_total = _const_expr_lists.size();
        size_t num_rows_per_driver = (rows_total + degree_of_parallelism - 1) / degree_of_parallelism;
        size_t rows_offset = num_rows_per_driver * driver_sequence;
        DCHECK(rows_total > rows_offset);
        size_t rows_count = std::min(num_rows_per_driver, rows_total - rows_offset);

        return std::make_shared<UnionConstSourceOperator>(this, _id, _plan_node_id, driver_sequence, _dst_slots,
                                                          _const_expr_lists.data() + rows_offset, rows_count);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    const std::vector<SlotDescriptor*>& _dst_slots;

    // The evaluation of each const expr_list is projected to ONE dest row.
    const std::vector<std::vector<ExprContext*>>& _const_expr_lists;
};

} // namespace starrocks::pipeline
