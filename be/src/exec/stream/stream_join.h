// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/scan_node.h"

namespace starrocks {

class StreamJoinNode final : public starrocks::ExecNode {
public:
    StreamJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs) {}
    ~StreamJoinNode() override {}

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    TJoinOp::type _join_op;
    std::string _sql_join_conjuncts;
    std::vector<ExprContext*> _probe_expr_ctxs;
    std::vector<ExprContext*> _build_expr_ctxs;
    std::vector<ExprContext*> _other_join_conjunct_ctxs;
};

class StreamJoinOperator final : public pipeline::Operator {
public:
    StreamJoinOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_seq)
            : Operator(factory, id, "stream_join", plan_node_id, driver_seq) {}

    ~StreamJoinOperator() override = default;

    // Setup
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    // Control flow
    bool is_finished() const override;
    bool has_output() const override;
    bool need_input() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    // Data flow
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
};

class StreamJoinOperatorFactory final : public pipeline::OperatorFactory {
public:
    StreamJoinOperatorFactory(int32_t id, int32_t plan_node_id, const RowDescriptor& row_descriptor,
                              const RowDescriptor& left_row_desc, const RowDescriptor& right_row_desc,
                              const std::string& sql_join_conjuncts, TJoinOp::type join_op,
                              const std::vector<ExprContext*>& probe_expr_conjuncts,
                              const std::vector<ExprContext*>& build_expr_conjuncts,
                              const std::vector<ExprContext*>& other_expr_conjuncts)
            : OperatorFactory(id, "stream_join", plan_node_id),
              _join_op(join_op),
              _row_descriptor(row_descriptor),
              _left_row_desc(left_row_desc),
              _right_row_desc(right_row_desc),
              _sql_join_conjuncts(sql_join_conjuncts),
              _probe_eq_exprs(probe_expr_conjuncts),
              _build_eq_exprs(build_expr_conjuncts),
              _other_join_conjunct_exprs(build_expr_conjuncts) {}

    ~StreamJoinOperatorFactory() override = default;

    pipeline::OperatorPtr create(int32_t dop, int32_t driver_seq) override;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    const TJoinOp::type _join_op;
    const RowDescriptor& _row_descriptor;
    const RowDescriptor& _left_row_desc;
    const RowDescriptor& _right_row_desc;

    std::string _sql_join_conjuncts;
    std::vector<ExprContext*> _probe_eq_exprs;
    std::vector<ExprContext*> _build_eq_exprs;
    std::vector<ExprContext*> _other_join_conjunct_exprs;
};

} // namespace starrocks