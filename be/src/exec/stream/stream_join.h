// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/scan_node.h"
#include "exec/stream/imt_state_table.h"
#include "exec/stream/lookupjoin/lookup_join_context.h"
#include "exec/vectorized/join_hash_map.h"
#include "exprs/vectorized/column_ref.h"

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

    std::vector<pipeline::LookupJoinKeyDesc> _join_key_descs;
    std::shared_ptr<pipeline::LookupJoinContext> _lookup_join_context;
    std::set<SlotId> _output_slots;
    RowDescriptor _left_row_desc;
    RowDescriptor _right_row_desc;

    IMTStateTablePtr _lhs_imt;
    IMTStateTablePtr _rhs_imt;
};

} // namespace starrocks
