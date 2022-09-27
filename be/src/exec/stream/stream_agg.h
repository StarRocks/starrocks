// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/exec_node.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks {

class StreamAggNode final : public starrocks::ExecNode {
public:
    StreamAggNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs), _tnode(tnode) {}
    ~StreamAggNode() override {}
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    std::vector<std::shared_ptr<pipeline::OperatorFactory> > decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    const TPlanNode& _tnode;
    // _group_by_expr_ctxs used by the pipeline execution engine to push down rf to children nodes before
    // pipeline decomposition.
    std::vector<ExprContext*> _group_by_expr_ctxs;
    AggregatorPtr _aggregator = nullptr;
};

} // namespace starrocks