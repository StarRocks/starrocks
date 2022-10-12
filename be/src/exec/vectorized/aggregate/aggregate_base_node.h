// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <any>

#include "exec/exec_node.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::vectorized {

class AggregateBaseNode : public ExecNode {
public:
    AggregateBaseNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AggregateBaseNode() override;
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    void push_down_join_runtime_filter(RuntimeState* state,
                                       vectorized::RuntimeFilterProbeCollector* collector) override;

protected:
    const TPlanNode& _tnode;
    // _group_by_expr_ctxs used by the pipeline execution engine to push down rf to children nodes before
    // pipeline decomposition.
    std::vector<ExprContext*> _group_by_expr_ctxs;
    AggregatorPtr _aggregator = nullptr;
    bool _child_eos = false;
};

} // namespace starrocks::vectorized
