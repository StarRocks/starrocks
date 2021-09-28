// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <any>

#include "exec/exec_node.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::vectorized {

class AggregateBaseNode : public ExecNode {
public:
    AggregateBaseNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AggregateBaseNode() override;

    Status prepare(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    void push_down_join_runtime_filter(RuntimeState* state,
                                       vectorized::RuntimeFilterProbeCollector* collector) override;

protected:
    const TPlanNode _tnode;
    AggregatorPtr _aggregator = nullptr;
    bool _child_eos = false;
};

} // namespace starrocks::vectorized
