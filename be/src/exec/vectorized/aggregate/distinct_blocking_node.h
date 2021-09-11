// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/vectorized/aggregate/aggregate_base_node.h"

// Distinct means this node handle distinct or group by no aggregate function query.
// Blocking means this node will consume all input and build hash set in open phase.
namespace starrocks::vectorized {
class DistinctBlockingNode final : public AggregateBaseNode {
public:
    DistinctBlockingNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : AggregateBaseNode(pool, tnode, descs) {
        _aggregator->set_aggr_phase(AggrPhase2);
    };
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
};
} // namespace starrocks::vectorized