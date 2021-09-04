// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/vectorized/aggregate/aggregate_base_node.h"

// Aggregate means this node handle query with aggregate functions.
// Blocking means this node will consume all input and build hash map in open phase.
namespace starrocks::vectorized {
class AggregateBlockingNode final : public AggregateBaseNode {
public:
    AggregateBlockingNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : AggregateBaseNode(pool, tnode, descs) {
        _aggr_phase = AggrPhase2;
    };
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
};
} // namespace starrocks::vectorized
