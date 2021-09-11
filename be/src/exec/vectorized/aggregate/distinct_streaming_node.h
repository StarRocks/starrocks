// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/vectorized/aggregate/aggregate_base_node.h"

// Distinct means this node handle distinct or group by no aggregate function query.
// Streaming means this node will handle input in get_next phase, and maybe directly
// ouput child chunk.
namespace starrocks::vectorized {
class DistinctStreamingNode final : public AggregateBaseNode {
public:
    DistinctStreamingNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : AggregateBaseNode(pool, tnode, descs) {
        _aggregator->set_aggr_phase(AggrPhase1);
    };
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

private:
    void _output_chunk_from_hash_set(ChunkPtr* chunk);
};
} // namespace starrocks::vectorized