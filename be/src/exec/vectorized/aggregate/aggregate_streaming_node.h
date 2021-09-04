// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/vectorized/aggregate/aggregate_base_node.h"

// Aggregate means this node handle query with aggregate functions.
// Streaming means this node will handle input in get_next phase, and maybe directly
// ouput child chunk.
namespace starrocks::vectorized {
class AggregateStreamingNode final : public AggregateBaseNode {
public:
    AggregateStreamingNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : AggregateBaseNode(pool, tnode, descs) {
        _aggr_phase = AggrPhase1;
    };
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

private:
    void _output_chunk_from_hash_map(ChunkPtr* chunk);
};
} // namespace starrocks::vectorized