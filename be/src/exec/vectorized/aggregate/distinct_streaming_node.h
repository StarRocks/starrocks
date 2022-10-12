// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/vectorized/aggregate/aggregate_base_node.h"

// Distinct means this node handle distinct or group by no aggregate function query.
// Streaming means this node will handle input in get_next phase, and maybe directly
// ouput child chunk.
namespace starrocks::vectorized {
class DistinctStreamingNode final : public AggregateBaseNode {
public:
    DistinctStreamingNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : AggregateBaseNode(pool, tnode, descs) {}

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    void _output_chunk_from_hash_set(ChunkPtr* chunk);
};
} // namespace starrocks::vectorized