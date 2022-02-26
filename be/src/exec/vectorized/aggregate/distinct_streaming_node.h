// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <vector>

#include "exec/vectorized/aggregate/aggregate_base_node.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/exec_node.h"

namespace starrocks {
class DescriptorTbl;
class ObjectPool;
class RuntimeState;
class TPlanNode;
namespace pipeline {
class OperatorFactory;
class PipelineBuilderContext;
}  // namespace pipeline
}  // namespace starrocks

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