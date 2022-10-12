// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/exec_node.h"
#include "exec/pipeline/operator.h"
#include "exec/vectorized/aggregate/aggregate_base_node.h"

// Aggregate means this node handle query with aggregate functions.
// Blocking means this node will consume all input and build hash map in open phase.
namespace starrocks::vectorized {
class AggregateBlockingNode final : public AggregateBaseNode {
public:
    AggregateBlockingNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : AggregateBaseNode(pool, tnode, descs) {}

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;
};
} // namespace starrocks::vectorized
