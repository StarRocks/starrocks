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

    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    template <class AggFactory, class SourceFactory, class SinkFactory>
    pipeline::OpFactories _decompose_to_pipeline(pipeline::OpFactories& ops_with_sink,
                                                 pipeline::PipelineBuilderContext* context);
};
} // namespace starrocks::vectorized
