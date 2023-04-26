// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "exec/aggregate/aggregate_base_node.h"
#include "exec/exec_node.h"
#include "exec/pipeline/operator.h"

// Aggregate means this node handle query with aggregate functions.
// Blocking means this node will consume all input and build hash map in open phase.
namespace starrocks {
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
} // namespace starrocks
