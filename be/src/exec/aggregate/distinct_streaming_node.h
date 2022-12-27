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

// Distinct means this node handle distinct or group by no aggregate function query.
// Streaming means this node will handle input in get_next phase, and maybe directly
// ouput child chunk.
namespace starrocks {
class DistinctStreamingNode final : public AggregateBaseNode {
public:
    DistinctStreamingNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : AggregateBaseNode(pool, tnode, descs) {}

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    void _output_chunk_from_hash_set(ChunkPtr* chunk);
};
} // namespace starrocks
