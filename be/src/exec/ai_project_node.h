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

#include <string>
#include <vector>

#include "exec/pipeline/ai/ai_project_factory.h"
#include "exec/pipeline_node.h"

namespace starrocks {

class AIProjectNode final : public PipelineNode {
public:
    AIProjectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AIProjectNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    void close(RuntimeState* state) override;

    void push_down_tuple_slot_mappings(RuntimeState* state,
                                       const std::vector<TupleSlotMapping>& parent_mappings) override;

    StatusOr<pipeline::OpFactories> decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    pipeline::AIProjectProjectionSpec _projection_spec;
    std::string _endpoint;
};

} // namespace starrocks
