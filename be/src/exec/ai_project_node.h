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

#include "exec/pipeline_node.h"

namespace starrocks {

class AIProjectNode final : public PipelineNode {
public:
    AIProjectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AIProjectNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    void close(RuntimeState* state) override;

    void push_down_join_runtime_filter(RuntimeState* state, RuntimeFilterProbeCollector* collector) override;
    void push_down_tuple_slot_mappings(RuntimeState* state,
                                       const std::vector<TupleSlotMapping>& parent_mappings) override;

    StatusOr<pipeline::OpFactories> decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    std::vector<SlotId> _output_slot_ids;
    std::vector<ExprContext*> _output_expr_ctxs;
    std::vector<bool> _output_nullables;
    std::vector<bool> _output_is_ai;

    std::vector<SlotId> _common_slot_ids;
    std::vector<ExprContext*> _common_expr_ctxs;

    std::string _endpoint;
    std::string _default_model;
    bool _expressions_transferred = false;
};

} // namespace starrocks
