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

#include "column/vectorized_fwd.h"
#include "exec/exec_node.h"
#include "exprs/expr_context.h"
#include "runtime/global_dict/parser.h"
#include "util/runtime_profile.h"

namespace starrocks {
class ProjectNode final : public ExecNode {
public:
    ProjectNode(ObjectPool* pool, const TPlanNode& node, const DescriptorTbl& desc);

    ~ProjectNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status reset(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    void push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs) override;
    void push_down_join_runtime_filter(RuntimeState* state, RuntimeFilterProbeCollector* collector) override;
    void push_down_tuple_slot_mappings(RuntimeState* state,
                                       const std::vector<TupleSlotMapping>& parent_mappings) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    std::vector<SlotId> _slot_ids;
    std::vector<ExprContext*> _expr_ctxs;
    std::vector<bool> _type_is_nullable;

    std::vector<SlotId> _common_sub_slot_ids;
    std::vector<ExprContext*> _common_sub_expr_ctxs;

    RuntimeProfile::Counter* _expr_compute_timer = nullptr;
    RuntimeProfile::Counter* _common_sub_expr_compute_timer = nullptr;
};

} // namespace starrocks
