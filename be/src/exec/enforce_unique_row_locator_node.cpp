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

#include "exec/enforce_unique_row_locator_node.h"

#include "exec/pipeline/enforce_unique_row_locator_operator.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"

namespace starrocks {

EnforceUniqueRowLocatorNode::EnforceUniqueRowLocatorNode(ObjectPool* pool, const TPlanNode& tnode,
                                                         const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs) {
    if (tnode.__isset.enforce_unique_row_locator_node &&
        tnode.enforce_unique_row_locator_node.__isset.unique_key_slot_ids) {
        const auto& slot_ids = tnode.enforce_unique_row_locator_node.unique_key_slot_ids;
        _unique_key_slot_ids.assign(slot_ids.begin(), slot_ids.end());
    }
}

Status EnforceUniqueRowLocatorNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

void EnforceUniqueRowLocatorNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    ExecNode::close(state);
}

StatusOr<pipeline::OpFactories> EnforceUniqueRowLocatorNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    ASSIGN_OR_RETURN(auto ops, _children[0]->decompose_to_pipeline(context));

    auto factory = std::make_shared<EnforceUniqueRowLocatorOperatorFactory>(context->next_operator_id(), id(),
                                                                            _unique_key_slot_ids);
    ops.emplace_back(std::move(factory));

    // Initialize OperatorFactory's fields involving runtime filters.
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    pipeline::init_runtime_filter_for_operator(*this, ops.back().get(), context, rc_rf_probe_collector);

    if (limit() != -1) {
        ops.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return ops;
}

} // namespace starrocks
