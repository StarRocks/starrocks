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

#include "exec/enforce_unique_node.h"

#include "exec/pipeline/enforce_unique_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

EnforceUniqueNode::EnforceUniqueNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs) {
    if (tnode.__isset.enforce_unique_node && tnode.enforce_unique_node.__isset.unique_key_col_indices) {
        const auto& indices = tnode.enforce_unique_node.unique_key_col_indices;
        _unique_key_col_indices.assign(indices.begin(), indices.end());
    }
}

Status EnforceUniqueNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

void EnforceUniqueNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    ExecNode::close(state);
}

StatusOr<pipeline::OpFactories> EnforceUniqueNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    ASSIGN_OR_RETURN(auto ops, _children[0]->decompose_to_pipeline(context));
    // Correctness barrier (not a performance optimization): the default overload passes
    // num_receivers=1, which funnels all upstream drivers into a single pipeline driver.
    // This is required because EnforceUniqueOperator maintains a per-instance hash set
    // of seen (file, pos) keys — splitting across multiple drivers would allow duplicates
    // that land on different drivers to escape detection.
    ops = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), ops);

    auto factory =
            std::make_shared<EnforceUniqueOperatorFactory>(context->next_operator_id(), id(), _unique_key_col_indices);
    ops.emplace_back(std::move(factory));

    // Initialize OperatorFactory's fields involving runtime filters.
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    this->init_runtime_filter_for_operator(ops.back().get(), context, rc_rf_probe_collector);

    if (limit() != -1) {
        ops.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return ops;
}

} // namespace starrocks
