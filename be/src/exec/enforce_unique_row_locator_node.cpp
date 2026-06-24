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
#include "exec/pipeline/pipeline_builder_operators.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

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

    // Self-sufficient within-BE check: locally shuffle by the (_file, _pos) row-locator
    // key so all matches of one target row reach the same operator instance, regardless of
    // how the child pipeline distributes chunks across drivers (e.g. a post-probe
    // passthrough the join adds when hash_join_interpolate_passthrough is on). Cross-BE
    // co-location is still provided by the FE-pinned shuffle merge join. maybe_interpolate
    // is a no-op / cheap when the input is already locally key-partitioned. NULL-key rows
    // (NOT-MATCHED inserts) are exempt from the check and may land on any driver.
    // ExprContexts are left unprepared: the PartitionExchanger prepares/opens/closes them.
    // Use context->runtime_state(): this PipelineNode's prepare() never runs, so
    // ExecNode::runtime_state() is null.
    RuntimeState* state = context->runtime_state();
    std::vector<ExprContext*> partition_ctxs;
    partition_ctxs.reserve(_unique_key_slot_ids.size());
    for (SlotId slot_id : _unique_key_slot_ids) {
        SlotDescriptor* slot_desc = state->desc_tbl().get_slot_descriptor(slot_id);
        if (slot_desc == nullptr) {
            return Status::InternalError("EnforceUniqueRowLocatorNode: row-locator slot " + std::to_string(slot_id) +
                                         " not found in the descriptor table");
        }
        auto* col_ref = _pool->add(new ColumnRef(slot_desc));
        partition_ctxs.push_back(_pool->add(new ExprContext(col_ref)));
    }
    ops = pipeline::builder::maybe_interpolate_local_shuffle_exchange(context, state, id(), ops, partition_ctxs);

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
