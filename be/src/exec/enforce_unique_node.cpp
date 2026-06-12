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
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

EnforceUniqueNode::EnforceUniqueNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs) {
    if (tnode.__isset.enforce_unique_node && tnode.enforce_unique_node.__isset.unique_key_slot_ids) {
        const auto& slot_ids = tnode.enforce_unique_node.unique_key_slot_ids;
        _unique_key_slot_ids.assign(slot_ids.begin(), slot_ids.end());
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
    // Keep local parallelism for MERGE duplicate checks by routing equal
    // (_file, _pos) keys to the same local driver. Each EnforceUniqueOperator
    // only needs a local seen-set for its hash partition. Rows whose key
    // contains NULL (MERGE NOT-MATCHED insert rows) are exempt from the check
    // and spread round-robin by the exchange.
    //
    // CROSS-BE INVARIANT: this exchange only deduplicates within one BE. The
    // FE guarantees that all copies of one target row reach the same BE by
    // enforcing the sink fragment's distribution: hash by the target's
    // PARTITION columns for partitioned tables (copies carry identical
    // partition values because partition-column UPDATEs are rejected), or
    // hash by _file for non-partitioned tables. MergeIntoPlanner::
    // validateDuplicateCheckDistribution rejects any other distribution at
    // plan time — keep that guard in sync when changing this node.
    ops = context->interpolate_local_column_hash_partition_exchange(runtime_state(), id(), ops, _unique_key_slot_ids,
                                                                    static_cast<int>(context->degree_of_parallelism()));
    if (context->degree_of_parallelism() > 1) {
        auto* source = dynamic_cast<LocalExchangeSourceOperatorFactory*>(context->source_operator(ops));
        DCHECK(source != nullptr);
        if (source != nullptr) {
            DCHECK(source->is_column_hash_partitioned_by(_unique_key_slot_ids));
        }
    }

    auto factory =
            std::make_shared<EnforceUniqueOperatorFactory>(context->next_operator_id(), id(), _unique_key_slot_ids);
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
