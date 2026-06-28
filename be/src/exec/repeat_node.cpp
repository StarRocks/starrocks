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

#include "exec/repeat_node.h"

#include "common/config_exec_fwd.h"
#include "exec/pipeline/aggregate/repeat/repeat_operator.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks {
RepeatNode::RepeatNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs),
          _slot_id_set_list(tnode.repeat_node.slot_id_set_list),
          _all_slot_ids(tnode.repeat_node.all_slot_ids),
          _repeat_id_list(tnode.repeat_node.repeat_id_list),
          _repeat_times_required(_repeat_id_list.size()),
          _repeat_times_last(_repeat_times_required),
          _grouping_list(tnode.repeat_node.grouping_list),
          _output_tuple_id(tnode.repeat_node.output_tuple_id),
          _tuple_desc(descs.get_tuple_descriptor(_output_tuple_id)) {
    // initial for null slots;
    for (int i = 0; i < _repeat_times_required; ++i) {
        std::set<SlotId>& repeat_ids = _slot_id_set_list[i];
        std::vector<SlotId> null_slots;
        for (auto slot_id : _all_slot_ids) {
            if (repeat_ids.find(slot_id) == repeat_ids.end()) {
                null_slots.push_back(slot_id);
            }
        }
        _null_slot_ids.push_back(null_slots);
    }

    // initial for _columns_null of 4096 rows;
    _column_null = generate_null_column(config::vector_chunk_size);

    // initial for _grouping_columns;
    for (auto& group : _grouping_list) {
        Columns columns;
        columns.reserve(group.size());
        for (auto slot_id : group) {
            columns.push_back(generate_repeat_column(slot_id, config::vector_chunk_size));
        }
        _grouping_columns.push_back(columns);
    }

    DCHECK_EQ(_grouping_list.size(), _tuple_desc->slots().size());
    DCHECK_EQ(_grouping_list[0].size(), _repeat_id_list.size());
    DCHECK_EQ(_slot_id_set_list.size(), _repeat_id_list.size());
}

void RepeatNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    ExecNode::close(state);
}

StatusOr<pipeline::OpFactories> RepeatNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    ASSIGN_OR_RETURN(auto operators, _children[0]->decompose_to_pipeline(context));

    operators.emplace_back(std::make_shared<RepeatOperatorFactory>(
            context->next_operator_id(), id(), std::move(_slot_id_set_list), std::move(_all_slot_ids),
            std::move(_null_slot_ids), std::move(_repeat_id_list), _repeat_times_required, _repeat_times_last,
            std::move(_column_null), std::move(_grouping_columns), std::move(_grouping_list), _output_tuple_id,
            _tuple_desc, std::move(_conjunct_ctxs)));
    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, operators.back().get(), context, rc_rf_probe_collector);
    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return operators;
}

} // namespace starrocks
