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

#include "exec/dict_decode_node.h"

#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/logging.h"
#include "common/runtime_profile.h"
#include "exec/pipeline/dict_decode_operator.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "runtime/global_dict/fragment_dict_state.h"
#include "runtime/runtime_state.h"

namespace starrocks {

DictDecodeNode::DictDecodeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs) {}

Status DictDecodeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    _init_counter();

    std::vector<SlotId> slots;
    for (const auto& [slot_id, texpr] : tnode.decode_node.string_functions) {
        ExprContext* context;
        RETURN_IF_ERROR(ExprFactory::create_expr_tree(_pool, texpr, &context, state));
        _string_functions[slot_id] = std::make_pair(context, DictOptimizeContext{});
        _expr_ctxs.push_back(context);
        slots.emplace_back(slot_id);
    }

    DictOptimizeParser::set_output_slot_id(&_expr_ctxs, slots);
    for (const auto& [encode_id, decode_id] : tnode.decode_node.dict_id_to_string_ids) {
        _encode_column_cids.emplace_back(encode_id);
        _decode_column_cids.emplace_back(decode_id);
    }

    auto& tuple_id = this->_tuple_ids[0];
    for (const auto& dict_id : _decode_column_cids) {
        auto idx = this->row_desc().get_tuple_idx(tuple_id);
        auto& tuple = this->row_desc().tuple_descriptors()[idx];
        for (const auto& slot : tuple->slots()) {
            if (slot->id() == dict_id) {
                _decode_column_types.emplace_back(&slot->type());
                break;
            }
        }
    }

    return Status::OK();
}

void DictDecodeNode::_init_counter() {
    _decode_timer = ADD_TIMER(_runtime_profile, "DictDecodeTime");
}

void DictDecodeNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    ExecNode::close(state);
    ExprExecutor::close(_expr_ctxs, state);
}

StatusOr<pipeline::OpFactories> DictDecodeNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    ASSIGN_OR_RETURN(auto operators, _children[0]->decompose_to_pipeline(context));
    operators.emplace_back(std::make_shared<DictDecodeOperatorFactory>(
            context->next_operator_id(), id(), std::move(_encode_column_cids), std::move(_decode_column_cids),
            std::move(_decode_column_types), std::move(_expr_ctxs), std::move(_string_functions)));
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
