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

#include "exec/fetch_node.h"

#include <protocol/TDebugProtocol.h>
#include <stdexcept>

#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "exec/exec_node.h"
#include "exec/pipeline/fetch_processor.h"
#include "exec/pipeline/fetch_sink_operator.h"
#include "exec/pipeline/fetch_source_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/tablet_info.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {
FetchNode::FetchNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {
    _target_node_id = tnode.fetch_node.target_node_id;
    for (const auto& [tuple_id, row_pos_desc] : tnode.fetch_node.row_pos_descs) {
        auto* desc = RowPositionDescriptor::from_thrift(row_pos_desc, pool);
        _row_pos_descs.emplace(tuple_id, desc);
    }

    _nodes_info = std::make_shared<StarRocksNodesInfo>(tnode.fetch_node.nodes_info);
}

FetchNode::~FetchNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status FetchNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    std::vector<TupleId> tuple_ids;
    for (const auto& [tuple_id, row_pos_desc] : _row_pos_descs) {
        tuple_ids.emplace_back(tuple_id);
    }

    _dispatcher =
            state->exec_env()->lookup_dispatcher_mgr()->create_dispatcher(state, state->query_id(), _target_node_id, tuple_ids);
    for (const auto& tuple_id : tuple_ids) {
        const auto& tuple_desc = state->desc_tbl().get_tuple_descriptor(tuple_id);
        for (const auto& slot : tuple_desc->slots()) {
            _slot_id_to_desc.insert({slot->id(), slot});
        }
    }

    return Status::OK();
}

pipeline::OpFactories FetchNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    OpFactories operators = _children[0]->decompose_to_pipeline(context);

    auto fetch_processor_factory = std::make_shared<pipeline::FetchProcessorFactory>(
            _target_node_id, _row_pos_descs, _slot_id_to_desc, _nodes_info, _dispatcher);

    auto sink_op = std::make_shared<pipeline::FetchSinkOperatorFactory>(context->next_operator_id(), id(),
                                                                        fetch_processor_factory);

    auto source_op = std::make_shared<pipeline::FetchSourceOperatorFactory>(context->next_operator_id(), id(),
                                                                            fetch_processor_factory);
    source_op->set_degree_of_parallelism(context->degree_of_parallelism());
    context->inherit_upstream_source_properties(source_op.get(), context->source_operator(operators));

    operators.emplace_back(std::move(sink_op));
    context->add_pipeline(operators);

    return OpFactories{std::move(source_op)};
}

} // namespace starrocks