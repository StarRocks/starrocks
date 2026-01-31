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

#include "exec/lookup_node.h"

#include <protocol/TDebugProtocol.h>

#include "exec/exec_node.h"
#include "exec/pipeline/lookup_operator.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "runtime/exec_env.h"
#include "runtime/lookup_stream_mgr.h"
#include "runtime/runtime_state.h"

namespace starrocks {
LookUpNode::LookUpNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {
    for (const auto& [tuple_id, row_pos_desc] : tnode.look_up_node.row_pos_descs) {
        auto* desc = RowPositionDescriptor::from_thrift(row_pos_desc, pool);
        _row_pos_descs.emplace(tuple_id, desc);
    }
}

LookUpNode::~LookUpNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status LookUpNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    std::vector<TupleId> tuple_ids;
    for (const auto& [tuple_id, row_pos_desc] : _row_pos_descs) {
        tuple_ids.emplace_back(tuple_id);
    }
    _dispatcher =
            state->exec_env()->lookup_dispatcher_mgr()->create_dispatcher(state, state->query_id(), id(), tuple_ids);

    return Status::OK();
}

Status LookUpNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    return Status::OK();
}

void LookUpNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    if (_dispatcher) {
        state->exec_env()->lookup_dispatcher_mgr()->remove_dispatcher(state->query_id(), id());
    }
    ExecNode::close(state);
}

pipeline::OpFactories LookUpNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    int32_t max_io_tasks = context->degree_of_parallelism();
    auto lookup_op = std::make_shared<pipeline::LookUpOperatorFactory>(context->next_operator_id(), id(),
                                                                       _row_pos_descs, _dispatcher, max_io_tasks);

    lookup_op->set_degree_of_parallelism(1);
    return OpFactories{std::move(lookup_op)};
}
} // namespace starrocks