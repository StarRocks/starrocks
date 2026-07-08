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

#include "exec/multi_sink_dispatch_node.h"

#include <utility>

#include "exec/pipeline/pipeline_builder.h"
#include "runtime/runtime_state.h"

namespace starrocks {

MultiSinkDispatchNode::MultiSinkDispatchNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {}

Status MultiSinkDispatchNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    return Status::NotSupported("MultiSinkDispatchNode is pipeline-only");
}

StatusOr<pipeline::OpFactories> MultiSinkDispatchNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    // Build one ExchangeSource pipeline per child ExchangeNode and stash it, keyed by the child's plan-node-id
    // (== the branch's dest_node_id). The collector's MultiSink DataSink caps each with an OlapTableSink.
    // No gather/merge (that is the only difference from UnionNode).
    for (int i = 0; i < static_cast<int>(_children.size()); ++i) {
        ASSIGN_OR_RETURN(auto child_ops, child(i)->decompose_to_pipeline(context));
        context->add_multi_sink_branch(child(i)->id(), std::move(child_ops));
    }
    return pipeline::OpFactories{};
}

} // namespace starrocks
