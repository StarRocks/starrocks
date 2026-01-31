// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Stub implementations for lookup functionality on macOS

#include "exec/fetch_node.h"
#include "exec/lookup_node.h"
#include "runtime/lookup_stream_mgr.h"

namespace starrocks {

// LookUpNode stubs
LookUpNode::LookUpNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {}
LookUpNode::~LookUpNode() = default;

Status LookUpNode::init(const TPlanNode& tnode, RuntimeState* state) {
    return Status::NotSupported("LookUpNode is disabled on macOS");
}

Status LookUpNode::prepare(RuntimeState* state) {
    return Status::NotSupported("LookUpNode is disabled on macOS");
}

void LookUpNode::close(RuntimeState* state) {}

std::vector<std::shared_ptr<pipeline::OperatorFactory>> LookUpNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    return {};
}

// FetchNode stubs
FetchNode::FetchNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {}
FetchNode::~FetchNode() = default;

Status FetchNode::init(const TPlanNode& tnode, RuntimeState* state) {
    return Status::NotSupported("FetchNode is disabled on macOS");
}

pipeline::OpFactories FetchNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    return {};
}

// LookUpDispatcherMgr stubs
LookUpDispatcherPtr LookUpDispatcherMgr::create_dispatcher(RuntimeState* /*state*/, const TUniqueId& /*query_id*/,
                                                           PlanNodeId /*target_node_id*/,
                                                           const std::vector<TupleId>& /*request_tuple_ids*/) {
    return nullptr;
}

StatusOr<LookUpDispatcherPtr> LookUpDispatcherMgr::get_dispatcher(const TUniqueId& /*query_id*/,
                                                                  PlanNodeId /*target_node_id*/) {
    return Status::NotSupported("Lookup is disabled on macOS");
}

void LookUpDispatcherMgr::remove_dispatcher(const TUniqueId& /*query_id*/, PlanNodeId /*target_node_id*/) {}

Status LookUpDispatcherMgr::lookup(const pipeline::RemoteLookUpRequestContextPtr& /*ctx*/) {
    return Status::NotSupported("Lookup is disabled on macOS");
}

namespace pipeline {

// RemoteLookUpRequestContext stubs
Status RemoteLookUpRequestContext::collect_input_columns(ChunkPtr /*chunk*/) {
    return Status::NotSupported("Lookup is disabled on macOS");
}

StatusOr<size_t> RemoteLookUpRequestContext::fill_response(const ChunkPtr& /*result_chunk*/, SlotId /*source_id_slot*/,
                                                           const std::vector<SlotDescriptor*>& /*slots*/,
                                                           size_t /*start_offset*/) {
    return Status::NotSupported("Lookup is disabled on macOS");
}

void RemoteLookUpRequestContext::callback(const Status& /*status*/) {}

} // namespace pipeline
} // namespace starrocks
