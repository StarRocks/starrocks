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

#pragma once

#include <unordered_map>

#include "common/global_types.h"
#include "exec/exec_node.h"
#include "exec/tablet_info.h"
#include "runtime/descriptors.h"
#include "runtime/lookup_stream_mgr.h"

namespace starrocks {
class LookUpDispatcher;

class FetchNode final : public ExecNode {
public:
    FetchNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~FetchNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    int32_t _target_node_id;
    phmap::flat_hash_map<TupleId, RowPositionDescriptor*> _row_pos_descs;
    phmap::flat_hash_map<SlotId, SlotDescriptor*> _slot_id_to_desc;
    std::shared_ptr<StarRocksNodesInfo> _nodes_info;
    std::shared_ptr<LookUpDispatcher> _dispatcher;
};
} // namespace starrocks