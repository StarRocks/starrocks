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

#include "exec_primitive/exec_node.h"

namespace starrocks {

// Option X (CK-compatible logical-sink MV JOIN/agg): the collector fragment's plan root. An N-ary NON-merging
// node whose children are the N ExchangeNodes (one per branch fragment). Its decompose_to_pipeline builds each
// child's ExchangeSource pipeline and stashes it (keyed by the child ExchangeNode's plan-node-id) into the
// PipelineBuilderContext; the collector's MultiSink DataSink then caps each stashed pipeline with its own
// OlapTableSink. Unlike UnionNode it never merges its children. See CH_REALTIME_MV_DESIGN.md sec 12/13.
class MultiSinkDispatchNode final : public ExecNode {
public:
    MultiSinkDispatchNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~MultiSinkDispatchNode() override = default;

    // Pipeline-only node; the row-at-a-time engine never pulls it.
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    StatusOr<pipeline::OpFactories> decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;
};

} // namespace starrocks
