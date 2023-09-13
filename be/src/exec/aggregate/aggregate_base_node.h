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

#include <any>

#include "exec/aggregator.h"
#include "exec/exec_node.h"

namespace starrocks {

class AggregateBaseNode : public ExecNode {
public:
    AggregateBaseNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AggregateBaseNode() override;
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    void push_down_join_runtime_filter(RuntimeState* state, RuntimeFilterProbeCollector* collector) override;

protected:
    const TPlanNode& _tnode;
    // _group_by_expr_ctxs used by the pipeline execution engine to push down rf to children nodes before
    // pipeline decomposition.
    std::vector<ExprContext*> _group_by_expr_ctxs;
    AggregatorPtr _aggregator = nullptr;
    bool _child_eos = false;
};

} // namespace starrocks
