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

#include "column/chunk.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/stream/aggregate/stream_aggregate_operator.h"

namespace starrocks {
using StreamAggregatorPtr = std::shared_ptr<stream::StreamAggregator>;
using StreamAggregatorFactory = AggregatorFactoryBase<stream::StreamAggregator>;
using StreamAggregatorFactoryPtr = std::shared_ptr<StreamAggregatorFactory>;

class StreamAggregateNode final : public starrocks::ExecNode {
public:
    StreamAggregateNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs), _tnode(tnode) {}
    ~StreamAggregateNode() override {}
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    std::vector<std::shared_ptr<pipeline::OperatorFactory> > decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    const TPlanNode& _tnode;
    // _group_by_expr_ctxs used by the pipeline execution engine to push down rf to children nodes before
    // pipeline decomposition.
    std::vector<ExprContext*> _group_by_expr_ctxs;
};

} // namespace starrocks
