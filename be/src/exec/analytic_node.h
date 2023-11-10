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

#include "analytor.h"
#include "exec/exec_node.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"

namespace starrocks {

class AnalyticNode final : public ExecNode {
public:
    AnalyticNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AnalyticNode() override {
        if (runtime_state() != nullptr) {
            close(runtime_state());
        }
    }

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status close(RuntimeState* state) override;

    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    const TPlanNode _tnode;
    // Tuple descriptor for storing results of analytic fn evaluation.
    const TupleDescriptor* _result_tuple_desc;
    AnalytorPtr _analytor = nullptr;
    bool _use_hash_based_partition = false;
    std::vector<ExprContext*> _hash_partition_exprs;
};
} // namespace starrocks
