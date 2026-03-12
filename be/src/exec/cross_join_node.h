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

#include "column/vectorized_fwd.h"
#include "exec/pipeline_node.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_filter/runtime_filter_descriptor.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class CrossJoinNode final : public PipelineNode {
public:
    CrossJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    ~CrossJoinNode() override {
        if (runtime_state() != nullptr) {
            close(runtime_state());
        }
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    void close(RuntimeState* state) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

    // rewrite conjuncts as RuntimeFilter according to could_rewrite.
    // now we only support rewrites with chunk rows of 1.
    //
    // eg: if input chunk is [col3: 1, col4: 2]
    // slot1 > if (slot3 > 1, col3, col4) will be rewrited as slot1 > 4
    //
    // TODO: support multi rows rewrite
    static StatusOr<std::list<ExprContext*>> rewrite_runtime_filter(
            ObjectPool* pool, const std::vector<RuntimeFilterBuildDescriptor*>& rf_descs, Chunk* chunk,
            const std::vector<ExprContext*>& ctxs);

private:
    template <class BuildFactory, class ProbeFactory>
    std::vector<std::shared_ptr<pipeline::OperatorFactory>> _decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context);

    TJoinOp::type _join_op = TJoinOp::type::CROSS_JOIN;
    std::vector<ExprContext*> _join_conjuncts;
    std::string _sql_join_conjuncts;
    std::vector<RuntimeFilterBuildDescriptor*> _build_runtime_filters;
    bool _interpolate_passthrough = false;

    std::map<SlotId, ExprContext*> _common_expr_ctxs;
};

} // namespace starrocks
