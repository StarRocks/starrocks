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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/select_node.cpp

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

#include "exec/select_node.h"

#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/select_operator.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr.h"
#include "exprs/expr_factory.h"
#include "runtime/runtime_state.h"

namespace starrocks {

SelectNode::SelectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs) {}

SelectNode::~SelectNode() {
    if (runtime_state() != nullptr) {
        ExecNode::close(runtime_state());
    }
}

Status SelectNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    if (tnode.__isset.select_node && tnode.select_node.__isset.common_slot_map) {
        for (const auto& [key, val] : tnode.select_node.common_slot_map) {
            ExprContext* context;
            RETURN_IF_ERROR(ExprFactory::create_expr_tree(_pool, val, &context, state, true));
            _common_expr_ctxs.insert({key, context});
        }
    }
    return Status::OK();
}

void SelectNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    ExecNode::close(state);
}

StatusOr<pipeline::OpFactories> SelectNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    ASSIGN_OR_RETURN(auto operators, _children[0]->decompose_to_pipeline(context));

    operators.emplace_back(std::make_shared<SelectOperatorFactory>(
            context->next_operator_id(), id(), std::move(_conjunct_ctxs), std::move(_common_expr_ctxs)));

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, operators.back().get(), context, rc_rf_probe_collector);
    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return operators;
}

} // namespace starrocks
