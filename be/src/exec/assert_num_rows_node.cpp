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

#include "exec/assert_num_rows_node.h"

#include "common/runtime_profile.h"
#include "exec/pipeline/assert_num_rows_operator.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"

namespace starrocks {

AssertNumRowsNode::AssertNumRowsNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs),
          _desired_num_rows(tnode.assert_num_rows_node.desired_num_rows),
          _subquery_string(tnode.assert_num_rows_node.subquery_string)

{
    if (tnode.assert_num_rows_node.__isset.assertion) {
        _assertion = tnode.assert_num_rows_node.assertion;
    } else {
        _assertion = TAssertion::LE; // just comptiable for the previous code
    }
}

Status AssertNumRowsNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

void AssertNumRowsNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    ExecNode::close(state);
}

StatusOr<pipeline::OpFactories> AssertNumRowsNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    ASSIGN_OR_RETURN(auto operator_before_assert_num_rows_source, _children[0]->decompose_to_pipeline(context));
    operator_before_assert_num_rows_source = context->maybe_interpolate_local_passthrough_exchange(
            runtime_state(), id(), operator_before_assert_num_rows_source);

    auto source_factory = std::make_shared<AssertNumRowsOperatorFactory>(
            context->next_operator_id(), id(), _desired_num_rows, _subquery_string, _assertion);
    operator_before_assert_num_rows_source.emplace_back(std::move(source_factory));

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, operator_before_assert_num_rows_source.back().get(), context,
                                               rc_rf_probe_collector);
    if (limit() != -1) {
        operator_before_assert_num_rows_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return operator_before_assert_num_rows_source;
}

} // namespace starrocks
