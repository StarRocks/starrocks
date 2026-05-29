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

#include "base/phmap/phmap.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/join/join_hash_table.h"
#include "exec/pipeline_node.h"

namespace starrocks {

class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RuntimeState;
class ExprContext;

class ColumnRef;
class RuntimeFilterBuildDescriptor;

class HashJoinNode final : public PipelineNode {
public:
    HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~HashJoinNode() override {
        if (runtime_state() != nullptr) {
            close(runtime_state());
        }
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    void close(RuntimeState* state) override;
    StatusOr<pipeline::OpFactories> decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;
    bool can_generate_global_runtime_filter() const;
    TJoinDistributionMode::type distribution_mode() const;
    const std::list<RuntimeFilterBuildDescriptor*>& build_runtime_filters() const;
    void push_down_join_runtime_filter(RuntimeState* state, RuntimeFilterProbeCollector* collector) override;

private:
    template <class HashJoinerFactory, class HashJoinBuilderFactory, class HashJoinProbeFactory>
    StatusOr<pipeline::OpFactories> _decompose_to_pipeline(pipeline::PipelineBuilderContext* context);

    friend ExecNode;
    // _hash_join_node is used to construct HashJoiner, the reference is sound since
    // it's only used in FragmentExecutor::prepare function.
    const THashJoinNode& _hash_join_node;
    std::vector<ExprContext*> _probe_expr_ctxs;
    std::vector<ExprContext*> _build_expr_ctxs;
    std::vector<ExprContext*> _other_join_conjunct_ctxs;
    std::vector<bool> _is_null_safes;

    // If distribution type is SHUFFLE_HASH_BUCKET, local shuffle can use the
    // equivalence of ExchagneNode's partition colums
    std::vector<ExprContext*> _probe_equivalence_partition_expr_ctxs;
    std::vector<ExprContext*> _build_equivalence_partition_expr_ctxs;

    std::list<RuntimeFilterBuildDescriptor*> _build_runtime_filters;
    TJoinOp::type _join_type = TJoinOp::INNER_JOIN;
    TJoinDistributionMode::type _distribution_mode = TJoinDistributionMode::NONE;
    std::set<SlotId> _output_slots;

    ExprContext* _asof_join_condition_build_expr_ctx = nullptr;
    ExprContext* _asof_join_condition_probe_expr_ctx = nullptr;
    TExprOpcode::type _asof_join_condition_op = TExprOpcode::INVALID_OPCODE;

    bool _enable_late_materialization = false;

    bool _enable_partition_hash_join = false;

    bool _is_skew_join = false;

    JoinHashTable _ht;

    size_t _runtime_join_filter_pushdown_limit = 1024000;

    std::map<SlotId, ExprContext*> _common_expr_ctxs;
};

} // namespace starrocks
