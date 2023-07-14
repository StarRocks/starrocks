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

#include "exec/exec_node.h"
#include "exec/sort_exec_exprs.h"

namespace starrocks {

class ChunksSorter;

// Node for in-memory TopN (ORDER BY ... LIMIT).
//
// It sorts rows in a batch of chunks in turn at the open stage,
// and keeps LIMIT rows after each step for output.
class TopNNode final : public ::starrocks::ExecNode {
public:
    TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~TopNNode() override;

    // overridden methods defined in ::starrocks::ExecNode
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    void close(RuntimeState* state) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    template <class ContextFactory, class SinkFactory, class SourceFactory>
    std::vector<std::shared_ptr<pipeline::OperatorFactory>> _decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context, bool is_partition, bool is_merging, bool enable_parallel_merge);

    Status _consume_chunks(RuntimeState* state, ExecNode* child);
    const TPlanNode& _tnode;

    // Only used for profile
    std::string _sort_keys;
    int64_t _offset;

    // _sort_exec_exprs contains the ordering expressions
    SortExecExprs _sort_exec_exprs;
    std::vector<SlotId> _early_materialized_slots{};
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
    std::vector<OrderByType> _order_by_types;
    // if TopNNode is followed by AnalyticNode with partition_exprs, this partition_exprs is
    // also added to TopNNode to hint that local shuffle operator is prepended to TopNNode in
    // order to eliminate merging operation in pipeline execution engine.
    std::vector<ExprContext*> _analytic_partition_exprs;

    // Cached descriptor for the materialized tuple. Assigned in Prepare().
    TupleDescriptor* _materialized_tuple_desc;

    // True if the _limit comes from DEFAULT_ORDER_BY_LIMIT and option
    // ABORT_ON_DEFAULT_LIMIT_EXCEEDED is set.
    bool _abort_on_default_limit_exceeded = false;

    std::unique_ptr<ChunksSorter> _chunks_sorter;

    RuntimeProfile::Counter* _sort_timer;
    std::vector<RuntimeFilterBuildDescriptor*> _build_runtime_filters;
};

} // namespace starrocks
