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

#include "common/statusor.h"
#include "exec/pipeline_node.h"
#include "exprs/sort_exec_exprs.h"

namespace starrocks {

class ChunksSorter;
class RuntimeFilterBuildDescriptor;

// Node for in-memory TopN (ORDER BY ... LIMIT).
//
// It sorts rows in a batch of chunks in turn at the open stage,
// and keeps LIMIT rows after each step for output.
class TopNNode final : public PipelineNode {
public:
    TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~TopNNode() override;

    // overridden methods defined in PipelineNode
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    void close(RuntimeState* state) override;

    StatusOr<pipeline::OpFactories> decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    template <class ContextFactory, class SinkFactory, class SourceFactory>
    StatusOr<pipeline::OpFactories> _decompose_to_pipeline(pipeline::PipelineBuilderContext* context,
                                                           bool is_partition_topn, bool analytic_need_merge,
                                                           bool is_merging, bool enable_parallel_merge,
                                                           bool is_per_pipeline);

    const TPlanNode& _tnode;

    // Only used for profile
    std::string _sort_keys;
    int64_t _offset;

    // _sort_exec_exprs contains the ordering expressions
    SortExecExprs _sort_exec_exprs;

    // The record that is materialized and sorted: the SORT TUPLE alone.
    //
    // This is NOT the node's own record (ExecNode::_record_descriptor). A window pre-aggregation
    // plan gives the sort node a SECOND tuple -- the pre-agg tuple, appended after the sort tuple
    // by SortNode's constructor on the FE side -- which this node PRODUCES but does not materialize
    // before sorting. Conflating the two makes the sorter's slot list longer than its list of
    // sort-tuple expressions, and charges the late-materialization estimate for slots that are
    // never permuted.
    //
    // TODO: this exists only to carry slot ids; see the TODO on
    // ChunksSorter::materialize_chunk_before_sort for how to remove it entirely.
    RecordDescriptor _materialized_record_descriptor;

    std::vector<SlotId> _early_materialized_slots{};
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
    std::vector<OrderByType> _order_by_types;
    // if TopNNode is followed by AnalyticNode with partition_exprs, this partition_exprs is
    // also added to TopNNode to hint that local shuffle operator is prepended to TopNNode in
    // order to eliminate merging operation in pipeline execution engine.
    std::vector<ExprContext*> _analytic_partition_exprs;

    std::vector<ExprContext*> _local_partition_exprs;

    // True if the _limit comes from DEFAULT_ORDER_BY_LIMIT and option
    // ABORT_ON_DEFAULT_LIMIT_EXCEEDED is set.
    bool _abort_on_default_limit_exceeded = false;

    std::unique_ptr<ChunksSorter> _chunks_sorter;

    std::vector<RuntimeFilterBuildDescriptor*> _build_runtime_filters;
};

} // namespace starrocks
