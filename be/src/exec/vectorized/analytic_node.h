// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "analytor.h"
#include "exec/exec_node.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"

namespace starrocks {
namespace vectorized {

class AnalyticNode : public ExecNode {
public:
    ~AnalyticNode() override = default;
    AnalyticNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    Status close(RuntimeState* state) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    const TPlanNode _tnode;
    // Tuple descriptor for storing results of analytic fn evaluation.
    const TupleDescriptor* _result_tuple_desc;
    AnalytorPtr _analytor = nullptr;

    Status _get_next_for_unbounded_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos);
    Status _get_next_for_unbounded_preceding_range_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos);
    Status _get_next_for_unbounded_preceding_rows_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos);
    Status _get_next_for_sliding_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos);
    Status (AnalyticNode::*_get_next)(RuntimeState* state, ChunkPtr* chunk, bool* eos) = nullptr;

    Status _fetch_next_chunk(RuntimeState* state);
    Status _try_fetch_next_partition_data(RuntimeState* state, int64_t* partition_end);
};
} // namespace vectorized
} // namespace starrocks
