// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/exec_node.h"
#include "exec/sort_exec_exprs.h"

namespace starrocks::vectorized {

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
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    Status close(RuntimeState* state) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    Status _consume_chunks(RuntimeState* state, ExecNode* child);
    ChunkPtr _materialize_chunk_before_sort(Chunk* chunk);

    int64_t _offset;

    // _sort_exec_exprs contains the ordering expressions
    SortExecExprs _sort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
    std::vector<OrderByType> _order_by_types;

    // Cached descriptor for the materialized tuple. Assigned in Prepare().
    TupleDescriptor* _materialized_tuple_desc;

    // True if the _limit comes from DEFAULT_ORDER_BY_LIMIT and option
    // ABORT_ON_DEFAULT_LIMIT_EXCEEDED is set.
    bool _abort_on_default_limit_exceeded;

    std::unique_ptr<ChunksSorter> _chunks_sorter;

    RuntimeProfile::Counter* _sort_timer;
};

} // namespace starrocks::vectorized
