// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/crossjoin/cross_join_context.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "runtime/descriptors.h"

namespace starrocks::pipeline {

class NLJoinProbeOperator final : public OperatorWithDependency {
public:
    NLJoinProbeOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                        TJoinOp::type join_op, const std::vector<ExprContext*>& conjunct_ctxs,
                        const std::vector<SlotDescriptor*>& col_types, const size_t& probe_column_count,
                        const size_t& build_column_count, const std::shared_ptr<CrossJoinContext>& cross_join_context);

    ~NLJoinProbeOperator() override = default;

    void close(RuntimeState* state) override;

    // Control flow
    bool is_ready() const override { return _cross_join_context->is_right_finished(); }
    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    // Data flow
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    int _num_build_chunks() const;
    ChunkPtr _init_output_chunk(RuntimeState* state) const;
    Status _probe(RuntimeState* state, ChunkPtr chunk);
    void _permute_probe_row(RuntimeState* state, ChunkPtr chunk);
    void _permute_chunk(RuntimeState* state, ChunkPtr chunk);
    void _permute_build_rows_right_join(RuntimeState* state, ChunkPtr chunk);
    void _permute_probe_row_left_join(RuntimeState* state, ChunkPtr chunk, size_t probe_row_index);
    bool _is_curr_probe_chunk_finished() const;

private:
    const TJoinOp::type _join_op;
    const std::vector<SlotDescriptor*>& _col_types;
    const size_t& _probe_column_count;
    const size_t& _build_column_count;

    const std::vector<ExprContext*>& _conjunct_ctxs;
    const std::shared_ptr<CrossJoinContext>& _cross_join_context;

    bool _is_finished = false;

    // Build states
    vectorized::Chunk* _curr_build_chunk = nullptr;
    int32_t _curr_build_index =
            -1; // Range from [-1, num_chunk_index], -1 denotes not-started, num_chunk_index denotes probe finied
    std::vector<uint8_t> _build_match_flag; // Whether this build row matched by probe

    // Probe states
    vectorized::ChunkPtr _probe_chunk = nullptr;
    bool _probe_row_matched = false;
    int32_t _probe_row_index = 0;
};

} // namespace starrocks::pipeline