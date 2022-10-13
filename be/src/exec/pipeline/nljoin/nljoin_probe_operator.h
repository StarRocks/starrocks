// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/nljoin/nljoin_context.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {

// NestLoopJoin
// Implement the block-wise nestloop algorithm, support inner/outer join
// The algorithm consists of three steps:
// 1. Permute the block from probe side and build side
// 2. Apply the join conjuncts and filter data
// 3. Emit the unmatched probe rows and build row for outer join
class NLJoinProbeOperator final : public OperatorWithDependency {
public:
    NLJoinProbeOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                        TJoinOp::type join_op, const std::string& sql_join_conjuncts,
                        const std::vector<ExprContext*>& join_conjuncts, const std::vector<ExprContext*>& conjunct_ctxs,
                        const std::vector<SlotDescriptor*>& col_types, size_t probe_column_count,
                        size_t build_column_count, const std::shared_ptr<NLJoinContext>& cross_join_context);

    ~NLJoinProbeOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    // Control flow
    bool is_ready() const override;
    bool is_finished() const override;
    bool has_output() const override;
    bool need_input() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    // Data flow
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    enum JoinStage {
        Probe,         // Start probing left table
        RightJoin,     // The last prober need to merge the build_match_flags and perform the right join
        PostRightJoin, // Finish right join, and has some data to pull
        Finished,      // Finish all job
    };

    int _num_build_chunks() const;
    vectorized::Chunk* _move_build_chunk_index(int index);
    ChunkPtr _init_output_chunk(RuntimeState* state) const;
    Status _probe(RuntimeState* state, ChunkPtr chunk);
    void _advance_join_stage(JoinStage stage) const;
    bool _skip_probe() const;
    void _check_post_probe() const;
    void _init_build_match() const;
    void _permute_probe_row(RuntimeState* state, ChunkPtr chunk);
    ChunkPtr _permute_chunk(RuntimeState* state);
    Status _permute_right_join(RuntimeState* state);
    void _permute_left_join(RuntimeState* state, ChunkPtr chunk, size_t probe_row_index, size_t probe_rows);
    bool _is_curr_probe_chunk_finished() const;
    bool _is_left_join() const;
    bool _is_right_join() const;

private:
    const TJoinOp::type _join_op;
    const std::vector<SlotDescriptor*>& _col_types;
    const size_t _probe_column_count;
    const size_t _build_column_count;

    const std::string& _sql_join_conjuncts;
    const std::vector<ExprContext*>& _join_conjuncts;

    const std::vector<ExprContext*>& _conjunct_ctxs;
    const std::shared_ptr<NLJoinContext>& _cross_join_context;

    RuntimeState* _runtime_state = nullptr;
    bool _input_finished = false;
    mutable JoinStage _join_stage = JoinStage::Probe;
    mutable ChunkAccumulator _output_accumulator;

    // Build states
    int _curr_build_chunk_index = 0;
    vectorized::Chunk* _curr_build_chunk = nullptr;
    size_t _prev_chunk_start = 0;
    size_t _prev_chunk_size = 0;
    mutable std::vector<uint8_t> _self_build_match_flag;

    // Probe states
    vectorized::ChunkPtr _probe_chunk = nullptr;
    bool _probe_row_matched = false;
    size_t _probe_row_start = 0;   // Start index of current chunk
    size_t _probe_row_current = 0; // End index of current chunk

    // Counters
    RuntimeProfile::Counter* _permute_rows_counter = nullptr;
    RuntimeProfile::Counter* _permute_left_rows_counter = nullptr;
};

class NLJoinProbeOperatorFactory final : public OperatorWithDependencyFactory {
public:
    NLJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id, const RowDescriptor& row_descriptor,
                               const RowDescriptor& left_row_desc, const RowDescriptor& right_row_desc,
                               const std::string& sql_join_conjuncts, std::vector<ExprContext*>&& join_conjuncts,
                               std::vector<ExprContext*>&& conjunct_ctxs,
                               std::shared_ptr<NLJoinContext>&& cross_join_context, TJoinOp::type join_op)
            : OperatorWithDependencyFactory(id, "cross_join_left", plan_node_id),
              _join_op(join_op),
              _row_descriptor(row_descriptor),
              _left_row_desc(left_row_desc),
              _right_row_desc(right_row_desc),
              _sql_join_conjuncts(sql_join_conjuncts),
              _join_conjuncts(std::move(join_conjuncts)),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _cross_join_context(std::move(cross_join_context)) {}

    ~NLJoinProbeOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    void _init_row_desc();

    const TJoinOp::type _join_op;
    const RowDescriptor& _row_descriptor;
    const RowDescriptor& _left_row_desc;
    const RowDescriptor& _right_row_desc;

    vectorized::Buffer<SlotDescriptor*> _col_types;
    size_t _probe_column_count = 0;
    size_t _build_column_count = 0;

    std::string _sql_join_conjuncts;
    std::vector<ExprContext*> _join_conjuncts;
    std::vector<ExprContext*> _conjunct_ctxs;

    std::shared_ptr<NLJoinContext> _cross_join_context;
};

} // namespace starrocks::pipeline