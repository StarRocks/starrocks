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

#include <memory>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/nljoin/nljoin_context.h"
#include "exec/pipeline/nljoin/nljoin_probe_operator.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exec/spill/executor.h"
#include "exec/spill/spiller_factory.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

class NLJoinProber {
public:
    NLJoinProber(TJoinOp::type join_op, const std::vector<ExprContext*>& join_conjuncts,
                 const std::vector<ExprContext*>& conjunct_ctxs, const std::vector<SlotDescriptor*>& col_types,
                 size_t probe_column_count);

    ~NLJoinProber() = default;

    Status prepare(RuntimeState* state, RuntimeProfile* profile);

    void reset() {
        _probe_chunk = nullptr;
        _probe_row_current = 0;
    }

    // reset probe
    void reset_probe() { _probe_row_current = 0; }

    // current probe chunk has finished
    bool probe_finished() const { return _probe_chunk == nullptr || _probe_row_current >= _probe_chunk->num_rows(); }

    Status push_probe_chunk(const ChunkPtr& chunk);

    StatusOr<ChunkPtr> probe_chunk(RuntimeState* state, const ChunkPtr& build_chunk);

    const std::vector<ExprContext*>& conjunct_ctxs() const { return _conjunct_ctxs; }

    // Join type check
    bool is_left_join() const { return _join_op == TJoinOp::LEFT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN; }
    bool is_right_join() const { return _join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN; }
    bool is_left_semi_join() const { return _join_op == TJoinOp::LEFT_SEMI_JOIN; }
    bool is_left_anti_join() const { return _join_op == TJoinOp::LEFT_ANTI_JOIN; }

private:
    ChunkPtr _init_output_chunk(RuntimeState* state, const ChunkPtr& build_chunk);
    // Permute enough rows from build side and probe side
    // The chunk either consists two conditions:
    // 1. Multiple probe rows and multiple build single-chunk
    // 2. One probe rows and one build chunk
    void _permute_chunk(RuntimeState* state, const ChunkPtr& build_chunk, const ChunkPtr& output);

    ChunkPtr _permute_by_one_chunk(RuntimeState* state, const ChunkPtr& build_chunk);

    void _permute_probe_row(Chunk* dst, const ChunkPtr& build_chunk);

private:
    //
    const TJoinOp::type _join_op;
    const std::vector<SlotDescriptor*>& _col_types;
    const size_t _probe_column_count;

    const std::vector<ExprContext*>& _join_conjuncts;
    const std::vector<ExprContext*>& _conjunct_ctxs;

    //
    ChunkPtr _probe_chunk = nullptr;
    // End index of current chunk
    size_t _probe_row_current = 0;

private:
    RuntimeProfile::Counter* _permute_rows_counter = nullptr;
    RuntimeProfile::Counter* _permute_left_rows_counter = nullptr;
};

// now we only support cross-join/left-semi join/left-anti join
class SpillableNLJoinProbeOperator final : public OperatorWithDependency {
public:
    SpillableNLJoinProbeOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                 TJoinOp::type join_op, const std::string& sql_join_conjuncts,
                                 const std::vector<ExprContext*>& join_conjuncts,
                                 const std::vector<ExprContext*>& conjunct_ctxs,
                                 const std::vector<SlotDescriptor*>& col_types, size_t probe_column_count,
                                 const std::shared_ptr<NLJoinContext>& cross_join_context);

    ~SpillableNLJoinProbeOperator() override = default;

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
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    // TODO: implements reset_state
private:
    void _init_chunk_stream() const;
    // current build finish
    bool _is_current_build_probe_finished() const { return _current_build_probe_finished; }
    void _set_current_build_probe_finished(bool build_finished) { _current_build_probe_finished = build_finished; }

    ChunkAccumulator _accumulator;

private:
    bool _is_finishing{};
    bool _current_build_probe_finished = true;
    ChunkPtr _build_chunk;

    bool _is_finished{};
    NLJoinProber _prober;
    const std::shared_ptr<NLJoinContext>& _cross_join_context;
    spill::SpillerFactoryPtr _spill_factory;
    std::shared_ptr<spill::Spiller> _spiller;
    mutable std::unique_ptr<SpillableNLJoinChunkStream> _chunk_stream;
};

class SpillableNLJoinProbeOperatorFactory final : public OperatorWithDependencyFactory {
public:
    SpillableNLJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id, const RowDescriptor& row_descriptor,
                                        const RowDescriptor& left_row_desc, const RowDescriptor& right_row_desc,
                                        std::string sql_join_conjuncts, std::vector<ExprContext*>&& join_conjuncts,
                                        std::vector<ExprContext*>&& conjunct_ctxs,
                                        std::shared_ptr<NLJoinContext>&& cross_join_context, TJoinOp::type join_op)
            : OperatorWithDependencyFactory(id, "spillable_nl_join_left", plan_node_id),
              _join_op(join_op),
              _left_row_desc(left_row_desc),
              _right_row_desc(right_row_desc),
              _sql_join_conjuncts(std::move(sql_join_conjuncts)),
              _join_conjuncts(std::move(join_conjuncts)),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _cross_join_context(std::move(cross_join_context)) {}

    ~SpillableNLJoinProbeOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    void _init_row_desc();

    const TJoinOp::type _join_op;
    const RowDescriptor& _left_row_desc;
    const RowDescriptor& _right_row_desc;

    Buffer<SlotDescriptor*> _col_types;
    size_t _probe_column_count = 0;
    size_t _build_column_count = 0;

    std::string _sql_join_conjuncts;
    std::vector<ExprContext*> _join_conjuncts;
    std::vector<ExprContext*> _conjunct_ctxs;

    std::shared_ptr<NLJoinContext> _cross_join_context;
};

} // namespace starrocks::pipeline