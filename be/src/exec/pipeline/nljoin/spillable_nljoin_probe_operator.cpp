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

#include "exec/pipeline/nljoin/spillable_nljoin_probe_operator.h"

#include <glog/logging.h>

#include <memory>

#include "common/statusor.h"
#include "exec/spill/common.h"
#include "exec/spill/spiller_factory.h"

namespace starrocks::pipeline {

NLJoinProber::NLJoinProber(TJoinOp::type join_op, const std::vector<ExprContext*>& join_conjuncts,
                           const std::vector<ExprContext*>& conjunct_ctxs,
                           const std::vector<SlotDescriptor*>& col_types, size_t probe_column_count)
        : _join_op(join_op),
          _col_types(col_types),
          _probe_column_count(probe_column_count),
          _join_conjuncts(join_conjuncts),
          _conjunct_ctxs(conjunct_ctxs) {}

Status NLJoinProber::prepare(RuntimeState* state, RuntimeProfile* profile) {
    _permute_rows_counter = ADD_COUNTER(profile, "PermuteRows", TUnit::UNIT);
    _permute_left_rows_counter = ADD_COUNTER(profile, "PermuteLeftJoinRows", TUnit::UNIT);

    return Status::OK();
}

Status NLJoinProber::push_probe_chunk(const ChunkPtr& chunk) {
    _probe_chunk = chunk;
    _probe_row_current = 0;
    return Status::OK();
}

StatusOr<ChunkPtr> NLJoinProber::probe_chunk(RuntimeState* state, const ChunkPtr& build_chunk) {
    // probe chunk
    auto output_chunk = _init_output_chunk(state, build_chunk);
    //
    _permute_chunk(state, build_chunk, output_chunk);
    //
    return output_chunk;
}

ChunkPtr NLJoinProber::_init_output_chunk(RuntimeState* state, const ChunkPtr& build_chunk) {
    ChunkPtr chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < _col_types.size(); i++) {
        SlotDescriptor* slot = _col_types[i];
        bool is_probe = i < _probe_column_count;
        bool nullable = _col_types[i]->is_nullable();
        // OUTER JOIN must be nullable
        if ((is_probe && is_right_join()) || (!is_probe && is_left_join())) {
            nullable = true;
        }
        // Right side of LEFT SEMI/ANTI JOIN must be nullable
        if (!is_probe && (is_left_anti_join() || is_left_semi_join())) {
            nullable = true;
        }
        if (is_probe && _probe_chunk) {
            nullable |= _probe_chunk->get_column_by_slot_id(slot->id())->is_nullable();
        }
        if (!is_probe && build_chunk) {
            nullable |= build_chunk->get_column_by_slot_id(slot->id())->is_nullable();
        }
        ColumnPtr new_col = ColumnHelper::create_column(slot->type(), nullable);
        chunk->append_column(new_col, slot->id());
    }

    chunk->reserve(state->chunk_size());
    return chunk;
}

void NLJoinProber::_permute_chunk(RuntimeState* state, const ChunkPtr& build_chunk, const ChunkPtr& output) {
    for (; _probe_row_current < _probe_chunk->num_rows(); ++_probe_row_current) {
        if (output->num_rows() + build_chunk->num_rows() > state->chunk_size()) {
            DCHECK_LE(output->num_rows(), state->chunk_size());
            return;
        }
        _permute_probe_row(output.get(), build_chunk);
    }
}

void NLJoinProber::_permute_probe_row(Chunk* dst, const ChunkPtr& build_chunk) {
    DCHECK(build_chunk);
    size_t cur_build_chunk_rows = build_chunk->num_rows();
    COUNTER_UPDATE(_permute_rows_counter, cur_build_chunk_rows);
    for (size_t i = 0; i < _col_types.size(); i++) {
        bool is_probe = i < _probe_column_count;
        SlotDescriptor* slot = _col_types[i];
        ColumnPtr& dst_col = dst->get_column_by_slot_id(slot->id());
        if (is_probe) {
            ColumnPtr& src_col = _probe_chunk->get_column_by_slot_id(slot->id());
            dst_col->append_value_multiple_times(*src_col, _probe_row_current, cur_build_chunk_rows);
        } else {
            ColumnPtr& src_col = build_chunk->get_column_by_slot_id(slot->id());
            dst_col->append(*src_col);
        }
    }
}

SpillableNLJoinProbeOperator::SpillableNLJoinProbeOperator(
        OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence, TJoinOp::type join_op,
        const std::string& sql_join_conjuncts, const std::vector<ExprContext*>& join_conjuncts,
        const std::vector<ExprContext*>& conjunct_ctxs, const std::vector<SlotDescriptor*>& col_types,
        size_t probe_column_count, const std::shared_ptr<NLJoinContext>& cross_join_context)
        : OperatorWithDependency(factory, id, "spillable_nestloop_join_probe", plan_node_id, false, driver_sequence),
          _prober(join_op, join_conjuncts, conjunct_ctxs, col_types, probe_column_count),
          _cross_join_context(cross_join_context) {}

Status SpillableNLJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _accumulator.set_desired_size(state->chunk_size());
    RETURN_IF_ERROR(_prober.prepare(state, _unique_metrics.get()));
    _spill_factory = std::make_shared<spill::SpillerFactory>();
    _spiller = _spill_factory->create({});
    _spiller->set_metrics(spill::SpillProcessMetrics(_unique_metrics.get(), state->mutable_total_spill_bytes()));
    _cross_join_context->incr_prober();
    return Status::OK();
}

void SpillableNLJoinProbeOperator::close(RuntimeState* state) {
    _cross_join_context->decr_prober(state);
    Operator::close(state);
}

bool SpillableNLJoinProbeOperator::is_ready() const {
    auto res = _cross_join_context->is_right_finished();
    if (res) {
        _init_chunk_stream();
    }
    return res;
}

bool SpillableNLJoinProbeOperator::is_finished() const {
    return _is_finished || (_is_finishing && _prober.probe_finished() && _is_current_build_probe_finished());
}

bool SpillableNLJoinProbeOperator::has_output() const {
    return !_is_current_build_probe_finished() && _chunk_stream && _chunk_stream->has_output();
}

bool SpillableNLJoinProbeOperator::need_input() const {
    return _prober.probe_finished() && _is_current_build_probe_finished();
}

Status SpillableNLJoinProbeOperator::set_finishing(RuntimeState* state) {
    // set finishing
    _is_finishing = true;
    return Status::OK();
}

Status SpillableNLJoinProbeOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    return _cross_join_context->finish_one_left_prober(state);
}

StatusOr<ChunkPtr> SpillableNLJoinProbeOperator::pull_chunk(RuntimeState* state) {
    TRACE_SPILL_LOG << "pull_chunk:" << _driver_sequence;
    if (_prober.probe_finished() || _build_chunk == nullptr || _build_chunk->is_empty()) {
        auto chunk_st = _chunk_stream->get_next(state, _executor());
        if (chunk_st.status().is_end_of_file()) {
            _prober.reset();
            _set_current_build_probe_finished(true);
            RETURN_IF_ERROR(_chunk_stream->reset(state, _spiller.get()));
            return nullptr;
        }
        ASSIGN_OR_RETURN(_build_chunk, std::move(chunk_st));

        if (_build_chunk == nullptr || _build_chunk->is_empty()) {
            return nullptr;
        }

        _prober.reset_probe();
    }
    // if probe finished after reset probe side. it means probe side is empty
    if (_prober.probe_finished()) {
        _set_current_build_probe_finished(true);
        return nullptr;
    }

    ASSIGN_OR_RETURN(auto res, _prober.probe_chunk(state, _build_chunk));
    RETURN_IF_ERROR(eval_conjuncts(_prober.conjunct_ctxs(), res.get(), nullptr));

    return res;
}

Status SpillableNLJoinProbeOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    TRACE_SPILL_LOG << "push_chunk:" << _driver_sequence;
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }
    _set_current_build_probe_finished(false);
    RETURN_IF_ERROR(_prober.push_probe_chunk(chunk));
    RETURN_IF_ERROR(_chunk_stream->reset(state, _spiller.get()));
    RETURN_IF_ERROR(_chunk_stream->prefetch(state, _executor()));
    return Status::OK();
}

void SpillableNLJoinProbeOperator::_init_chunk_stream() const {
    if (_chunk_stream == nullptr) {
        _chunk_stream = _cross_join_context->builder().build_stream();
    }
}

spill::IOTaskExecutor& SpillableNLJoinProbeOperator::_executor() {
    return *_cross_join_context->spill_channel_factory()->executor();
}

void SpillableNLJoinProbeOperatorFactory::_init_row_desc() {
    for (auto& tuple_desc : _left_row_desc.tuple_descriptors()) {
        for (auto& slot : tuple_desc->slots()) {
            _col_types.emplace_back(slot);
            _probe_column_count++;
        }
    }
    for (auto& tuple_desc : _right_row_desc.tuple_descriptors()) {
        for (auto& slot : tuple_desc->slots()) {
            _col_types.emplace_back(slot);
            _build_column_count++;
        }
    }
}

OperatorPtr SpillableNLJoinProbeOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<SpillableNLJoinProbeOperator>(this, _id, _plan_node_id, driver_sequence, _join_op,
                                                          _sql_join_conjuncts, _join_conjuncts, _conjunct_ctxs,
                                                          _col_types, _probe_column_count, _cross_join_context);
}

Status SpillableNLJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorWithDependencyFactory::prepare(state));

    _cross_join_context->ref();

    _init_row_desc();
    RETURN_IF_ERROR(Expr::prepare(_join_conjuncts, state));
    RETURN_IF_ERROR(Expr::open(_join_conjuncts, state));
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));

    return Status::OK();
}

void SpillableNLJoinProbeOperatorFactory::close(RuntimeState* state) {
    Expr::close(_join_conjuncts, state);
    Expr::close(_conjunct_ctxs, state);

    OperatorWithDependencyFactory::close(state);
}

} // namespace starrocks::pipeline
