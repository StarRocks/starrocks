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

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "common/statusor.h"
#include "compute_env/spill/common.h"
#include "compute_env/spill/options.h"
#include "compute_env/spill/spiller_factory.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/runtime_compat/runtime_state_helper.h"
#include "exprs/expr_executor.h"

namespace starrocks::pipeline {
namespace {

// Total bytes a binary column occupies. For a const column the payload is stored once but
// logically repeats for every row, so scale it by the row count.
size_t nljoin_binary_payload_bytes(const Column* column) {
    const Column* data = ColumnHelper::get_data_column(column);
    if (!data->is_binary()) {
        return 0;
    }
    const size_t payload = down_cast<const BinaryColumn*>(data)->get_immutable_bytes().size();
    if (column->is_constant()) {
        return payload * column->size();
    }
    return payload;
}

// Bytes of a single row's value in a binary column.
size_t nljoin_binary_row_bytes(const Column* column, size_t idx) {
    const Column* data = ColumnHelper::get_data_column(column);
    if (!data->is_binary()) {
        return 0;
    }
    const auto& binary = down_cast<const BinaryColumn&>(*data);
    const auto& offsets = binary.get_offset();
    if (offsets.size() < 2) {
        return 0;
    }
    if (column->is_constant() || idx + 1 >= offsets.size()) {
        return static_cast<size_t>(offsets[1] - offsets[0]);
    }
    return static_cast<size_t>(offsets[idx + 1] - offsets[idx]);
}

// BinaryColumn addresses its bytes with uint32 offsets, so a column may hold at most
// Column::MAX_CAPACITY_LIMIT bytes before the offsets wrap.
bool nljoin_binary_add_exceeds(uint64_t dest_bytes, uint64_t add_bytes) {
    return dest_bytes + add_bytes >= Column::MAX_CAPACITY_LIMIT;
}

} // namespace

NLJoinProber::NLJoinProber(TJoinOp::type join_op, const std::vector<ExprContext*>& join_conjuncts,
                           const std::vector<ExprContext*>& conjunct_ctxs,
                           const std::map<SlotId, ExprContext*>& common_expr_ctxs,
                           const std::vector<SlotDescriptor*>& col_types, size_t probe_column_count)
        : _join_op(join_op),
          _col_types(col_types),
          _probe_column_count(probe_column_count),
          _join_conjuncts(join_conjuncts),
          _conjunct_ctxs(conjunct_ctxs),
          _common_expr_ctxs(common_expr_ctxs) {}

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
    RETURN_IF_ERROR(_permute_chunk(state, build_chunk, output_chunk));
    RETURN_IF_ERROR(output_chunk->upgrade_if_overflow());
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
        MutableColumnPtr new_col = ColumnHelper::create_column(slot->type(), nullable);
        chunk->append_column(std::move(new_col), slot->id());
    }

    chunk->reserve(state->chunk_size());
    return chunk;
}

Status NLJoinProber::_permute_chunk(RuntimeState* state, const ChunkPtr& build_chunk, const ChunkPtr& output) {
    for (; _probe_row_current < _probe_chunk->num_rows(); ++_probe_row_current) {
        if (output->num_rows() + build_chunk->num_rows() > state->chunk_size()) {
            DCHECK_LE(output->num_rows(), state->chunk_size());
            return Status::OK();
        }
        // BinaryColumn offsets are uint32. Repeating a large VARCHAR/JSON value across a big build
        // chunk can exceed 4GB in a single permute, wrapping the offsets. Stop before that happens.
        if (_permute_probe_row_exceeds_binary_limit(output.get(), build_chunk)) {
            if (output->num_rows() == 0) {
                return Status::CapacityLimitExceed(
                        "NestLoop join output VARCHAR/binary column would exceed 4GB in a single permute. "
                        "Extract compact fields before the cross join or reduce input row size.");
            }
            return Status::OK();
        }
        _permute_probe_row(output.get(), build_chunk);
    }
    return Status::OK();
}

bool NLJoinProber::_permute_probe_row_exceeds_binary_limit(const Chunk* dst, const ChunkPtr& build_chunk) const {
    const size_t build_rows = build_chunk->num_rows();
    for (size_t i = 0; i < _col_types.size(); i++) {
        const SlotId slot_id = _col_types[i]->id();
        const Column* dest = dst->get_column_by_slot_id(slot_id).get();
        const uint64_t dest_bytes = nljoin_binary_payload_bytes(dest);
        uint64_t add_bytes = 0;
        if (i < _probe_column_count) {
            const Column* src = _probe_chunk->get_column_by_slot_id(slot_id).get();
            add_bytes = static_cast<uint64_t>(nljoin_binary_row_bytes(src, _probe_row_current)) * build_rows;
        } else {
            const Column* src = build_chunk->get_column_by_slot_id(slot_id).get();
            add_bytes = nljoin_binary_payload_bytes(src);
        }
        if (nljoin_binary_add_exceeds(dest_bytes, add_bytes)) {
            return true;
        }
    }
    return false;
}

void NLJoinProber::_permute_probe_row(Chunk* dst, const ChunkPtr& build_chunk) {
    DCHECK(build_chunk);
    size_t cur_build_chunk_rows = build_chunk->num_rows();
    COUNTER_UPDATE(_permute_rows_counter, cur_build_chunk_rows);
    for (size_t i = 0; i < _col_types.size(); i++) {
        bool is_probe = i < _probe_column_count;
        SlotDescriptor* slot = _col_types[i];
        auto* dst_col = dst->get_column_raw_ptr_by_slot_id(slot->id());
        if (is_probe) {
            const ColumnPtr& src_col = _probe_chunk->get_column_by_slot_id(slot->id());
            dst_col->append_value_multiple_times(*src_col, _probe_row_current, cur_build_chunk_rows);
        } else {
            const ColumnPtr& src_col = build_chunk->get_column_by_slot_id(slot->id());
            dst_col->append(*src_col);
        }
    }
}

SpillableNLJoinProbeOperator::SpillableNLJoinProbeOperator(
        OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence, TJoinOp::type join_op,
        const std::string& sql_join_conjuncts, const std::vector<ExprContext*>& join_conjuncts,
        const std::vector<ExprContext*>& conjunct_ctxs, const std::map<SlotId, ExprContext*>& common_expr_ctxs,
        const std::vector<SlotDescriptor*>& col_types, size_t probe_column_count,
        const std::shared_ptr<NLJoinContext>& cross_join_context)
        : OperatorWithDependency(factory, id, "spillable_nestloop_join_probe", plan_node_id, false, driver_sequence),
          _prober(join_op, join_conjuncts, conjunct_ctxs, common_expr_ctxs, col_types, probe_column_count),
          _cross_join_context(cross_join_context) {}

Status SpillableNLJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _accumulator.set_desired_size(state->chunk_size());
    RETURN_IF_ERROR(_prober.prepare(state, _unique_metrics.get()));
    _spill_factory = std::make_shared<spill::SpillerFactory>();
    spill::SpilledOptions opts;
    opts.wg = state->fragment_runtime_state()->workgroup();
    _spiller = _spill_factory->create(opts);
    _spiller->set_metrics(
            spill::SpillProcessMetrics(_unique_metrics.get(), RuntimeStateHelper::mutable_total_spill_bytes(state)));
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
    if (!is_ready()) {
        return false;
    }
    RETURN_TRUE_IF_SPILL_TASK_ERROR(_spiller);
    return !_is_current_build_probe_finished() && _chunk_stream && _chunk_stream->has_output();
}

bool SpillableNLJoinProbeOperator::need_input() const {
    if (!is_ready()) {
        return false;
    }
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
    RETURN_IF_ERROR(_spiller->task_status());
    if (_prober.probe_finished() || _build_chunk == nullptr || _build_chunk->is_empty()) {
        auto chunk_st = _chunk_stream->get_next(state);
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
    RETURN_IF_ERROR(_chunk_stream->prefetch(state));
    return Status::OK();
}

void SpillableNLJoinProbeOperator::_init_chunk_stream() const {
    if (_chunk_stream == nullptr) {
        _chunk_stream = _cross_join_context->builder().build_stream();
    }
}

void SpillableNLJoinProbeOperatorFactory::_init_col_types() {
    for (auto* slot : _left_record_desc.slots()) {
        _col_types.emplace_back(slot);
        _probe_column_count++;
    }
    for (auto* slot : _right_record_desc.slots()) {
        _col_types.emplace_back(slot);
        _build_column_count++;
    }
}

OperatorPtr SpillableNLJoinProbeOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<SpillableNLJoinProbeOperator>(
            this, _id, _plan_node_id, driver_sequence, _join_op, _sql_join_conjuncts, _join_conjuncts, _conjunct_ctxs,
            _common_expr_ctxs, _col_types, _probe_column_count, _cross_join_context);
}

Status SpillableNLJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorWithDependencyFactory::prepare(state));

    _cross_join_context->ref();

    _init_col_types();
    RETURN_IF_ERROR(ExprExecutor::prepare(_join_conjuncts, state));
    RETURN_IF_ERROR(ExprExecutor::open(_join_conjuncts, state));
    RETURN_IF_ERROR(ExprExecutor::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(ExprExecutor::open(_conjunct_ctxs, state));

    return Status::OK();
}

void SpillableNLJoinProbeOperatorFactory::close(RuntimeState* state) {
    ExprExecutor::close(_join_conjuncts, state);
    ExprExecutor::close(_conjunct_ctxs, state);

    OperatorWithDependencyFactory::close(state);
}

} // namespace starrocks::pipeline
