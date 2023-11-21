// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/nljoin/nljoin_probe_operator.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "simd/simd.h"

namespace starrocks::pipeline {

NLJoinProbeOperator::NLJoinProbeOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                         int32_t driver_sequence, TJoinOp::type join_op,
                                         const std::string& sql_join_conjuncts,
                                         const std::vector<ExprContext*>& join_conjuncts,
                                         const std::vector<ExprContext*>& conjunct_ctxs,
                                         const std::vector<SlotDescriptor*>& col_types, size_t probe_column_count,
                                         const std::shared_ptr<NLJoinContext>& cross_join_context)
        : OperatorWithDependency(factory, id, "nestloop_join_probe", plan_node_id, driver_sequence),
          _join_op(join_op),
          _col_types(col_types),
          _probe_column_count(probe_column_count),
          _sql_join_conjuncts(sql_join_conjuncts),
          _join_conjuncts(join_conjuncts),
          _conjunct_ctxs(conjunct_ctxs),
          _cross_join_context(cross_join_context) {
    _cross_join_context->ref();
}

Status NLJoinProbeOperator::prepare(RuntimeState* state) {
    _output_accumulator.set_desired_size(state->chunk_size());

    _unique_metrics->add_info_string("JoinType", to_string(_join_op));
    _unique_metrics->add_info_string("JoinConjuncts", _sql_join_conjuncts);

    _permute_rows_counter = ADD_COUNTER(_unique_metrics, "PermuteRows", TUnit::UNIT);
    if (_is_left_join() || _is_left_anti_join()) {
        _permute_left_rows_counter = ADD_COUNTER(_unique_metrics, "PermuteLeftJoinRows", TUnit::UNIT);
    }
    return Operator::prepare(state);
}

void NLJoinProbeOperator::close(RuntimeState* state) {
    _cross_join_context->unref(state);
    Operator::close(state);
}

bool NLJoinProbeOperator::is_ready() const {
    bool res = _cross_join_context->is_right_finished();
    if (res) {
        _init_build_match();
    }
    return res;
}

bool NLJoinProbeOperator::_is_curr_probe_chunk_finished() const {
    return _probe_chunk == nullptr || _probe_row_current >= _probe_chunk->num_rows();
}

void NLJoinProbeOperator::_advance_join_stage(JoinStage stage) const {
    DCHECK_LE(_join_stage, stage) << "current=" << _join_stage << ", advance to " << stage;
    if (_join_stage != stage) {
        _join_stage = stage;
        VLOG(3) << fmt::format("operator {} enter join_stage {}", _driver_sequence, stage);
    }
}

bool NLJoinProbeOperator::_skip_probe() const {
    // Empty build table could skip probe unless it's LEFT/FULL OUTER JOIN or LEFT ANTI JOIN
    return is_ready() && !_is_left_join() && !_is_left_anti_join() && _cross_join_context->is_build_chunk_empty();
}

void NLJoinProbeOperator::_check_post_probe() const {
    if (_input_finished) {
        _output_accumulator.finalize();
    }
    bool skip_probe = _skip_probe();
    bool output_finished = _is_curr_probe_chunk_finished() && _output_accumulator.empty();

    if ((_input_finished && output_finished) || skip_probe) {
        switch (_join_stage) {
        case Probe: {
            if (!_is_right_join() || !_cross_join_context->finish_probe(_driver_sequence, _self_build_match_flag)) {
                _advance_join_stage(JoinStage::Finished);
            } else {
                _advance_join_stage(JoinStage::RightJoin);
            }
            break;
        }
        case RightJoin:
            // It should be advanced to PostRightJoin
            break;
        case PostRightJoin: {
            if (output_finished) {
                _advance_join_stage(JoinStage::Finished);
            }
            break;
        }
        case Finished:
            break;
        }
    }
}

bool NLJoinProbeOperator::has_output() const {
    _check_post_probe();
    return _join_stage != JoinStage::Finished;
}

bool NLJoinProbeOperator::need_input() const {
    if (!is_ready()) {
        return false;
    }
    if (_skip_probe()) {
        return false;
    }

    return _is_curr_probe_chunk_finished();
}

bool NLJoinProbeOperator::is_finished() const {
    return (_input_finished || _skip_probe()) && !has_output();
}

Status NLJoinProbeOperator::set_finishing(RuntimeState* state) {
    _check_post_probe();
    _input_finished = true;

    return Status::OK();
}

Status NLJoinProbeOperator::set_finished(RuntimeState* state) {
    return _cross_join_context->finish_one_left_prober(state);
}

bool NLJoinProbeOperator::_is_left_join() const {
    return _join_op == TJoinOp::LEFT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN;
}

bool NLJoinProbeOperator::_is_right_join() const {
    return _join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN;
}

bool NLJoinProbeOperator::_is_left_semi_join() const {
    return _join_op == TJoinOp::LEFT_SEMI_JOIN;
}

bool NLJoinProbeOperator::_is_left_anti_join() const {
    return _join_op == TJoinOp::LEFT_ANTI_JOIN;
}

int NLJoinProbeOperator::_num_build_chunks() const {
    return _cross_join_context->num_build_chunks();
}

void NLJoinProbeOperator::_next_build_chunk_index_for_other_join() {
    _move_build_chunk_index(_curr_build_chunk_index + 1);
}

// probe one chunk from right
void NLJoinProbeOperator::_next_probe_row_index_for_inner_join() {
    // next probe row
    _probe_row_current++;
    // probe chunk iter over
    if (_probe_row_current >= _probe_chunk->num_rows()) {
        // next build chunk
        _curr_build_chunk_index++;
        if (_curr_build_chunk_index >= _num_build_chunks()) {
            // build chunks iter over
            _curr_build_chunk_index = 0;
            _probe_row_current = 0;
            _probe_chunk = nullptr;
        } else {
            // next build chunk
            _probe_row_current = 0;
            _curr_build_chunk = _cross_join_context->get_build_chunk(_curr_build_chunk_index);
            _build_row_current = 0;
        }
    }
}

// probe one chunk from left
void NLJoinProbeOperator::_next_build_row_index_for_inner_join() {
    // next build row
    _build_row_current++;
    // all rows of current build chunk iter over
    if (_build_row_current >= _curr_build_chunk->num_rows()) {
        // next build chunk
        _curr_build_chunk_index++;
        _build_row_current = 0;
        // all build chunks iter over
        if (_curr_build_chunk_index >= _num_build_chunks()) {
            _probe_chunk = nullptr;
        } else {
            // get next build chunk
            _curr_build_chunk = _cross_join_context->get_build_chunk(_curr_build_chunk_index);
        }
    }
    // reset probe row index
    _probe_row_current = 0;
}

void NLJoinProbeOperator::_move_build_chunk_index(int index) {
    DCHECK_GE(index, 0);
    DCHECK_LE(index, _num_build_chunks());
    if (_curr_build_chunk) {
        _prev_chunk_start = _cross_join_context->get_build_chunk_size() * _curr_build_chunk_index;
        _prev_chunk_size = _curr_build_chunk->num_rows();
    }
    if (index < _num_build_chunks()) {
        _curr_build_chunk = _cross_join_context->get_build_chunk(index);
    } else {
        _curr_build_chunk = nullptr;
    }
    _curr_build_chunk_index = index;
}

ChunkPtr NLJoinProbeOperator::_init_output_chunk(size_t chunk_size) const {
    ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    bool left_to_nullable = _is_right_join();
    bool right_to_nullable = _is_left_join() || _is_left_anti_join() || _is_left_semi_join();

    for (size_t i = 0; i < _probe_column_count; i++) {
        SlotDescriptor* slot = _col_types[i];
        bool nullable = left_to_nullable | _col_types[i]->is_nullable();
        if (_probe_chunk) {
            nullable |= _probe_chunk->is_column_nullable(slot->id());
        }
        ColumnPtr new_col = vectorized::ColumnHelper::create_column(slot->type(), nullable);
        chunk->append_column(new_col, slot->id());
    }
    for (size_t i = _probe_column_count; i < _col_types.size(); i++) {
        SlotDescriptor* slot = _col_types[i];
        bool nullable = right_to_nullable | _col_types[i]->is_nullable();
        if (_curr_build_chunk) {
            nullable |= _curr_build_chunk->is_column_nullable(slot->id());
        }
        ColumnPtr new_col = vectorized::ColumnHelper::create_column(slot->type(), nullable);
        chunk->append_column(new_col, slot->id());
    }

    chunk->reserve(chunk_size);
    return chunk;
}

void NLJoinProbeOperator::iterate_enumerate_chunk(const ChunkPtr& chunk,
                                                  std::function<void(bool, size_t, size_t)> call) {
    if (_num_build_chunks() == 1) {
        // Multiple probe rows with one build chunk
        size_t num_build_rows = _cross_join_context->num_build_rows();
        for (size_t i = 0; i < chunk->num_rows(); i += num_build_rows) {
            call(true, i, i + num_build_rows);
        }
    } else {
        // Partial probe row
        call(false, 0, chunk->num_rows());
    }
}

Status NLJoinProbeOperator::_probe_for_inner_join(const ChunkPtr& chunk) {
    if (!_join_conjuncts.empty() && chunk && !chunk->is_empty()) {
        RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_join_conjuncts, chunk.get(), nullptr, true));
    }
    return Status::OK();
}

Status NLJoinProbeOperator::_probe_for_other_join(const ChunkPtr& chunk) {
    vectorized::FilterPtr filter;

    // directly return all probe chunk when it's left anti join and right input is empty
    if (_is_left_anti_join() && _num_build_chunks() == 0) {
        _permute_left_join(chunk, 0, _probe_chunk->num_rows());
        return Status::OK();
    }

    bool apply_filter = (!_is_left_semi_join() && !_is_left_anti_join()) || _num_build_chunks() == 0;
    if (!_join_conjuncts.empty() && chunk && !chunk->is_empty()) {
        size_t rows = chunk->num_rows();
        RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_join_conjuncts, chunk.get(), &filter, apply_filter));
        DCHECK(!!filter);
        // The filter has not been assigned if no rows matched
        if (chunk->num_rows() == 0) {
            filter->assign(rows, 0);
        }
    }

    if (_is_left_join()) {
        // If join conjuncts are empty, most join type do not need to filter data
        // Except left join and the right table is empty, in which it could not permute any chunk
        // So here we need to permute_left_join for this case
        if (_num_build_chunks() == 0) {
            // Empty right table
            DCHECK_EQ(_probe_row_current, _probe_chunk->num_rows());
            _permute_left_join(chunk, 0, _probe_chunk->num_rows());
        }
        if (filter) {
            if (_num_build_chunks() == 1) {
                // Multiple probe rows
                size_t num_build_rows = _cross_join_context->num_build_rows();
                DCHECK_GE(filter->size(), num_build_rows);
                DCHECK_LE(_probe_row_start, _probe_row_current);
                for (size_t i = 0; i < filter->size(); i += num_build_rows) {
                    bool probe_matched = SIMD::contain_nonzero(*filter, i, num_build_rows);
                    if (!probe_matched) {
                        size_t probe_row_index = _probe_row_start + i / num_build_rows;
                        _permute_left_join(chunk, probe_row_index, 1);
                    }
                }
            } else {
                _probe_row_matched = _probe_row_matched || SIMD::contain_nonzero(*filter);
                if (!_probe_row_matched && _probe_row_finished) {
                    _permute_left_join(chunk, _probe_row_current, 1);
                }
            }
        }
    }

    if ((_is_left_semi_join() || _is_left_anti_join()) && chunk->num_rows() > 0) {
        if (!filter && chunk->num_rows() > 0) {
            filter = std::make_shared<vectorized::Filter>(chunk->num_rows(), 0);
            if (_is_left_semi_join()) {
                (*filter)[0] = 1;
            } else {
                (*filter)[0] = 0;
            }
        }
        iterate_enumerate_chunk(chunk, [&](bool complete_probe_row, size_t start, size_t end) {
            size_t first_matched = SIMD::find_nonzero(*filter, start, end - start);
            std::fill(filter->begin() + start, filter->begin() + end, 0);
            if (_is_left_semi_join()) {
                // Keep the first matched now
                if (first_matched < end) {
                    (*filter)[first_matched] = 1;
                    // Finish current probe row once semi-join matched
                    _probe_row_finished = true;
                }
            } else if (_is_left_anti_join()) {
                // Keep the first row if all nows not matched
                if (first_matched == end) {
                    if (complete_probe_row || _probe_row_finished) {
                        (*filter)[start] = 1;
                    }
                } else {
                    // Once matched, this row would be thrown
                    _probe_row_finished = true;
                }
            }
        });
        chunk->filter(*filter);
    }

    if (_is_right_join()) {
        // If the filter and join_conjuncts are empty, it means join conjunct is always true
        // So we need to mark the build_match_flag for all rows
        if (_join_conjuncts.empty()) {
            DCHECK(!filter);
            _self_build_match_flag.assign(_self_build_match_flag.size(), 1);
        } else if (filter) {
            bool multi_probe_rows = _num_build_chunks() == 1;
            if (multi_probe_rows) {
                size_t num_build_rows = _cross_join_context->num_build_rows();
                DCHECK_GE(filter->size(), num_build_rows);
                for (size_t i = 0; i < filter->size(); i += num_build_rows) {
                    vectorized::ColumnHelper::or_two_filters(&_self_build_match_flag, filter->data() + i);
                }
            } else {
                DCHECK_LE(_prev_chunk_size + _prev_chunk_start, _self_build_match_flag.size());
                DCHECK_EQ(_prev_chunk_size, filter->size());
                vectorized::ColumnHelper::or_two_filters(
                        _prev_chunk_size, _self_build_match_flag.data() + _prev_chunk_start, filter->data());
            }
            VLOG(3) << fmt::format("NLJoin operator {} set build_flags for right join, filter={}, flags={}",
                                   _driver_sequence, fmt::join(*filter, ","), fmt::join(_self_build_match_flag, ","));
        }
    }

    return Status::OK();
}

ChunkPtr NLJoinProbeOperator::_permute_chunk_for_inner_join(size_t chunk_size) {
    ChunkPtr result_chunk = _init_output_chunk(chunk_size);

    do {
        size_t left_chunk_size = _probe_chunk->num_rows();
        size_t right_chunk_size = _curr_build_chunk->num_rows();
        size_t max_chunk_size = std::max(left_chunk_size, right_chunk_size);
        if (result_chunk->num_rows() != 0 && result_chunk->num_rows() + max_chunk_size > chunk_size) {
            // Prevent the size of a chunk from exceeding chunk size
            break;
        }

        if (left_chunk_size > right_chunk_size) {
            _permute_chunk_base_left(&result_chunk);
            _next_build_row_index_for_inner_join();
        } else {
            _permute_chunk_base_right(&result_chunk);
            _next_probe_row_index_for_inner_join();
        }
    } while (result_chunk->num_rows() < chunk_size && _probe_chunk != nullptr && _curr_build_chunk != nullptr);

    return result_chunk;
}

void NLJoinProbeOperator::_permute_chunk_base_left(ChunkPtr* chunk) {
    for (size_t i = 0; i < _probe_column_count; i++) {
        SlotId slot_id = _col_types[i]->id();
        ColumnPtr& dest_col = (*chunk)->get_column_by_slot_id(slot_id);
        const ColumnPtr& src_col = _probe_chunk->get_column_by_slot_id(slot_id);
        dest_col->append(*src_col);
    }
    for (size_t i = _probe_column_count; i < _col_types.size(); i++) {
        SlotId slot_id = _col_types[i]->id();
        ColumnPtr& dest_col = (*chunk)->get_column_by_slot_id(slot_id);
        const ColumnPtr& src_col = _curr_build_chunk->get_column_by_slot_id(slot_id);
        dest_col->append_value_multiple_times(*src_col, _build_row_current, _probe_chunk->num_rows());
    }
}

void NLJoinProbeOperator::_permute_chunk_base_right(ChunkPtr* chunk) {
    for (size_t i = 0; i < _probe_column_count; i++) {
        SlotId slot_id = _col_types[i]->id();
        ColumnPtr& dest_col = (*chunk)->get_column_by_slot_id(slot_id);
        const ColumnPtr& src_col = _probe_chunk->get_column_by_slot_id(slot_id);
        dest_col->append_value_multiple_times(*src_col, _probe_row_current, _curr_build_chunk->num_rows());
    }
    for (size_t i = _probe_column_count; i < _col_types.size(); i++) {
        SlotId slot_id = _col_types[i]->id();
        ColumnPtr& dest_col = (*chunk)->get_column_by_slot_id(slot_id);
        const ColumnPtr& src_col = _curr_build_chunk->get_column_by_slot_id(slot_id);
        dest_col->append(*src_col);
    }
}

// Permute enough rows from build side and probe side
// The chunk either consists two conditions:
// 1. Multiple probe rows and multiple build single-chunk
// 2. One probe rows and one build chunk
ChunkPtr NLJoinProbeOperator::_permute_chunk_for_other_join(size_t chunk_size) {
    // TODO: optimize the loop order for small build chunk
    ChunkPtr chunk = _init_output_chunk(chunk_size);
    bool probe_started = false;
    _probe_row_start = 0;
    auto probe_row_start = [&]() {
        if (!probe_started) {
            probe_started = true;
            _probe_row_start = _probe_row_current;
        }
    };
    for (; _probe_row_current < _probe_chunk->num_rows(); ++_probe_row_current) {
        // Last build chunk must permute a chunk
        bool is_last_build_chunk = _curr_build_chunk_index == _num_build_chunks() - 1 && _num_build_chunks() > 1;
        if (!_probe_row_finished && is_last_build_chunk) {
            _permute_probe_row(chunk);
            _move_build_chunk_index(0);
            _probe_row_finished = true;
            probe_row_start();
            return chunk;
        }

        // For SEMI/ANTI JOIN, the probe-row could be skipped once find matched/unmatched
        // Otherwise accumulate more build chunks into a larger chunk
        while (!_probe_row_finished && _curr_build_chunk_index < _num_build_chunks()) {
            _permute_probe_row(chunk);
            _next_build_chunk_index_for_other_join();
            probe_row_start();
            if (chunk->num_rows() >= chunk_size) {
                return chunk;
            }
        }

        // Move to next probe row
        _probe_row_matched = false;
        _probe_row_finished = false;
        _move_build_chunk_index(0);
    }
    return chunk;
}

// Permute one probe row with current build chunk
void NLJoinProbeOperator::_permute_probe_row(const ChunkPtr& chunk) {
    DCHECK(_curr_build_chunk);
    size_t cur_build_chunk_rows = _curr_build_chunk->num_rows();
    COUNTER_UPDATE(_permute_rows_counter, cur_build_chunk_rows);
    for (size_t i = 0; i < _col_types.size(); i++) {
        bool is_probe = i < _probe_column_count;
        SlotDescriptor* slot = _col_types[i];
        ColumnPtr& dst_col = chunk->get_column_by_slot_id(slot->id());
        if (is_probe) {
            ColumnPtr& src_col = _probe_chunk->get_column_by_slot_id(slot->id());
            dst_col->append_value_multiple_times(*src_col, _probe_row_current, cur_build_chunk_rows);
        } else {
            ColumnPtr& src_col = _curr_build_chunk->get_column_by_slot_id(slot->id());
            dst_col->append(*src_col);
        }
    }
}

// Permute probe side for left join
void NLJoinProbeOperator::_permute_left_join(const ChunkPtr& chunk, size_t probe_row_index, size_t probe_rows) {
    COUNTER_UPDATE(_permute_left_rows_counter, probe_rows);
    for (size_t i = 0; i < _col_types.size(); i++) {
        SlotDescriptor* slot = _col_types[i];
        ColumnPtr& dst_col = chunk->get_column_by_slot_id(slot->id());
        bool is_probe = i < _probe_column_count;
        if (is_probe) {
            ColumnPtr& src_col = _probe_chunk->get_column_by_slot_id(slot->id());
            DCHECK_LT(probe_row_index, src_col->size());
            dst_col->append(*src_col, probe_row_index, probe_rows);
        } else {
            DCHECK(dst_col->is_nullable());
            dst_col->append_nulls(probe_rows);
        }
    }
}

// Permute build side for right join
Status NLJoinProbeOperator::_permute_right_join(size_t chunk_size) {
    const std::vector<uint8_t>& build_match_flag = _cross_join_context->get_shared_build_match_flag();
    if (!SIMD::contain_zero(build_match_flag)) {
        return Status::OK();
    }
    VLOG(2) << "build_match_flag: "
            << fmt::format("{}/{}", SIMD::count_zero(build_match_flag), build_match_flag.size());
    auto build_unmatch_counter = ADD_COUNTER(_unique_metrics, "BuildUnmatchCount", TUnit::UNIT);
    COUNTER_SET(build_unmatch_counter, (int64_t)SIMD::count_zero(build_match_flag));

    size_t match_flag_index = 0;
    int64_t permute_rows = 0;
    for (int chunk_index = 0; chunk_index < _num_build_chunks(); chunk_index++) {
        _move_build_chunk_index(chunk_index);
        size_t cur_chunk_size = _curr_build_chunk->num_rows();

        ChunkPtr chunk = _init_output_chunk(chunk_size);
        for (size_t col = 0; col < _col_types.size(); col++) {
            SlotDescriptor* slot = _col_types[col];
            ColumnPtr& dst_col = chunk->get_column_by_slot_id(slot->id());
            bool is_probe = col < _probe_column_count;
            if (is_probe) {
                size_t nonmatched_count = SIMD::count_zero(build_match_flag.data() + match_flag_index, cur_chunk_size);
                if (nonmatched_count > 0) {
                    dst_col->append_nulls(nonmatched_count);
                }
            } else {
                ColumnPtr& src_col = _curr_build_chunk->get_column_by_slot_id(slot->id());
                for (int i = 0; i < cur_chunk_size; i++) {
                    if (!build_match_flag[match_flag_index + i]) {
                        dst_col->append(*src_col, i, 1);
                    }
                }
            }
        }
        permute_rows += chunk->num_rows();

        RETURN_IF_ERROR(eval_conjuncts(_conjunct_ctxs, chunk.get(), nullptr));
        RETURN_IF_ERROR(_output_accumulator.push(std::move(chunk)));
        match_flag_index += cur_chunk_size;
    }
    auto permute_right_rows_counter = ADD_COUNTER(_unique_metrics, "PermuteRightRows", TUnit::UNIT);
    permute_right_rows_counter->set(permute_rows);
    _output_accumulator.finalize();

    return Status::OK();
}

// Nestloop Join algorithm:
// 1. Permute chunk from build side and probe side, until chunk size reach 4096
// 2. Apply the conjuncts, and append it to output buffer
// 3. Maintain match index and implement left join and right join
StatusOr<vectorized::ChunkPtr> NLJoinProbeOperator::pull_chunk(RuntimeState* state) {
    size_t chunk_size = state->chunk_size();

    if (_join_op == TJoinOp::INNER_JOIN) {
        return _pull_chunk_for_inner_join(chunk_size);
    } else {
        return _pull_chunk_for_other_join(chunk_size);
    }
}

StatusOr<vectorized::ChunkPtr> NLJoinProbeOperator::_pull_chunk_for_other_join(size_t chunk_size) {
    switch (_join_stage) {
    case Probe:
        break;
    case RightJoin: {
        DCHECK(_is_right_join());
        VLOG(3) << fmt::format("Driver {} permute right_join", _driver_sequence);
        RETURN_IF_ERROR(_permute_right_join(chunk_size));
        _advance_join_stage(JoinStage::PostRightJoin);
        break;
    }
    case PostRightJoin:
        break;
    case Finished:
        return nullptr;
    }

    if (ChunkPtr chunk = _output_accumulator.pull()) {
        return chunk;
    }
    while (!_is_curr_probe_chunk_finished()) {
        ChunkPtr chunk = _permute_chunk_for_other_join(chunk_size);
        DCHECK(chunk);
        RETURN_IF_ERROR(_probe_for_other_join(chunk));
        RETURN_IF_ERROR(eval_conjuncts(_conjunct_ctxs, chunk.get(), nullptr));

        RETURN_IF_ERROR(_output_accumulator.push(std::move(chunk)));
        if (ChunkPtr res = _output_accumulator.pull()) {
            return res;
        }

        if (_output_accumulator.reach_limit()) {
            _output_accumulator.finalize();
            return _output_accumulator.pull();
        }
    }
    _output_accumulator.finalize();

    return _output_accumulator.pull();
}

StatusOr<ChunkPtr> NLJoinProbeOperator::_pull_chunk_for_inner_join(size_t chunk_size) {
    if (_join_stage == Finished) {
        return nullptr;
    }
    if (ChunkPtr chunk = _output_accumulator.pull()) {
        return chunk;
    }

    while (!_is_curr_probe_chunk_finished()) {
        ChunkPtr chunk = _permute_chunk_for_inner_join(chunk_size);
        DCHECK(chunk);
        RETURN_IF_ERROR(_probe_for_inner_join(chunk));
        RETURN_IF_ERROR(eval_conjuncts(_conjunct_ctxs, chunk.get(), nullptr));

        RETURN_IF_ERROR(_output_accumulator.push(std::move(chunk)));
        if (ChunkPtr res = _output_accumulator.pull()) {
            return res;
        }

        if (_output_accumulator.reach_limit()) {
            _output_accumulator.finalize();
            return _output_accumulator.pull();
        }
    }
    _output_accumulator.finalize();

    return _output_accumulator.pull();
}

void NLJoinProbeOperator::_init_build_match() const {
    if (_is_right_join() && _self_build_match_flag.size() < _cross_join_context->num_build_rows()) {
        VLOG(3) << "init build_match_flags " << _cross_join_context->num_build_rows();
        _self_build_match_flag.resize(_cross_join_context->num_build_rows(), 0);
    }
}

Status NLJoinProbeOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _probe_chunk = chunk;
    _probe_row_start = 0;
    _probe_row_current = 0;
    _probe_row_matched = false;
    _probe_row_finished = false;
    _move_build_chunk_index(0);

    return Status::OK();
}

void NLJoinProbeOperatorFactory::_init_row_desc() {
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

OperatorPtr NLJoinProbeOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<NLJoinProbeOperator>(this, _id, _plan_node_id, driver_sequence, _join_op,
                                                 _sql_join_conjuncts, _join_conjuncts, _conjunct_ctxs, _col_types,
                                                 _probe_column_count, _cross_join_context);
}

Status NLJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorWithDependencyFactory::prepare(state));

    _init_row_desc();
    RETURN_IF_ERROR(Expr::prepare(_join_conjuncts, state));
    RETURN_IF_ERROR(Expr::open(_join_conjuncts, state));
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));

    return Status::OK();
}

void NLJoinProbeOperatorFactory::close(RuntimeState* state) {
    Expr::close(_join_conjuncts, state);
    Expr::close(_conjunct_ctxs, state);

    OperatorWithDependencyFactory::close(state);
}
} // namespace starrocks::pipeline
