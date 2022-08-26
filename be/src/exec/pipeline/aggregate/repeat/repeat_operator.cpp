// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/aggregate/repeat/repeat_operator.h"

#include "column/chunk.h"
#include "exec/exec_node.h"
#include "runtime/descriptors.h"

namespace starrocks::pipeline {

Status RepeatOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

bool RepeatOperator::is_finished() const {
    // _repeat_times_last >= _repeat_times_required means there has no output.
    return _is_finished && _repeat_times_last >= _repeat_times_required;
}

Status RepeatOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

bool RepeatOperator::has_output() const {
    return _repeat_times_last < _repeat_times_required;
}

StatusOr<vectorized::ChunkPtr> RepeatOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr curr_chunk = _curr_chunk->clone_empty(_curr_chunk->num_rows());
    curr_chunk->append_safe(*_curr_chunk, 0, _curr_chunk->num_rows());
    extend_and_update_columns(&curr_chunk);
    RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_conjunct_ctxs, curr_chunk.get()));
    return curr_chunk;
}

void RepeatOperator::extend_and_update_columns(ChunkPtr* curr_chunk) {
    // extend virtual columns for gourping_id and grouping()/grouping_id() columns.
    for (int i = 0; i < _grouping_list.size(); ++i) {
        auto grouping_column = generate_repeat_column(_grouping_list[i][_repeat_times_last], (*curr_chunk)->num_rows());

        (*curr_chunk)->append_column(grouping_column, _tuple_desc->slots()[i]->id());
    }

    // update columns for unneed columns.
    const std::vector<SlotId>& null_slot_ids = _null_slot_ids[_repeat_times_last];
    for (auto slot_id : null_slot_ids) {
        auto null_column = ColumnHelper::create_const_null_column((*curr_chunk)->num_rows());

        (*curr_chunk)->update_column(null_column, slot_id);
    }
    ++_repeat_times_last;
}

Status RepeatOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    // get new chunk.
    _curr_chunk = chunk;

    // set _repeat_times_last to 0 drive to use this new chunk(_curr_chunk).
    _repeat_times_last = 0;
    return Status::OK();
}
} // namespace starrocks::pipeline
