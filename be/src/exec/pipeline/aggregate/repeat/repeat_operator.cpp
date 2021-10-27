// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/aggregate/repeat/repeat_operator.h"

#include "column/chunk.h"
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

void RepeatOperator::finish(RuntimeState* state) {
    if (_is_finished) {
        return;
    }
    _is_finished = true;
}

bool RepeatOperator::has_output() const {
    return _repeat_times_last < _repeat_times_required;
}

StatusOr<vectorized::ChunkPtr> RepeatOperator::pull_chunk(RuntimeState* state) {
    if (_repeat_times_last > 0) {
        Columns columns = _curr_columns;
        // get a suitable chunk.
        _curr_chunk->set_columns(columns);

        // unneed to extend, because columns has been extended at first access.
        // update virtual columns for grouping_id and grouping()/grouping_id() columns.
        for (int i = 0; i < _grouping_list.size(); ++i) {
            // _grouping_columns needs to be referenced more than once, so it cannot be moved.
            auto grouping_column =
                    (_curr_chunk->num_rows() == config::vector_chunk_size)
                            ? _grouping_columns[i][_repeat_times_last]
                            : generate_repeat_column(_grouping_list[i][_repeat_times_last], _curr_chunk->num_rows());

            _curr_chunk->update_column(grouping_column, _tuple_desc->slots()[i]->id());
        }

        // update columns for unneed columns.
        const std::vector<SlotId>& null_slot_ids = _null_slot_ids[_repeat_times_last];
        for (auto slot_id : null_slot_ids) {
            // _column_null needs to be referenced more than once, so it cannot be moved.
            auto null_column = (_curr_chunk->num_rows() == config::vector_chunk_size)
                                       ? _column_null
                                       : ColumnHelper::create_const_null_column(_curr_chunk->num_rows());

            _curr_chunk->update_column(null_column, slot_id);
        }

        // tranform to the next.
        ++_repeat_times_last;
        return _curr_chunk;
    } else {
        // extend virtual columns for grouping_id and grouping()/grouping_id() columns.
        for (int i = 0; i < _grouping_list.size(); ++i) {
            // _grouping_columns needs to be referenced more than once, so it cannot be moved.
            auto grouping_column =
                    (_curr_chunk->num_rows() == config::vector_chunk_size)
                            ? _grouping_columns[i][_repeat_times_last]
                            : generate_repeat_column(_grouping_list[i][_repeat_times_last], _curr_chunk->num_rows());

            _curr_chunk->append_column(grouping_column, _tuple_desc->slots()[i]->id());
        }

        // save original exgtended columns.
        _curr_columns = _curr_chunk->columns();

        // update columns for unneed columns.
        const std::vector<SlotId>& null_slot_ids = _null_slot_ids[_repeat_times_last];
        for (auto slot_id : null_slot_ids) {
            // _column_null needs to be referenced more than once, so it cannot be moved.
            auto null_column = (_curr_chunk->num_rows() == config::vector_chunk_size)
                                       ? _column_null
                                       : ColumnHelper::ColumnHelper::create_const_null_column(_curr_chunk->num_rows());

            _curr_chunk->update_column(null_column, slot_id);
        }

        // tranform to the next.
        ++_repeat_times_last;
        return _curr_chunk;
    }
}

Status RepeatOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    // get new chunk.
    _curr_chunk = chunk;

    // set _repeat_times_last to 0 drive to use this new chunk(_curr_chunk).
    _repeat_times_last = 0;
    return Status::OK();
}
} // namespace starrocks::pipeline