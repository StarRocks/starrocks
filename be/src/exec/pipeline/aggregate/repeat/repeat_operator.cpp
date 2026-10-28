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

#include "exec/pipeline/aggregate/repeat/repeat_operator.h"

#include "column/chunk.h"
#include "exec_primitive/exec_node.h"
#include "exprs/expr_executor.h"
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

StatusOr<ChunkPtr> RepeatOperator::pull_chunk(RuntimeState* state) {
    // The row count comes from the source chunk. The copy cannot be asked for it: a chunk reports
    // the size of its column 0, and this grouping set may be the one that blanks that column out.
    const size_t num_rows = _curr_chunk->num_rows();
    ChunkPtr curr_chunk = clone_curr_chunk(num_rows);
    append_grouping_columns(curr_chunk.get(), num_rows);
    RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_conjunct_ctxs, curr_chunk.get()));
    return curr_chunk;
}

ChunkPtr RepeatOperator::clone_curr_chunk(size_t num_rows) {
    // The columns this grouping set blanks out are not copied, they are built: a column the caller
    // is about to overwrite with a const NULL column does not need its data deep-copied first. That
    // discarded copy used to be paid once per grouping set for every chunk, which on a wide table
    // under GROUPING SETS / ROLLUP / CUBE is the bulk of this operator's work.
    //
    // The columns that do survive are still copied: eval_conjuncts_and_in_filters() filters the
    // result in place through as_mutable_raw_ptr(), which would write through to _curr_chunk if the
    // two shared a column, and _curr_chunk has to stay intact for the remaining repeats.
    ChunkPtr chunk = _curr_chunk->clone_empty(0);

    const size_t num_columns = _curr_chunk->num_columns();
    _is_nulled_column.assign(num_columns, 0);
    for (auto slot_id : _null_slot_ids[_repeat_times_last]) {
        if (_curr_chunk->is_slot_exist(slot_id)) {
            _is_nulled_column[_curr_chunk->get_index_by_slot_id(slot_id)] = 1;
        }
    }

    for (size_t i = 0; i < num_columns; ++i) {
        ColumnPtr& column = chunk->get_column_by_index(i);
        if (_is_nulled_column[i]) {
            // clone_empty(0) already left an empty column of the right type here, and its type is
            // all generate_null_column() reads. Filling it in now rather than after the grouping
            // columns are appended also keeps the chunk valid throughout: Chunk::append_column()
            // checks that every non-constant column has the chunk's row count.
            column = generate_null_column(column, num_rows);
        } else {
            column = _curr_chunk->get_column_by_index(i)->clone();
        }
    }

    // Bookkeeping Chunk::clone_unique() used to carry for us; owner_info in particular drives the
    // query cache and the last-chunk marker.
    chunk->owner_info() = _curr_chunk->owner_info();
    if (_curr_chunk->has_extra_data()) {
        chunk->set_extra_data(_curr_chunk->get_extra_data()->clone());
    }
    return chunk;
}

void RepeatOperator::append_grouping_columns(Chunk* curr_chunk, size_t num_rows) {
    // extend virtual columns for gourping_id and grouping()/grouping_id() columns.
    for (int i = 0; i < _grouping_list.size(); ++i) {
        auto grouping_column = generate_repeat_column(_grouping_list[i][_repeat_times_last], num_rows);

        curr_chunk->append_column(std::move(grouping_column), _tuple_desc->slots()[i]->id());
    }
    ++_repeat_times_last;
}

Status RepeatOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    // get new chunk.
    _curr_chunk = chunk;

    // set _repeat_times_last to 0 drive to use this new chunk(_curr_chunk).
    _repeat_times_last = 0;
    return Status::OK();
}

Status RepeatOperator::reset_state(starrocks::RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _curr_chunk.reset();
    _repeat_times_last = _repeat_times_required;
    _is_finished = false;
    return Status::OK();
}

Status RepeatOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExprExecutor::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(ExprExecutor::open(_conjunct_ctxs, state));
    return Status::OK();
}

} // namespace starrocks::pipeline
