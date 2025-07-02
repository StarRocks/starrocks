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

StatusOr<ChunkPtr> RepeatOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr curr_chunk = _curr_chunk->clone_unique();
    extend_and_update_columns(&curr_chunk);
    RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_conjunct_ctxs, curr_chunk.get()));
    return curr_chunk;
}

void RepeatOperator::extend_and_update_columns(ChunkPtr* curr_chunk) {
    const size_t num_rows = (*curr_chunk)->num_rows();
    // extend virtual columns for gourping_id and grouping()/grouping_id() columns.
    for (int i = 0; i < _grouping_list.size(); ++i) {
        auto grouping_column = generate_repeat_column(_grouping_list[i][_repeat_times_last], num_rows);

        (*curr_chunk)->append_column(std::move(grouping_column), _tuple_desc->slots()[i]->id());
    }

    // update columns for unneed columns.
    const std::vector<SlotId>& null_slot_ids = _null_slot_ids[_repeat_times_last];
    for (auto slot_id : null_slot_ids) {
        auto& cur_column = (*curr_chunk)->get_column_by_slot_id(slot_id);
        auto null_column = generate_null_column(cur_column, num_rows);
        (*curr_chunk)->update_column(std::move(null_column), slot_id);
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
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    return Status::OK();
}

} // namespace starrocks::pipeline
