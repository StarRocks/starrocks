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

#include "exec/repeat_node.h"

#include "exec/pipeline/aggregate/repeat/repeat_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks {
RepeatNode::RepeatNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _slot_id_set_list(tnode.repeat_node.slot_id_set_list),
          _all_slot_ids(tnode.repeat_node.all_slot_ids),
          _repeat_id_list(tnode.repeat_node.repeat_id_list),
          _repeat_times_required(_repeat_id_list.size()),
          _repeat_times_last(_repeat_times_required),
          _grouping_list(tnode.repeat_node.grouping_list),
          _output_tuple_id(tnode.repeat_node.output_tuple_id),
          _tuple_desc(descs.get_tuple_descriptor(_output_tuple_id)) {
    // initial for null slots;
    for (int i = 0; i < _repeat_times_required; ++i) {
        std::set<SlotId>& repeat_ids = _slot_id_set_list[i];
        std::vector<SlotId> null_slots;
        for (auto slot_id : _all_slot_ids) {
            if (repeat_ids.find(slot_id) == repeat_ids.end()) {
                null_slots.push_back(slot_id);
            }
        }
        _null_slot_ids.push_back(null_slots);
    }

    // initial for _columns_null of 4096 rows;
    _column_null = generate_null_column(config::vector_chunk_size);

    // initial for _grouping_columns;
    for (auto& group : _grouping_list) {
        std::vector<ColumnPtr> columns;
        columns.reserve(group.size());
        for (auto slot_id : group) {
            columns.push_back(generate_repeat_column(slot_id, config::vector_chunk_size));
        }
        _grouping_columns.push_back(columns);
    }

    DCHECK_EQ(_grouping_list.size(), _tuple_desc->slots().size());
    DCHECK_EQ(_grouping_list[0].size(), _repeat_id_list.size());
    DCHECK_EQ(_slot_id_set_list.size(), _repeat_id_list.size());
}

Status RepeatNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    if (_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    _extend_column_timer = ADD_TIMER(runtime_profile(), "ExtendColumnTime");
    _copy_column_timer = ADD_TIMER(runtime_profile(), "CopyColumnTime");
    _update_column_timer = ADD_TIMER(runtime_profile(), "UpdateColumnTime");

    return Status::OK();
}

Status RepeatNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));
    return Status::OK();
}

/*
 * for new chunk A.
 * It used as first time and non-first time:
 *
 * first time(_repeat_times_last == 0):
 * step 1:
 * move A as curr_chunk
 * copy curr_chunk as _curr_chunk.
 *
 * step 2:
 * Extend multiple virtual columns for curr_chunk,
 * virtual columns is consist of gourping_id and grouping()/grouping_id() columns.
 *
 * step 3:
 * update columns of curr_chunk for unneed columns,
 * and return reulst chunk to parent.
 *
 *
 * non-first time, it measn _repeat_times_last in [1, _repeat_times_required):
 * step 1:
 * copy _curr_chunk as curr_chunk.
 *
 * step 2/step 3 is the same as first time.
 *
 */
Status RepeatNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    DCHECK_EQ(_children.size(), 1);

    for (;;) {
        // if _repeat_times_last < _repeat_times_required
        // continue access old chunk.
        if (_repeat_times_last < _repeat_times_required) {
            ChunkPtr curr_chunk = _curr_chunk->clone_empty(_curr_chunk->num_rows());
            {
                SCOPED_TIMER(_copy_column_timer);
                curr_chunk->append_safe(*_curr_chunk, 0, _curr_chunk->num_rows());
            }

            extend_and_update_columns(&curr_chunk, chunk);

            ++_repeat_times_last;
            break;
        } else {
            _curr_chunk.reset();
            // get a new chunk.
            RETURN_IF_ERROR(_children[0]->get_next(state, chunk, eos));

            // check for over.
            if (*eos || (*chunk) == nullptr) {
                break;
            } else if ((*chunk)->num_rows() == 0) {
                continue;
            } else {
                // got a new chunk.
                _repeat_times_last = 0;
                auto curr_chunk = std::move(*chunk);

                {
                    SCOPED_TIMER(_copy_column_timer);
                    // Used for next time.
                    _curr_chunk = curr_chunk->clone_empty(curr_chunk->num_rows());
                    _curr_chunk->append_safe(*curr_chunk, 0, curr_chunk->num_rows());
                }

                extend_and_update_columns(&curr_chunk, chunk);

                ++_repeat_times_last;
                break;
            }
        }
    }

    if ((*chunk) != nullptr) {
        ExecNode::eval_join_runtime_filters(chunk);
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get()));
        _num_rows_returned += (*chunk)->num_rows();
    }
    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

void RepeatNode::extend_and_update_columns(ChunkPtr* curr_chunk, ChunkPtr* chunk) {
    {
        SCOPED_TIMER(_extend_column_timer);
        // extend virtual columns for gourping_id and grouping()/grouping_id() columns.
        for (int i = 0; i < _grouping_list.size(); ++i) {
            auto grouping_column =
                    generate_repeat_column(_grouping_list[i][_repeat_times_last], (*curr_chunk)->num_rows());

            (*curr_chunk)->append_column(grouping_column, _tuple_desc->slots()[i]->id());
        }
    }

    {
        SCOPED_TIMER(_update_column_timer);
        // update columns for unneed columns.
        std::vector<SlotId>& null_slot_ids = _null_slot_ids[_repeat_times_last];
        for (auto slot_id : null_slot_ids) {
            auto null_column = generate_null_column((*curr_chunk)->num_rows());

            (*curr_chunk)->update_column(null_column, slot_id);
        }
    }

    {
        SCOPED_TIMER(_copy_column_timer);
        // get result chunk.
        *chunk = *curr_chunk;
    }
}

void RepeatNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    ExecNode::close(state);
}

std::vector<std::shared_ptr<pipeline::OperatorFactory> > RepeatNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    OpFactories operators = _children[0]->decompose_to_pipeline(context);

    operators.emplace_back(std::make_shared<RepeatOperatorFactory>(
            context->next_operator_id(), id(), std::move(_slot_id_set_list), std::move(_all_slot_ids),
            std::move(_null_slot_ids), std::move(_repeat_id_list), _repeat_times_required, _repeat_times_last,
            std::move(_column_null), std::move(_grouping_columns), std::move(_grouping_list), _output_tuple_id,
            _tuple_desc, std::move(_conjunct_ctxs)));
    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(operators.back().get(), context, rc_rf_probe_collector);
    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return operators;
}

} // namespace starrocks
