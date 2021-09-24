// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/repeat_node.h"

#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {
RepeatNode::RepeatNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _slot_id_set_list(tnode.repeat_node.slot_id_set_list),
          _all_slot_ids(tnode.repeat_node.all_slot_ids),
          _repeat_id_list(tnode.repeat_node.repeat_id_list),
          _repeat_times_required(_repeat_id_list.size()),
          _repeat_times_last(_repeat_times_required),
          _curr_columns(_all_slot_ids.size()),
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
    _runtime_state = state;
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

Status RepeatNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("get_next for row_batch is not supported");
}

// for new chunk A.
// step 1:
// Extend columns of A, with virtual columns
// for gourping_id and grouping()/grouping_id() columns.
//
// step 2:
// save current chunk of A.
//
// step 3:
// set null columns of A for unneed columns
// and return reulst chunk to parent.
//
// step 4:
// for every rest of the group by
// obtain original chunk from step 2, and
// do step 1 with update columns instead of append columns.
// do step 3.
Status RepeatNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    DCHECK_EQ(_children.size(), 1);

    for (;;) {
        // if _repeat_times_last < _repeat_times_required
        // continue access old chunk.
        if (_repeat_times_last < _repeat_times_required) {
            {
                SCOPED_TIMER(_copy_column_timer);
                Columns columns = _curr_columns;
                // get a suitable chunk.
                _curr_chunk->set_columns(columns);
            }

            {
                SCOPED_TIMER(_update_column_timer);
                // unneed to extend, because columns has been extended at first access.
                // update virtual columns for gourping_id and grouping()/grouping_id() columns.
                for (int i = 0; i < _grouping_list.size(); ++i) {
                    auto grouping_column = (_curr_chunk->num_rows() == config::vector_chunk_size)
                                                   ? _grouping_columns[i][_repeat_times_last]
                                                   : generate_repeat_column(_grouping_list[i][_repeat_times_last],
                                                                            _curr_chunk->num_rows());

                    _curr_chunk->update_column(grouping_column, _tuple_desc->slots()[i]->id());
                }
            }

            {
                SCOPED_TIMER(_update_column_timer);
                // update columns for unneed columns.
                std::vector<SlotId>& null_slot_ids = _null_slot_ids[_repeat_times_last];
                for (auto slot_id : null_slot_ids) {
                    auto null_column = (_curr_chunk->num_rows() == config::vector_chunk_size)
                                               ? _column_null
                                               : generate_null_column(_curr_chunk->num_rows());

                    _curr_chunk->update_column(null_column, slot_id);
                }
            }

            {
                SCOPED_TIMER(_copy_column_timer);
                *chunk = _curr_chunk;
            }

            ++_repeat_times_last;
            break;
        } else {
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
                _curr_chunk = std::move(*chunk);

                {
                    SCOPED_TIMER(_extend_column_timer);
                    // extend virtual columns for gourping_id and grouping()/grouping_id() columns.
                    for (int i = 0; i < _grouping_list.size(); ++i) {
                        auto grouping_column = (_curr_chunk->num_rows() == config::vector_chunk_size)
                                                       ? _grouping_columns[i][_repeat_times_last]
                                                       : generate_repeat_column(_grouping_list[i][_repeat_times_last],
                                                                                _curr_chunk->num_rows());

                        _curr_chunk->append_column(grouping_column, _tuple_desc->slots()[i]->id());
                    }
                }

                {
                    SCOPED_TIMER(_copy_column_timer);
                    // save original exgtended columns.
                    _curr_columns = _curr_chunk->columns();
                }

                {
                    SCOPED_TIMER(_update_column_timer);
                    // update columns for unneed columns.
                    std::vector<SlotId>& null_slot_ids = _null_slot_ids[_repeat_times_last];
                    for (auto slot_id : null_slot_ids) {
                        auto null_column = (_curr_chunk->num_rows() == config::vector_chunk_size)
                                                   ? _column_null
                                                   : generate_null_column(_curr_chunk->num_rows());

                        _curr_chunk->update_column(null_column, slot_id);
                    }
                }

                {
                    SCOPED_TIMER(_copy_column_timer);
                    // get result chunk.
                    *chunk = _curr_chunk;
                }

                ++_repeat_times_last;
                break;
            }
        }
    }

    if ((*chunk) != nullptr) {
        _num_rows_returned += (*chunk)->num_rows();
    }
    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

Status RepeatNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

} // namespace starrocks::vectorized
