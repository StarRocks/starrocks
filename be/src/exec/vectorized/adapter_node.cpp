// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/adapter_node.h"

#include <memory>

#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"

namespace starrocks::vectorized {

AdapterNode::AdapterNode(starrocks::ObjectPool* pool, const starrocks::TPlanNode& node,
                         const starrocks::DescriptorTbl& desc)
        : ExecNode(pool, node, desc) {}

AdapterNode::~AdapterNode() = default;

Status AdapterNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

Status AdapterNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _next_batch_timer = ADD_TIMER(runtime_profile(), "NextBatchTime");
    _transfer_timer = ADD_TIMER(runtime_profile(), "TransferTime");
    _tuple_init_timer = ADD_TIMER(runtime_profile(), "TupleInitTime");

    for (auto tuple_desc : row_desc().tuple_descriptors()) {
        _tuple_row_byte_size += tuple_desc->byte_size();
    }
    _chunk = std::make_shared<Chunk>();
    return Status::OK();
}

Status AdapterNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    return _children[0]->open(state);
}

Status AdapterNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    {
        SCOPED_TIMER(_next_batch_timer);
        do {
            RETURN_IF_ERROR(_children[0]->get_next(state, &_chunk, eos));
        } while (!*eos && _chunk->is_empty());
    }

    if (*eos) {
        return Status::OK();
    }

    uint8_t* tuple_buf = nullptr;
    {
        SCOPED_TIMER(_tuple_init_timer);
        tuple_buf = row_batch->tuple_data_pool()->allocate(state->batch_size() * _tuple_row_byte_size);
        if (UNLIKELY(tuple_buf == nullptr)) {
            return Status::InternalError("Mem usage has exceed the limit of BE");
        }
        bzero(tuple_buf, state->batch_size() * _tuple_row_byte_size);

        size_t num_rows = _chunk->num_rows();
        _num_rows_returned += num_rows;
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        row_batch->commit_rows(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            TupleRow* row = row_batch->get_row(i);
            Tuple* first_tuple = _get_next_tuple(reinterpret_cast<Tuple*>(tuple_buf), _tuple_row_byte_size * i);
            size_t first_offset = 0;
            for (int tuple_id = 0; tuple_id < row_desc().tuple_descriptors().size(); ++tuple_id) {
                auto tuple_desc = row_desc().tuple_descriptors()[tuple_id];
                row->set_tuple(tuple_id, _get_next_tuple(first_tuple, first_offset));
                first_offset += tuple_desc->byte_size();
            }
        }
    }

    {
        SCOPED_TIMER(_transfer_timer);

        TupleRow* row = row_batch->get_row(0);
        for (int tuple_id = 0; tuple_id < row_desc().tuple_descriptors().size(); ++tuple_id) {
            auto tuple_desc = row_desc().tuple_descriptors()[tuple_id];
            starrocks::Tuple* tuple = row->get_tuple(tuple_id);
            for (auto slot : tuple_desc->slots()) {
                if (!slot->is_materialized()) {
                    continue;
                }
                _fill_slot(row_batch, tuple_id, tuple, slot);
            }
        }
    }

    if (VLOG_ROW_IS_ON) {
        VLOG_ROW << "AdapterNode: #rows=" << row_batch->num_rows() << " desc=" << row_desc().debug_string();

        for (int i = 0; i < row_batch->num_rows(); ++i) {
            TupleRow* row = row_batch->get_row(i);
            VLOG_ROW << row->to_string(row_desc());
        }
    }

    return Status::OK();
}

starrocks::Tuple* AdapterNode::_get_next_tuple(starrocks::Tuple* tuple, int byte_size) {
    char* new_tuple = reinterpret_cast<char*>(tuple);
    new_tuple += byte_size;
    return reinterpret_cast<Tuple*>(new_tuple);
}

void AdapterNode::_fill_slot(RowBatch* row_batch, int tuple_id, starrocks::Tuple* tuple,
                             const SlotDescriptor* slot_desc) {
    switch (slot_desc->type().type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        _fill_slice_tuple(row_batch, tuple_id, tuple, slot_desc);
        break;
    }
    case TYPE_DATETIME: {
        _fill_timestamp_tuple(row_batch, tuple_id, tuple, slot_desc);
        break;
    }
    case TYPE_DATE: {
        _fill_date_tuple(row_batch, tuple_id, tuple, slot_desc);
        break;
    }
    case TYPE_HLL: {
        _fill_object_tuple<TYPE_HLL>(row_batch, tuple_id, tuple, slot_desc);
        break;
    }
    case TYPE_OBJECT: {
        _fill_object_tuple<TYPE_OBJECT>(row_batch, tuple_id, tuple, slot_desc);
        break;
    }
    case TYPE_PERCENTILE: {
        _fill_object_tuple<TYPE_PERCENTILE>(row_batch, tuple_id, tuple, slot_desc);
        break;
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMALV2: {
        _fill_fix_size_tuple(row_batch, tuple_id, tuple, slot_desc);
        break;
    }
    default: {
        LOG(WARNING) << "Vectorized engine not support the type: " << slot_desc->type().type;
        DCHECK(false);
    }
    }
}

void AdapterNode::_fill_timestamp_tuple(RowBatch* row_batch, int tuple_id, starrocks::Tuple* start_tuple,
                                        const starrocks::SlotDescriptor* slot_desc) {
    ColumnPtr column = _chunk->get_column_by_slot_id(slot_desc->id());
    Tuple* tuple = start_tuple;

    ColumnViewer<TYPE_DATETIME> viewer(column);
    for (int row = 0; row < _chunk->num_rows(); ++row) {
        if (viewer.is_null(row)) {
            if (slot_desc->is_nullable()) {
                tuple->set_null(slot_desc->null_indicator_offset());
            } else {
                TupleRow* tuple_row = row_batch->get_row(row);
                tuple_row->set_tuple(tuple_id, nullptr);
            }
        } else {
            auto value = viewer.value(row);
            auto* slot = tuple->get_datetime_slot(slot_desc->tuple_offset());

            int year, month, day, hour, minute, second, usec;
            value.to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);

            *slot = DateTimeValue(TIME_DATETIME, year, month, day, hour, minute, second, usec);
        }

        tuple = _get_next_tuple(tuple, _tuple_row_byte_size);
    }
}

void AdapterNode::_fill_date_tuple(RowBatch* row_batch, int tuple_id, starrocks::Tuple* start_tuple,
                                   const starrocks::SlotDescriptor* slot_desc) {
    ColumnPtr column = _chunk->get_column_by_slot_id(slot_desc->id());
    Tuple* tuple = start_tuple;

    ColumnViewer<TYPE_DATE> viewer(column);
    for (int row = 0; row < _chunk->num_rows(); ++row) {
        if (viewer.is_null(row)) {
            if (slot_desc->is_nullable()) {
                tuple->set_null(slot_desc->null_indicator_offset());
            } else {
                TupleRow* tuple_row = row_batch->get_row(row);
                tuple_row->set_tuple(tuple_id, nullptr);
            }
        } else {
            auto value = viewer.value(row);
            auto* slot = tuple->get_datetime_slot(slot_desc->tuple_offset());

            int year, month, day;
            value.to_date(&year, &month, &day);

            *slot = DateTimeValue(TIME_DATE, year, month, day, 0, 0, 0, 0);
        }

        tuple = _get_next_tuple(tuple, _tuple_row_byte_size);
    }
}

void AdapterNode::_fill_slice_tuple(RowBatch* row_batch, int tuple_id, starrocks::Tuple* start_tuple,
                                    const starrocks::SlotDescriptor* slot_desc) {
    ColumnPtr column = _chunk->get_column_by_slot_id(slot_desc->id());
    _string_columns.emplace_back(column);

    Tuple* tuple = start_tuple;

    int row_idx = 0;
    while (row_idx < _chunk->num_rows()) {
        if (column->only_null() || column->is_null(row_idx)) {
            if (slot_desc->is_nullable()) {
                tuple->set_null(slot_desc->null_indicator_offset());
            } else {
                TupleRow* tuple_row = row_batch->get_row(row_idx);
                tuple_row->set_tuple(tuple_id, nullptr);
            }
        } else {
            const Slice* slice;
            if (column->is_constant()) {
                slice = reinterpret_cast<const Slice*>(column->raw_data());
            } else {
                slice = reinterpret_cast<const Slice*>(column->raw_data()) + row_idx;
            }

            auto slot = tuple->get_string_slot(slot_desc->tuple_offset());
            slot->ptr = slice->data;
            slot->len = slice->size;
        }

        tuple = _get_next_tuple(tuple, _tuple_row_byte_size);
        ++row_idx;
    }
}

void AdapterNode::_fill_fix_size_tuple(RowBatch* row_batch, int tuple_id, Tuple* start_tuple,
                                       const SlotDescriptor* slot_desc) {
    ColumnPtr column = _chunk->get_column_by_slot_id(slot_desc->id());
    size_t type_size = slot_desc->type().get_byte_size();
    size_t type_step = type_size;

    Tuple* tuple = start_tuple;

    if (column->is_constant()) {
        type_step = 0;
    }

    int row_idx = 0;
    while (row_idx < _chunk->num_rows()) {
        if (column->only_null() || column->is_null(row_idx)) {
            if (slot_desc->is_nullable()) {
                tuple->set_null(slot_desc->null_indicator_offset());
            } else {
                TupleRow* tuple_row = row_batch->get_row(row_idx);
                tuple_row->set_tuple(tuple_id, nullptr);
            }
        } else {
            const uint8_t* ptr = column->raw_data() + row_idx * type_step;
            void* slot = tuple->get_slot(slot_desc->tuple_offset());

            memory_copy(slot, ptr, type_size);
        }

        tuple = _get_next_tuple(tuple, _tuple_row_byte_size);
        ++row_idx;
    }
}

Status AdapterNode::reset(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::reset(state));
    _chunk = std::make_shared<Chunk>();
    return Status::OK();
}

Status AdapterNode::close(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::close(state));
    _chunk = std::make_shared<Chunk>();
    return Status::OK();
}

} // namespace starrocks::vectorized
