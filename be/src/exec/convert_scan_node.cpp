// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/convert_scan_node.h"

#include "column/column_helper.h"
#include "exprs/slot_ref.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30
namespace starrocks {
Status ConvertScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::prepare(state));

    _convert_row_batch =
            std::make_unique<RowBatch>(row_desc(), config::vector_chunk_size, state->instance_mem_tracker());

    for (auto tuple_desc : _convert_row_batch->row_desc().tuple_descriptors()) {
        for (auto desc : tuple_desc->slots()) {
            _slot_descs.push_back(desc);

            SlotRef slot(desc);
            slot.prepare(desc, _convert_row_batch->row_desc());
            _slots.push_back(slot);
        }
    }

    return Status::OK();
}

Status ConvertScanNode::close(RuntimeState* state) {
    _convert_row_batch.reset();
    RETURN_IF_ERROR(ExecNode::close(state));
    return Status::OK();
}

Status ConvertScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_CANCELLED(state);

    _convert_row_batch->reset();

    // RETURN_IF_ERROR(get_next(state, _convert_row_batch.get(), eos));

    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (_convert_row_batch->num_rows() > 0) {
        *eos = false;
        ChunkPtr result_chunk = std::make_shared<vectorized::Chunk>();
        RETURN_IF_ERROR(convert_rowbatch_to_chunk(*_convert_row_batch, result_chunk.get()));
        *chunk = std::move(result_chunk);
    } else {
        *eos = true;
        *chunk = nullptr;
    }

    return Status::OK();
}

template <PrimitiveType SlotType>
void ConvertScanNode::_fill_data_column_with_slot(vectorized::Column* data_column, void* slot) {
    using ColumnType = typename vectorized::RunTimeTypeTraits<SlotType>::ColumnType;
    using ValueType = typename vectorized::RunTimeTypeTraits<SlotType>::CppType;

    ColumnType* result = down_cast<ColumnType*>(data_column);
    if constexpr (vectorized::IsDate<ValueType>) {
        DateTimeValue* date_time_value = (DateTimeValue*)slot;
        vectorized::DateValue date_value = vectorized::DateValue::create(
                date_time_value->year(), date_time_value->month(), date_time_value->day());
        result->append(date_value);
    } else if constexpr (vectorized::IsTimestamp<ValueType>) {
        DateTimeValue* date_time_value = (DateTimeValue*)slot;
        vectorized::TimestampValue timestamp_value = vectorized::TimestampValue::create(
                date_time_value->year(), date_time_value->month(), date_time_value->day(), date_time_value->hour(),
                date_time_value->minute(), date_time_value->second());
        result->append(timestamp_value);
    } else {
        result->append(*(ValueType*)slot);
    }
}

template <PrimitiveType SlotType>
void ConvertScanNode::_fill_column_with_slot(const RowBatch& batch, SlotRef* slot, vectorized::Column* result) {
    if (result->is_nullable()) {
        vectorized::NullableColumn* nullable_column = down_cast<vectorized::NullableColumn*>(result);
        vectorized::NullData& null_data = nullable_column->null_column_data();
        vectorized::Column* data_column = nullable_column->data_column().get();
        for (size_t i = 0; i < batch.num_rows(); ++i) {
            if (slot->is_null_bit_set(batch.get_row(i))) {
                nullable_column->append_nulls(1);
                continue;
            }
            null_data.push_back(0);

            auto cell_ptr = slot->get_slot(batch.get_row(i));
            _fill_data_column_with_slot<SlotType>(data_column, cell_ptr);
        }
    } else {
        for (size_t i = 0; i < batch.num_rows(); ++i) {
            auto cell_ptr = slot->get_slot(batch.get_row(i));
            _fill_data_column_with_slot<SlotType>(result, cell_ptr);
        }
    }
}

Status ConvertScanNode::convert_rowbatch_to_chunk(const RowBatch& batch, vectorized::Chunk* result) {
    for (size_t i = 0; i < _slot_descs.size(); ++i) {
        auto column = vectorized::ColumnHelper::create_column(_slot_descs[i]->type(), _slot_descs[i]->is_nullable());
        column->reserve(batch.num_rows());
        switch (_slot_descs[i]->type().type) {
        case TYPE_BOOLEAN: {
            _fill_column_with_slot<TYPE_BOOLEAN>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_TINYINT: {
            _fill_column_with_slot<TYPE_TINYINT>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_SMALLINT: {
            _fill_column_with_slot<TYPE_SMALLINT>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_INT: {
            _fill_column_with_slot<TYPE_INT>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_BIGINT: {
            _fill_column_with_slot<TYPE_BIGINT>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_LARGEINT: {
            _fill_column_with_slot<TYPE_LARGEINT>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_FLOAT: {
            _fill_column_with_slot<TYPE_FLOAT>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_DOUBLE: {
            _fill_column_with_slot<TYPE_DOUBLE>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_DATE: {
            _fill_column_with_slot<TYPE_DATE>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_DATETIME: {
            _fill_column_with_slot<TYPE_DATETIME>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_CHAR: {
            _fill_column_with_slot<TYPE_CHAR>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_VARCHAR: {
            _fill_column_with_slot<TYPE_VARCHAR>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_DECIMALV2: {
            _fill_column_with_slot<TYPE_DECIMALV2>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_DECIMAL32: {
            _fill_column_with_slot<TYPE_DECIMAL32>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_DECIMAL64: {
            _fill_column_with_slot<TYPE_DECIMAL64>(batch, &_slots[i], column.get());
            break;
        }
        case TYPE_DECIMAL128: {
            _fill_column_with_slot<TYPE_DECIMAL128>(batch, &_slots[i], column.get());
            break;
        }
        default: {
            LOG(WARNING) << "Unsupport column type " << _slot_descs[i]->col_name();
            return Status::InternalError("Unsupport column type");
            break;
        }
        }
        result->append_column(std::move(column), _slot_descs[i]->id());
    }
    return Status::OK();
}
} // namespace starrocks
