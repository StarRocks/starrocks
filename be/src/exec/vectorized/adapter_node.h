// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/column_viewer.h"
#include "column/vectorized_fwd.h"
#include "exec/exec_node.h"
#include "runtime/row_batch.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {
class AdapterNode final : public ExecNode {
public:
    AdapterNode(ObjectPool* pool, const TPlanNode& node, const DescriptorTbl& desc);

    ~AdapterNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    Status reset(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

private:
    void _fill_slot(RowBatch* row_batch, int tuple_id, Tuple* tuple, const SlotDescriptor* slot_desc);

    void _fill_slice_tuple(RowBatch* row_batch, int tuple_id, Tuple* tuple, const SlotDescriptor* slot_desc);

    void _fill_fix_size_tuple(RowBatch* row_batch, int tuple_id, Tuple* tuple, const SlotDescriptor* slot_desc);

    void _fill_timestamp_tuple(RowBatch* row_batch, int tuple_id, Tuple* tuple, const SlotDescriptor* slot_desc);

    void _fill_date_tuple(RowBatch* row_batch, int tuple_id, Tuple* tuple, const SlotDescriptor* slot_desc);

    template <PrimitiveType T>
    void _fill_object_tuple(RowBatch* row_batch, int tuple_id, Tuple* tuple, const SlotDescriptor* slot_desc);

    starrocks::Tuple* _get_next_tuple(starrocks::Tuple* tuple, int byte_size);

private:
    ChunkPtr _chunk;
    int _tuple_row_byte_size = 0;
    // ensure the string memory don't early free
    std::vector<ColumnPtr> _string_columns;

    RuntimeProfile::Counter* _transfer_timer = nullptr;
    RuntimeProfile::Counter* _next_batch_timer = nullptr;
    RuntimeProfile::Counter* _tuple_init_timer = nullptr;
};

template <PrimitiveType T>
void AdapterNode::_fill_object_tuple(starrocks::RowBatch* row_batch, int tuple_id, starrocks::Tuple* start_tuple,
                                     const starrocks::SlotDescriptor* slot_desc) {
    ColumnPtr column = _chunk->get_column_by_slot_id(slot_desc->id());
    _string_columns.emplace_back(column);

    Tuple* tuple = start_tuple;

    ColumnViewer<T> viewer(column);
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
            auto slot = tuple->get_string_slot(slot_desc->tuple_offset());

            // only keep pointer reference
            slot->ptr = reinterpret_cast<char*>(value);
            slot->len = 0;
        }

        tuple = _get_next_tuple(tuple, _tuple_row_byte_size);
    }
}

} // namespace starrocks::vectorized
