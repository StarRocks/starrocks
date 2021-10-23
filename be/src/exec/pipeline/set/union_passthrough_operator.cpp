// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/set/union_passthrough_operator.h"

#include "column/column_helper.h"
#include "column/nullable_column.h"

namespace starrocks {
namespace pipeline {
Status UnionPassthroughOperator::push_chunk(RuntimeState* state, const ChunkPtr& src_chunk) {
    DCHECK_EQ(_dst_chunk, nullptr);

    _dst_chunk = std::make_shared<vectorized::Chunk>();

    if (_dst2src_slot_map != nullptr) {
        for (auto* dst_slot : _dst_slots) {
            auto& src_slot_item = (*_dst2src_slot_map)[dst_slot->id()];
            ColumnPtr& src_column = src_chunk->get_column_by_slot_id(src_slot_item.slot_id);
            // If there are multiple dest slots mapping to the same src slot id,
            // we should clone the src column instead of directly moving the src column.
            if (src_slot_item.ref_count > 1) {
                _clone_column(_dst_chunk, src_column, dst_slot, src_chunk->num_rows());
            } else {
                _move_column(_dst_chunk, src_column, dst_slot, src_chunk->num_rows());
            }
        }
    } else {
        // For backward compatibility, the indexes of the src and dst slots are one-to-one correspondence.
        // TODO: when StarRocks 2.0 release, we could remove this branch.
        size_t i = 0;
        // When passthrough, the child tuple size must be 1;
        for (auto* src_slot : _src_slots) {
            auto* dst_slot = _dst_slots[i++];
            ColumnPtr& src_column = src_chunk->get_column_by_slot_id(src_slot->id());
            _move_column(_dst_chunk, src_column, dst_slot, src_chunk->num_rows());
        }
    }

    DCHECK_CHUNK(_dst_chunk);

    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> UnionPassthroughOperator::pull_chunk(RuntimeState* state) {
    return std::move(_dst_chunk);
}

void UnionPassthroughOperator::_clone_column(ChunkPtr& dst_chunk, const ColumnPtr& src_column,
                                             const SlotDescriptor* dst_slot, size_t row_count) {
    if (src_column->is_nullable() || !dst_slot->is_nullable()) {
        dst_chunk->append_column(src_column->clone_shared(), dst_slot->id());
    } else {
        // If the dst slot is nullable and the src slot isn't nullable, we need insert null mask to column.
        ColumnPtr nullable_column = vectorized::NullableColumn::create(src_column->clone_shared(),
                                                                       vectorized::NullColumn::create(row_count, 0));
        dst_chunk->append_column(nullable_column, dst_slot->id());
    }
}

void UnionPassthroughOperator::_move_column(ChunkPtr& dst_chunk, const ColumnPtr& src_column,
                                            const SlotDescriptor* dst_slot, size_t row_count) {
    if (src_column->is_nullable() || !dst_slot->is_nullable()) {
        dst_chunk->append_column(src_column, dst_slot->id());
    } else {
        // If the dst slot is nullable and the src slot isn't nullable, we need insert null mask to column.
        ColumnPtr nullable_column =
                vectorized::NullableColumn::create(src_column, vectorized::NullColumn::create(row_count, 0));
        dst_chunk->append_column(std::move(nullable_column), dst_slot->id());
    }
}

} // namespace pipeline
} // namespace starrocks
