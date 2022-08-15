// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/set/union_passthrough_operator.h"

#include "column/column_helper.h"
#include "column/nullable_column.h"

namespace starrocks::pipeline {

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
                auto dst_column = vectorized::ColumnHelper::clone_column(dst_slot->type(), dst_slot->is_nullable(),
                                                                         src_column, src_chunk->num_rows());
                _dst_chunk->append_column(std::move(dst_column), dst_slot->id());
            } else {
                auto dst_column = vectorized::ColumnHelper::move_column(dst_slot->type(), dst_slot->is_nullable(),
                                                                        src_column, src_chunk->num_rows());
                _dst_chunk->append_column(std::move(dst_column), dst_slot->id());
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
            auto dst_column = vectorized::ColumnHelper::move_column(dst_slot->type(), dst_slot->is_nullable(),
                                                                    src_column, src_chunk->num_rows());
            _dst_chunk->append_column(std::move(dst_column), dst_slot->id());
        }
    }

    DCHECK_CHUNK(_dst_chunk);

    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> UnionPassthroughOperator::pull_chunk(RuntimeState* state) {
    return std::move(_dst_chunk);
}

} // namespace starrocks::pipeline
