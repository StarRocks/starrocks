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

#include "exec/pipeline/set/union_passthrough_operator.h"

#include "column/column_helper.h"
#include "column/nullable_column.h"

namespace starrocks::pipeline {

Status UnionPassthroughOperator::push_chunk(RuntimeState* state, const ChunkPtr& src_chunk) {
    DCHECK_EQ(_dst_chunk, nullptr);

    _dst_chunk = std::make_shared<Chunk>();

    if (_dst2src_slot_map != nullptr) {
        for (auto* dst_slot : _dst_slots) {
            auto& src_slot_item = (*_dst2src_slot_map)[dst_slot->id()];
            ColumnPtr& src_column = src_chunk->get_column_by_slot_id(src_slot_item.slot_id);
            // If there are multiple dest slots mapping to the same src slot id,
            // we should clone the src column instead of directly moving the src column.
            if (src_slot_item.ref_count > 1) {
                auto dst_column = ColumnHelper::clone_column(dst_slot->type(), dst_slot->is_nullable(), src_column,
                                                             src_chunk->num_rows());
                _dst_chunk->append_column(std::move(dst_column), dst_slot->id());
            } else {
                auto dst_column = ColumnHelper::move_column(dst_slot->type(), dst_slot->is_nullable(), src_column,
                                                            src_chunk->num_rows());
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
            auto dst_column = ColumnHelper::move_column(dst_slot->type(), dst_slot->is_nullable(), src_column,
                                                        src_chunk->num_rows());
            _dst_chunk->append_column(std::move(dst_column), dst_slot->id());
        }
    }

    DCHECK_CHUNK(_dst_chunk);

    return Status::OK();
}

StatusOr<ChunkPtr> UnionPassthroughOperator::pull_chunk(RuntimeState* state) {
    return std::move(_dst_chunk);
}

} // namespace starrocks::pipeline
