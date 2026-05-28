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

#include "runtime/chunk_helper.h"

#include <memory>
#include <utility>

#include "column/chunk_factory.h"
#include "column/column_helper.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"

namespace starrocks {

// create object column then reserve is exception safe.

StatusOr<Chunk*> RuntimeChunkHelper::new_chunk_pooled_checked(const Schema& schema, size_t n) {
    TRY_CATCH_ALLOC_SCOPE_START()
    auto* chunk = ChunkFactory::new_chunk_pooled(schema, n);
    return chunk;
    TRY_CATCH_ALLOC_SCOPE_END();
}

StatusOr<ChunkUniquePtr> RuntimeChunkHelper::new_chunk_checked(const Schema& schema, size_t n) {
    TRY_CATCH_ALLOC_SCOPE_START()
    ChunkUniquePtr chunk;
    chunk = ChunkFactory::new_chunk(schema, n);
    return chunk;
    TRY_CATCH_ALLOC_SCOPE_END();
}

StatusOr<MutableChunkPtr> RuntimeChunkHelper::new_mutable_chunk_checked(const Schema& schema, size_t n) {
    TRY_CATCH_ALLOC_SCOPE_START()
    MutableChunkPtr chunk;
    chunk = ChunkFactory::new_mutable_chunk(schema, n);
    return chunk;
    TRY_CATCH_ALLOC_SCOPE_END();
}

ChunkUniquePtr RuntimeChunkHelper::new_chunk(const TupleDescriptor& tuple_desc, size_t n) {
    return new_chunk(tuple_desc.slots(), n);
}

ChunkUniquePtr RuntimeChunkHelper::new_chunk(const std::vector<SlotDescriptor*>& slots, size_t n) {
    auto chunk = std::make_unique<Chunk>();
    for (const auto slot : slots) {
        auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
        column->reserve(n);
        chunk->append_column(std::move(column), slot->id());
    }
    return chunk;
}

StatusOr<ChunkUniquePtr> RuntimeChunkHelper::new_chunk_checked(const std::vector<SlotDescriptor*>& slots, size_t n) {
    TRY_CATCH_ALLOC_SCOPE_START()
    ChunkUniquePtr chunk;
    chunk = RuntimeChunkHelper::new_chunk(slots, n);
    return chunk;
    TRY_CATCH_ALLOC_SCOPE_END();
}

StatusOr<ChunkUniquePtr> RuntimeChunkHelper::new_chunk_checked(const TupleDescriptor& tuple_desc, size_t n) {
    return RuntimeChunkHelper::new_chunk_checked(tuple_desc.slots(), n);
}

MutableChunkPtr RuntimeChunkHelper::new_mutable_chunk(const TupleDescriptor& tuple_desc, size_t n) {
    return new_mutable_chunk(tuple_desc.slots(), n);
}

MutableChunkPtr RuntimeChunkHelper::new_mutable_chunk(const std::vector<SlotDescriptor*>& slots, size_t n) {
    auto chunk = std::make_shared<MutableChunk>();
    for (const auto slot : slots) {
        auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
        column->reserve(n);
        chunk->append_column(std::move(column), slot->id());
    }
    return chunk;
}

StatusOr<MutableChunkPtr> RuntimeChunkHelper::new_mutable_chunk_checked(const std::vector<SlotDescriptor*>& slots,
                                                                        size_t n) {
    TRY_CATCH_ALLOC_SCOPE_START()
    MutableChunkPtr chunk;
    chunk = RuntimeChunkHelper::new_mutable_chunk(slots, n);
    return chunk;
    TRY_CATCH_ALLOC_SCOPE_END();
}

StatusOr<MutableChunkPtr> RuntimeChunkHelper::new_mutable_chunk_checked(const TupleDescriptor& tuple_desc, size_t n) {
    return RuntimeChunkHelper::new_mutable_chunk_checked(tuple_desc.slots(), n);
}

void RuntimeChunkHelper::reorder_chunk(const TupleDescriptor& tuple_desc, Chunk* chunk) {
    return reorder_chunk(tuple_desc.slots(), chunk);
}

void RuntimeChunkHelper::reorder_chunk(const std::vector<SlotDescriptor*>& slots, Chunk* chunk) {
    auto reordered_chunk = Chunk();
    auto& original_chunk = (*chunk);
    for (auto slot : slots) {
        auto slot_id = slot->id();
        reordered_chunk.append_column(original_chunk.get_column_by_slot_id(slot_id), slot_id);
    }
    original_chunk.swap_chunk(reordered_chunk);
}

} // namespace starrocks
