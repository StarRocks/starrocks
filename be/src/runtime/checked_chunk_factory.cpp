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

#include "runtime/checked_chunk_factory.h"

#include "column/chunk_factory.h"
#include "runtime/current_thread.h"

namespace starrocks {

// create object column then reserve is exception safe.

StatusOr<Chunk*> CheckedChunkFactory::new_chunk_pooled_checked(const Schema& schema, size_t n) {
    TRY_CATCH_ALLOC_SCOPE_START()
    auto* chunk = ChunkFactory::new_chunk_pooled(schema, n);
    return chunk;
    TRY_CATCH_ALLOC_SCOPE_END();
}

StatusOr<ChunkUniquePtr> CheckedChunkFactory::new_chunk_checked(const Schema& schema, size_t n) {
    TRY_CATCH_ALLOC_SCOPE_START()
    ChunkUniquePtr chunk;
    chunk = ChunkFactory::new_chunk(schema, n);
    return chunk;
    TRY_CATCH_ALLOC_SCOPE_END();
}

StatusOr<MutableChunkPtr> CheckedChunkFactory::new_mutable_chunk_checked(const Schema& schema, size_t n) {
    TRY_CATCH_ALLOC_SCOPE_START()
    MutableChunkPtr chunk;
    chunk = ChunkFactory::new_mutable_chunk(schema, n);
    return chunk;
    TRY_CATCH_ALLOC_SCOPE_END();
}

} // namespace starrocks
