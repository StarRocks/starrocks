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

#pragma once

#include <cstddef>
#include <vector>

#include "column/chunk.h"
#include "common/statusor.h"
#include "runtime/descriptors_fwd.h"

namespace starrocks {

class Schema;

class RuntimeChunkHelper {
public:
    static StatusOr<Chunk*> new_chunk_pooled_checked(const Schema& schema, size_t n);
    static StatusOr<ChunkUniquePtr> new_chunk_checked(const Schema& schema, size_t n);
    static StatusOr<MutableChunkPtr> new_mutable_chunk_checked(const Schema& schema, size_t n);

    // Create an empty chunk according to the |slots| and reserve it of size |n|.
    static ChunkUniquePtr new_chunk(const std::vector<SlotDescriptor*>& slots, size_t n);
    // Create an empty chunk according to the |tuple_desc| and reserve it of size |n|.
    static ChunkUniquePtr new_chunk(const TupleDescriptor& tuple_desc, size_t n);
    // a wrapper of new_chunk with memory check
    static StatusOr<ChunkUniquePtr> new_chunk_checked(const std::vector<SlotDescriptor*>& slots, size_t n);
    static StatusOr<ChunkUniquePtr> new_chunk_checked(const TupleDescriptor& tuple_desc, size_t n);

    // Create an empty mutable chunk according to the |slots| and reserve it of size |n|.
    static MutableChunkPtr new_mutable_chunk(const std::vector<SlotDescriptor*>& slots, size_t n);
    static MutableChunkPtr new_mutable_chunk(const TupleDescriptor& tuple_desc, size_t n);
    // a wrapper of new_mutable_chunk with memory check
    static StatusOr<MutableChunkPtr> new_mutable_chunk_checked(const std::vector<SlotDescriptor*>& slots, size_t n);
    static StatusOr<MutableChunkPtr> new_mutable_chunk_checked(const TupleDescriptor& tuple_desc, size_t n);

    // Reorder columns of `chunk` according to the order of |tuple_desc|.
    static void reorder_chunk(const TupleDescriptor& tuple_desc, Chunk* chunk);
    // Reorder columns of `chunk` according to the order of |slots|.
    static void reorder_chunk(const std::vector<SlotDescriptor*>& slots, Chunk* chunk);
};

} // namespace starrocks
