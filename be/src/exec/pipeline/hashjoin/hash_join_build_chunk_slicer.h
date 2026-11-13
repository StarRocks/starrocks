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

#include "column/chunk_slice.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"

namespace starrocks::pipeline {

// Cuts the build chunks of the hash tables into chunks of at most chunk_size rows, for the spill of a hash join build.
// Every build chunk starts with the hash table's dummy row (kHashJoinKeyColumnOffset), which is never spilled, and a
// build chunk that holds only the dummy row produces no slice.
class HashJoinBuildChunkSlicer {
public:
    // Takes the build chunks to spill, checks their capacity and positions at the first real row. The spill can run
    // before JoinHashTable::build, so the build chunks may not have been checked yet.
    Status reset(std::vector<ChunkPtr> build_chunks);

    // Returns a copy of the next at most chunk_size rows, or EndOfFile once every build chunk is consumed. The build
    // chunks are released at EndOfFile.
    StatusOr<ChunkPtr> next(size_t chunk_size);

private:
    // Owned as shared_ptr so the spill never dereferences hash-table state that the join builder may free on
    // cancel/close while the spill runs asynchronously.
    std::vector<ChunkPtr> _build_chunks;
    size_t _build_chunk_idx = 0;
    ChunkSharedSlice _slice;
};

} // namespace starrocks::pipeline
