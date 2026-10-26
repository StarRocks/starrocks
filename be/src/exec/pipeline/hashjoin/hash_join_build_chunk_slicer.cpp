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

#include "exec/pipeline/hashjoin/hash_join_build_chunk_slicer.h"

#include <utility>

#include "column/chunk.h"
#include "exec/pipeline/hashjoin/hash_joiner_fwd.h"

namespace starrocks::pipeline {

Status HashJoinBuildChunkSlicer::reset(std::vector<ChunkPtr> build_chunks) {
    _build_chunks = std::move(build_chunks);
    _build_chunk_idx = 0;
    _slice = ChunkSharedSlice();
    for (const auto& build_chunk : _build_chunks) {
        DCHECK_GT(build_chunk->num_rows(), 0);
        RETURN_IF_ERROR(build_chunk->capacity_limit_reached());
    }
    if (!_build_chunks.empty()) {
        _slice.reset(_build_chunks[0]);
        _slice.skip(kHashJoinKeyColumnOffset);
    }
    return Status::OK();
}

StatusOr<ChunkPtr> HashJoinBuildChunkSlicer::next(size_t chunk_size) {
    if (_slice.empty()) {
        _build_chunk_idx++;
        for (; _build_chunk_idx < _build_chunks.size(); _build_chunk_idx++) {
            const auto& build_chunk = _build_chunks[_build_chunk_idx];
            // Skip the build chunks that hold only the dummy row.
            if (build_chunk->num_rows() > kHashJoinKeyColumnOffset) {
                _slice.reset(build_chunk);
                _slice.skip(kHashJoinKeyColumnOffset);
                break;
            }
        }
        if (_slice.empty()) {
            // Done: drop the build chunks so they can be reclaimed.
            _build_chunks.clear();
            return Status::EndOfFile("eos");
        }
    }
    // cutoff() copies the rows into a fresh chunk, which never shares the columns of the build chunk.
    return ChunkPtr(_slice.cutoff(chunk_size));
}

} // namespace starrocks::pipeline
