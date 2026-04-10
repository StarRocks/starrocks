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

#include "column/chunk_slice.h"

#include <algorithm>
#include <utility>

#include "column/chunk.h"
#include "storage/chunk_helper.h"

namespace starrocks {

template <class Ptr>
bool ChunkSliceTemplate<Ptr>::empty() const {
    return !chunk || offset == chunk->num_rows();
}

template <class Ptr>
size_t ChunkSliceTemplate<Ptr>::rows() const {
    return chunk->num_rows() - offset;
}

template <class Ptr>
void ChunkSliceTemplate<Ptr>::reset(Ptr input) {
    chunk = std::move(input);
}

template <class Ptr>
size_t ChunkSliceTemplate<Ptr>::skip(size_t skip_rows) {
    size_t real_skipped = std::min(rows(), skip_rows);
    offset += real_skipped;
    if (empty()) {
        chunk.reset();
        offset = 0;
    }

    return real_skipped;
}

// Cutoff required rows from this chunk.
template <class Ptr>
ChunkUniquePtr ChunkSliceTemplate<Ptr>::cutoff(size_t required_rows) {
    DCHECK(!empty());
    size_t cut_rows = std::min(rows(), required_rows);
    auto res = chunk->clone_empty(cut_rows);
    res->append(*chunk, offset, cut_rows);
    offset += cut_rows;
    if (empty()) {
        chunk.reset();
        offset = 0;
    }
    return res;
}

// Specialized for SegmentedChunkPtr.
template <>
ChunkUniquePtr ChunkSliceTemplate<SegmentedChunkPtr>::cutoff(size_t required_rows) {
    DCHECK(!empty());
    // Cutoff a chunk from current segment; if it doesn't meet the requirement just let it be.
    ChunkPtr segment = chunk->segments()[segment_id];
    size_t segment_offset = offset % chunk->segment_size();
    size_t cut_rows = std::min(segment->num_rows() - segment_offset, required_rows);

    auto res = segment->clone_empty(cut_rows);
    res->append(*segment, segment_offset, cut_rows);
    offset += cut_rows;

    // Move to next segment.
    segment_id = offset / chunk->segment_size();

    if (empty()) {
        chunk->reset();
        offset = 0;
    }
    return res;
}

template struct ChunkSliceTemplate<ChunkPtr>;
template struct ChunkSliceTemplate<ChunkUniquePtr>;
template struct ChunkSliceTemplate<SegmentedChunkPtr>;

} // namespace starrocks
