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

#include "exec/spill/mem_table.h"

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/chunks_sorter.h"
#include "runtime/current_thread.h"

namespace starrocks {
Status UnorderedMemTable::append(ChunkPtr chunk) {
    _tracker->consume(chunk->memory_usage());
    _chunks.emplace_back(std::move(chunk));
    return Status::OK();
}

Status UnorderedMemTable::flush(FlushCallBack callback) {
    for (const auto& chunk : _chunks) {
        RETURN_IF_ERROR(callback(chunk));
    }
    _tracker->release(_tracker->consumption());
    _chunks.clear();
    return Status::OK();
}

Status OrderedMemTable::append(ChunkPtr chunk) {
    if (_chunk == nullptr) {
        _chunk = chunk->clone_empty();
    }
    _chunk->append(*chunk);
    _tracker->set(_chunk->memory_usage());
    return Status::OK();
}

Status OrderedMemTable::flush(FlushCallBack callback) {
    while (!_chunk_slice.empty()) {
        auto chunk = _chunk_slice.cutoff(_runtime_state->chunk_size());
        RETURN_IF_ERROR(callback(chunk));
    }
    _chunk_slice.reset(nullptr);
    _tracker->release(_tracker->consumption());
    _chunk.reset();
    return Status::OK();
}

Status OrderedMemTable::done() {
    // do sort
    ASSIGN_OR_RETURN(_chunk, _do_sort(_chunk));
    _chunk_slice.reset(_chunk);
    return Status::OK();
}

StatusOr<ChunkPtr> OrderedMemTable::_do_sort(const ChunkPtr& chunk) {
    RETURN_IF_ERROR(chunk->upgrade_if_overflow());
    DataSegment segment(_sort_exprs, chunk);
    _permutation.resize(0);

    auto& order_bys = segment.order_by_columns;
    RETURN_IF_ERROR(sort_and_tie_columns(_runtime_state->cancelled_ref(), order_bys, _sort_desc, &_permutation));

    ChunkPtr sorted_chunk = _chunk->clone_empty_with_slot(_chunk->num_rows());
    materialize_by_permutation(sorted_chunk.get(), {_chunk}, _permutation);

    return sorted_chunk;
}

} // namespace starrocks