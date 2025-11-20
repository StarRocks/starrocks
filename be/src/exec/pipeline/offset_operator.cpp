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

#include "exec/pipeline/offset_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

StatusOr<ChunkPtr> OffsetOperator::pull_chunk(RuntimeState*) {
    return std::move(_cur_chunk);
}

// Core logic similar to LimitOperator::push_chunk
Status OffsetOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _cur_chunk = chunk;

    if (_offset <= 0) {
        return Status::OK();
    }

    int64_t old_offset;
    int64_t rows_to_skip;
    do {
        old_offset = _offset.load(std::memory_order_relaxed);
        rows_to_skip = std::min(old_offset, (int64_t)chunk->num_rows());
    } while (rows_to_skip && !_offset.compare_exchange_strong(old_offset, old_offset - rows_to_skip));

    if (rows_to_skip == chunk->num_rows()) {
        _cur_chunk.reset();
        return Status::OK();
    }

    if (rows_to_skip > 0) {
        auto new_chunk = _cur_chunk->clone_empty();
        new_chunk->append(*_cur_chunk, rows_to_skip, chunk->num_rows() - rows_to_skip);
        _cur_chunk = std::move(new_chunk);
    }

    return Status::OK();
}

} // namespace starrocks::pipeline
