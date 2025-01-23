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

#include "exec/pipeline/scan/chunk_buffer_limiter.h"

#include <atomic>

#include "glog/logging.h"

namespace starrocks::pipeline {

void DynamicChunkBufferLimiter::update_avg_row_bytes(size_t added_sum_row_bytes, size_t added_num_rows,
                                                     size_t max_chunk_rows) {
    std::lock_guard<std::mutex> lock(_mutex);

    _sum_row_bytes += added_sum_row_bytes;
    _num_rows += added_num_rows;
    size_t avg_row_bytes = 0;
    if (_num_rows > 0) {
        avg_row_bytes = _sum_row_bytes / _num_rows;
    }
    if (avg_row_bytes == 0) {
        return;
    }

    size_t chunk_mem_usage = avg_row_bytes * max_chunk_rows;
    size_t new_capacity = std::max<size_t>(_mem_limit.load() / chunk_mem_usage, 1);
    _capacity = std::min(new_capacity, _max_capacity);
}

ChunkBufferTokenPtr DynamicChunkBufferLimiter::pin(int num_chunks) {
    size_t prev_value = _pinned_chunks_counter.fetch_add(num_chunks);
    if (prev_value + num_chunks > _capacity) {
        unpin(num_chunks);
        return nullptr;
    }
    return std::make_unique<DynamicChunkBufferLimiter::Token>(*this, num_chunks);
}

void DynamicChunkBufferLimiter::unpin(int num_chunks) {
    int prev_value = _pinned_chunks_counter.fetch_sub(num_chunks);
    if ((prev_value >= _capacity || _returned_full_event.load(std::memory_order_acquire)) && !is_full()) {
        _has_full_event = true;
        _returned_full_event = false;
    }
    DCHECK_GE(prev_value, 1);
}

void DynamicChunkBufferLimiter::update_mem_limit(int64_t value) {
    _mem_limit.store(value);
    // No need to update capacity now, capacity will be updated in next `update_avg_row_bytes` call.
}

} // namespace starrocks::pipeline
