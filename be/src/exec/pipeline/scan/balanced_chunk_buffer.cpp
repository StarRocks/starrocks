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

#include "exec/pipeline/scan/balanced_chunk_buffer.h"

#include "fmt/format.h"
#include "util/blocking_queue.hpp"

namespace starrocks::pipeline {

BalancedChunkBuffer::BalancedChunkBuffer(BalanceStrategy strategy, int output_operators, ChunkBufferLimiterPtr limiter)
        : _output_operators(output_operators), _strategy(strategy), _limiter(std::move(limiter)) {
    DCHECK_GT(output_operators, 0);
    for (int i = 0; i < output_operators; i++) {
        _sub_buffers.emplace_back(std::make_unique<QueueT>());
    }
}

BalancedChunkBuffer::~BalancedChunkBuffer() = default;

const BalancedChunkBuffer::SubBuffer& BalancedChunkBuffer::_get_sub_buffer(int index) const {
    DCHECK_LT(index, _output_operators);
    return _sub_buffers[index % _output_operators];
}

BalancedChunkBuffer::SubBuffer& BalancedChunkBuffer::_get_sub_buffer(int index) {
    DCHECK_LT(index, _output_operators);
    return _sub_buffers[index % _output_operators];
}

size_t BalancedChunkBuffer::size(int buffer_index) const {
    return _get_sub_buffer(buffer_index)->get_size();
}

bool BalancedChunkBuffer::all_empty() const {
    for (auto& buffer : _sub_buffers) {
        if (!buffer->empty()) {
            return false;
        }
    }
    return true;
}

bool BalancedChunkBuffer::empty(int buffer_index) const {
    return _get_sub_buffer(buffer_index)->empty();
}

void BalancedChunkBuffer::close() {
    for (auto& buffer : _sub_buffers) {
        buffer->clear();
    }
}

bool BalancedChunkBuffer::try_get(int buffer_index, ChunkPtr* output_chunk) {
    // Will release the token after exiting this scope.
    ChunkWithToken chunk_with_token = std::make_pair(nullptr, nullptr);
    bool ok = _get_sub_buffer(buffer_index)->try_get(&chunk_with_token);
    if (ok) {
        *output_chunk = std::move(chunk_with_token.first);
    }
    return ok;
}

bool BalancedChunkBuffer::put(int buffer_index, ChunkPtr chunk, ChunkBufferTokenPtr chunk_token) {
    // CRITICAL (by satanson)
    // EOS chunks may be empty and must be delivered in order to notify CacheOperator that all chunks of the tablet
    // has been processed.
    if (!chunk || (!chunk->owner_info().is_last_chunk() && chunk->num_rows() == 0)) return true;
    if (_strategy == BalanceStrategy::kDirect) {
        return _get_sub_buffer(buffer_index)->put(std::make_pair(std::move(chunk), std::move(chunk_token)));
    } else if (_strategy == BalanceStrategy::kRoundRobin) {
        // TODO: try to balance data according to number of rows
        // But the hard part is, that may needs to maintain a min-heap to account the rows of each
        // output operator, which would introduce some extra overhead
        int target_index = _output_index.fetch_add(1);
        target_index %= _output_operators;
        return _get_sub_buffer(target_index)->put(std::make_pair(std::move(chunk), std::move(chunk_token)));
    } else {
        CHECK(false) << "unreachable";
    }
}

void BalancedChunkBuffer::set_finished(int buffer_index) {
    _get_sub_buffer(buffer_index)->shutdown();
    _get_sub_buffer(buffer_index)->clear();
}

void BalancedChunkBuffer::update_limiter(Chunk* chunk) {
    static constexpr int UPDATE_AVG_ROW_BYTES_FREQUENCY = 8;
    // Update local counters.
    LimiterContext& ctx = _limiter_context;
    ctx.local_sum_row_bytes += chunk->memory_usage();
    ctx.local_num_rows += chunk->num_rows();
    ctx.local_max_chunk_rows = std::max(ctx.local_max_chunk_rows, chunk->num_rows());

    if (ctx.local_sum_chunks++ % UPDATE_AVG_ROW_BYTES_FREQUENCY == 0) {
        _limiter->update_avg_row_bytes(ctx.local_sum_row_bytes, ctx.local_num_rows, ctx.local_max_chunk_rows);
        ctx.local_sum_row_bytes = 0;
        ctx.local_num_rows = 0;
    }
}

} // namespace starrocks::pipeline
