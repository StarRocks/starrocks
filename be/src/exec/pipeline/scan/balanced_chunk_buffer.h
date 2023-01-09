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

#include <vector>

#include "column/chunk.h"
#include "exec/pipeline/scan/chunk_buffer_limiter.h"
#include "util/blocking_queue.hpp"

namespace starrocks::pipeline {

// TODO: support hash distribution instead of simple round-robin
enum BalanceStrategy {
    kDirect,     // Assign chunks from input operator to output operator directly
    kRoundRobin, // Assign chunks from input operator to output with round-robin strategy
};

// A chunk-buffer which try to balance output for each operator
class BalancedChunkBuffer {
public:
    BalancedChunkBuffer(BalanceStrategy strategy, int output_operators, ChunkBufferLimiterPtr limiter);
    ~BalancedChunkBuffer();

    bool all_empty() const;
    size_t size(int buffer_index) const;
    bool empty(int buffer_index) const;
    bool try_get(int buffer_index, ChunkPtr* output_chunk);
    bool put(int buffer_index, ChunkPtr chunk, ChunkBufferTokenPtr chunk_token);
    void close();
    // Mark that it needn't produce any chunk anymore.
    void set_finished(int buffer_index);

    ChunkBufferLimiter* limiter() { return _limiter.get(); }
    void update_limiter(Chunk* chunk);

private:
    struct LimiterContext {
        // ========================
        // Local counters for row-size estimation, will be reset after a batch
        size_t local_sum_row_bytes = 0;
        size_t local_num_rows = 0;
        size_t local_sum_chunks = 0;
        size_t local_max_chunk_rows = 0;
    };

    using ChunkWithToken = std::pair<ChunkPtr, ChunkBufferTokenPtr>;
    using QueueT = UnboundedBlockingQueue<ChunkWithToken>;
    using SubBuffer = std::unique_ptr<QueueT>;

    const SubBuffer& _get_sub_buffer(int index) const;
    SubBuffer& _get_sub_buffer(int index);

    const int _output_operators;
    const BalanceStrategy _strategy;
    std::vector<SubBuffer> _sub_buffers;
    std::atomic_int64_t _output_index = 0;

    ChunkBufferLimiterPtr _limiter;
    LimiterContext _limiter_context;
};

} // namespace starrocks::pipeline
