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

#include <queue>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/chunk_buffer_memory_manager.h"
#include "util/runtime_profile.h"

namespace starrocks {
template <class BufferMetrics>
class LimitedPipelineChunkBuffer {
public:
    LimitedPipelineChunkBuffer(BufferMetrics* metrics, size_t max_dop, size_t max_memory_usage, size_t max_chunk_count)
            : _metrics(metrics), _buffer_mem_manager(max_dop, max_memory_usage, max_chunk_count) {}

    bool is_full() const { return _buffer_mem_manager.is_full(); }

    void push(const ChunkPtr& chunk) {
        size_t mem_usage = chunk->memory_usage();
        size_t num_rows = chunk->num_rows();
        {
            std::lock_guard l(_buffer_mutex);
            _buffer.push(chunk);
        }
        _buffer_mem_manager.update_memory_usage(mem_usage, num_rows);
        COUNTER_ADD(_metrics->chunk_buffer_peak_memory, mem_usage);
        COUNTER_ADD(_metrics->chunk_buffer_peak_size, 1);
    }

    ChunkPtr pull() {
        ChunkPtr chunk;
        {
            std::lock_guard<std::mutex> l(_buffer_mutex);
            if (_buffer.empty()) {
                return nullptr;
            }
            chunk = _buffer.front();
            _buffer.pop();
        }
        size_t mem_usage = chunk->memory_usage();
        size_t num_rows = chunk->num_rows();
        _buffer_mem_manager.update_memory_usage(-mem_usage, -num_rows);

        COUNTER_ADD(_metrics->chunk_buffer_peak_memory, -mem_usage);
        COUNTER_ADD(_metrics->chunk_buffer_peak_size, -1);
        return chunk;
    }

    bool is_empty() {
        std::lock_guard<std::mutex> l(_buffer_mutex);
        return _buffer.empty();
    }

    void clear() {
        std::lock_guard<std::mutex> l(_buffer_mutex);
        _buffer = {};
        _buffer_mem_manager.clear();
    }

private:
    std::mutex _buffer_mutex;
    std::queue<ChunkPtr> _buffer;
    BufferMetrics* _metrics;
    pipeline::ChunkBufferMemoryManager _buffer_mem_manager;
};
} // namespace starrocks