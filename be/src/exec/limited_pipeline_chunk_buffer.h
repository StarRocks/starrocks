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

#include <atomic>
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

    bool is_full() const {
#ifndef NDEBUG
        std::lock_guard l(_buffer_mutex);
        DCHECK(!(_buffer_mem_manager.is_full() && _element_size == 0));
#endif
        return _buffer_mem_manager.is_full();
    }

    void push(const ChunkPtr& chunk) {
        ChunkWithInfo chunk_with_info(chunk);
        size_t mem_usage = chunk_with_info.mem_usage;
        size_t num_rows = chunk_with_info.num_rows;
        {
            std::lock_guard l(_buffer_mutex);
            _buffer.push(std::move(chunk_with_info));
        }
        _element_size++;
        _buffer_mem_manager.update_memory_usage(mem_usage, num_rows);
        COUNTER_ADD(_metrics->chunk_buffer_peak_memory, mem_usage);
        COUNTER_ADD(_metrics->chunk_buffer_peak_size, 1);
    }

    ChunkPtr pull() {
        ChunkWithInfo chunk_with_info;
        {
            std::lock_guard l(_buffer_mutex);
            if (_buffer.empty()) {
                return nullptr;
            }
            chunk_with_info = std::move(_buffer.front());
            _buffer.pop();
        }
        _element_size--;
        size_t mem_usage = chunk_with_info.mem_usage;
        size_t num_rows = chunk_with_info.num_rows;
        _buffer_mem_manager.update_memory_usage(-mem_usage, -num_rows);

        DCHECK_EQ(mem_usage, chunk_with_info.chunk->memory_usage());
        DCHECK_EQ(num_rows, chunk_with_info.chunk->num_rows());

        COUNTER_ADD(_metrics->chunk_buffer_peak_memory, -mem_usage);
        COUNTER_ADD(_metrics->chunk_buffer_peak_size, -1);
        return chunk_with_info.chunk;
    }

    bool is_empty() const { return _element_size == 0; }

    void clear() {
        std::lock_guard l(_buffer_mutex);
        _buffer = {};
        _buffer_mem_manager.clear();
        _element_size = 0;
    }

private:
    std::atomic_size_t _element_size{};
    mutable std::mutex _buffer_mutex;

    struct ChunkWithInfo {
        ChunkWithInfo() = default;
        ChunkWithInfo(ChunkPtr chunk_) : chunk(std::move(chunk_)) {
            mem_usage = chunk->memory_usage();
            num_rows = chunk->num_rows();
        }

        size_t mem_usage;
        size_t num_rows;
        ChunkPtr chunk;
    };

    std::queue<ChunkWithInfo> _buffer;
    BufferMetrics* _metrics;
    pipeline::ChunkBufferMemoryManager _buffer_mem_manager;
};
} // namespace starrocks