// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/balanced_chunk_buffer.h"

#include "util/blocking_queue.hpp"

namespace starrocks::pipeline {

BalancedChunkBuffer::BalancedChunkBuffer(BalanceStrategy strategy, int output_runs)
        : _output_runs(output_runs), _strategy(strategy) {
    DCHECK_GT(output_runs, 0);
    for (int i = 0; i < output_runs; i++) {
        _sub_buffers.emplace_back(std::make_unique<QueueT>());
    }
}

const BalancedChunkBuffer::SubBuffer& BalancedChunkBuffer::_get_sub_buffer(int index) const {
    DCHECK_LT(index, _output_runs);
    return _sub_buffers[index % _output_runs];
}

BalancedChunkBuffer::SubBuffer& BalancedChunkBuffer::_get_sub_buffer(int index) {
    DCHECK_LT(index, _output_runs);
    return _sub_buffers[index % _output_runs];
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

bool BalancedChunkBuffer::try_get(int buffer_index, vectorized::ChunkPtr* output_chunk) {
    return _get_sub_buffer(buffer_index)->try_get(output_chunk);
}

bool BalancedChunkBuffer::put(int buffer_index, vectorized::ChunkPtr chunk) {
    int target_index = _output_index.fetch_add(1);
    target_index %= _output_runs;
    return _get_sub_buffer(target_index)->put(chunk);
}

} // namespace starrocks::pipeline