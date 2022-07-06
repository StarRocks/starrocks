// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/balanced_chunk_buffer.h"

#include "fmt/format.h"
#include "util/blocking_queue.hpp"

namespace starrocks::pipeline {

BalancedChunkBuffer::BalancedChunkBuffer(BalanceStrategy strategy, int output_operators)
        : _output_operators(output_operators), _strategy(strategy) {
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

bool BalancedChunkBuffer::try_get(int buffer_index, vectorized::ChunkPtr* output_chunk) {
    return _get_sub_buffer(buffer_index)->try_get(output_chunk);
}

bool BalancedChunkBuffer::put(int buffer_index, vectorized::ChunkPtr chunk) {
    if (_strategy == BalanceStrategy::kDirect) {
        return _get_sub_buffer(buffer_index)->put(chunk);
    } else if (_strategy == BalanceStrategy::kRoundRobin) {
        // TODO: try to balance data according to number of rows
        // But the hard part is, that may needs to maintain a min-heap to account the rows of each
        // output operator, which would introduce some extra overhead
        int target_index = _output_index.fetch_add(1);
        target_index %= _output_operators;
        return _get_sub_buffer(target_index)->put(chunk);
    } else {
        CHECK(false) << "unreachable";
    }
}

} // namespace starrocks::pipeline