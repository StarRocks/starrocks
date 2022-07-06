// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <vector>

#include "column/chunk.h"
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
    BalancedChunkBuffer(BalanceStrategy strategy, int output_operators);
    ~BalancedChunkBuffer();

    bool all_empty() const;
    size_t size(int buffer_index) const;
    bool empty(int buffer_index) const;
    bool try_get(int buffer_index, vectorized::ChunkPtr* output_chunk);
    bool put(int buffer_index, vectorized::ChunkPtr chunk);
    void close();

private:
    using QueueT = UnboundedBlockingQueue<vectorized::ChunkPtr>;
    using SubBuffer = std::unique_ptr<QueueT>;

    const SubBuffer& _get_sub_buffer(int index) const;
    SubBuffer& _get_sub_buffer(int index);

    const int _output_operators;
    const BalanceStrategy _strategy;
    std::vector<SubBuffer> _sub_buffers;
    std::atomic_int64_t _output_index = 0;
};

} // namespace starrocks::pipeline
