// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <vector>

#include "column/chunk.h"
#include "util/blocking_queue.hpp"

namespace starrocks::pipeline {

// TODO: support hash distribution instead of simple round-robin
enum BalanceStrategy {
    kDirect,
    kRoundRobin,
};

// A chunk-buffer which try to balance output for each operator
class BalancedChunkBuffer {
public:
    BalancedChunkBuffer(BalanceStrategy strategy, int output_runs);
    ~BalancedChunkBuffer() = default;

    size_t size(int buffer_index) const;
    bool empty(int buffer_index) const;
    bool all_empty() const;
    bool try_get(int buffer_index, vectorized::ChunkPtr* output_chunk);
    bool put(int buffer_index, vectorized::ChunkPtr chunk);

private:
    using QueueT = UnboundedBlockingQueue<vectorized::ChunkPtr>;
    using SubBuffer = std::unique_ptr<QueueT>;

    const SubBuffer& _get_sub_buffer(int index) const;
    SubBuffer& _get_sub_buffer(int index);

    const int _output_runs;
    const BalanceStrategy _strategy;
    std::atomic_int64_t _output_index = 0;
    std::vector<SubBuffer> _sub_buffers;
};

} // namespace starrocks::pipeline
