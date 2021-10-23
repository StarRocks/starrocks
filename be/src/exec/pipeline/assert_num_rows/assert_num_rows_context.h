// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <deque>

#include "column/vectorized_fwd.h"
namespace starrocks {
namespace pipeline {
class AssertNumRowsContext {
public:
    AssertNumRowsContext() : _input_chunks(), _input_data_complete(false) {}

    void add_rows(int64_t rows) { _num_rows_returned += rows; }

    int64_t get_rows() { return _num_rows_returned; }

    void add_chunk(const vectorized::ChunkPtr& chunk) { _input_chunks.emplace_back(chunk); }

    vectorized::ChunkPtr get_chunk() {
        auto chunk = _input_chunks.front();
        _input_chunks.pop_front();
        return chunk;
    }

    bool has_chunk() { return !_input_chunks.empty(); }

    bool is_data_complete() { return _input_data_complete.load(std::memory_order_acquire); }

    void set_data_complete() { _input_data_complete.store(true, std::memory_order_release); }

private:
    std::deque<vectorized::ChunkPtr> _input_chunks;
    int64_t _num_rows_returned = 0;
    std::atomic<bool> _input_data_complete;
};

} // namespace pipeline
} // namespace starrocks
