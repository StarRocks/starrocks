// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <algorithm>
#include <atomic>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"

namespace starrocks::pipeline {

class CrossJoinContext {
public:
    explicit CrossJoinContext(const int32_t num_right_sinkers)
            : _num_right_sinkers(num_right_sinkers), _build_chunks(num_right_sinkers) {}

    bool is_build_chunk_empty() const {
        return std::all_of(_build_chunks.begin(), _build_chunks.end(),
                           [](const vectorized::ChunkPtr& chunk) { return chunk == nullptr || chunk->is_empty(); });
    }

    int32_t num_build_chunks() const { return _num_right_sinkers; }

    vectorized::Chunk* get_build_chunk(int32_t build_id) const { return _build_chunks[build_id].get(); }

    void set_build_chunk(const int32_t sinker_id, const vectorized::ChunkPtr& build_chunk) {
        _build_chunks[sinker_id] = build_chunk;
    }

    void finish_one_right_sinker() { _num_finished_right_sinkers.fetch_add(1, std::memory_order_release); }

    bool is_right_finished() const {
        return _num_finished_right_sinkers.load(std::memory_order_acquire) == _num_right_sinkers;
    }

private:
    const int32_t _num_right_sinkers;
    // A CrossJoinLeftOperator waits for all the CrossJoinRightSinkOperators to be finished,
    // and then reads _build_chunks written by the CrossJoinRightSinkOperators.
    // _num_finished_right_sinkers is used to ensure CrossJoinLeftOperator can see all the parts
    // of _build_chunks, when it sees all the CrossJoinRightSinkOperators are finished.
    std::atomic<int32_t> _num_finished_right_sinkers = 0;

    // _build_chunks[i] contains all the rows from i-th CrossJoinRightSinkOperator.
    std::vector<vectorized::ChunkPtr> _build_chunks;
};

} // namespace starrocks::pipeline
