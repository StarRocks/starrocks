// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <algorithm>
#include <atomic>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/context_with_dependency.h"

namespace starrocks::pipeline {

class CrossJoinContext final : public ContextWithDependency {
public:
    explicit CrossJoinContext(const int32_t num_right_sinkers)
            : _num_right_sinkers(num_right_sinkers), _build_chunks(num_right_sinkers) {}

    void close(RuntimeState* state) override { _build_chunks.clear(); }

    bool is_build_chunk_empty() const { return _is_build_chunk_empty; }

    int32_t num_build_chunks() const { return _num_right_sinkers; }

    vectorized::Chunk* get_build_chunk(int32_t build_id) const { return _build_chunks[build_id].get(); }

    void set_build_chunk(const int32_t sinker_id, const vectorized::ChunkPtr& build_chunk) {
        _build_chunks[sinker_id] = build_chunk;
    }

    void finish_one_right_sinker() {
        if (_num_right_sinkers - 1 == _num_finished_right_sinkers.fetch_add(1)) {
            _is_build_chunk_empty = std::all_of(
                    _build_chunks.begin(), _build_chunks.end(),
                    [](const vectorized::ChunkPtr& chunk) { return chunk == nullptr || chunk->is_empty(); });
            _all_right_finished.store(true, std::memory_order_release);
        }
    }

    bool is_right_finished() const { return _all_right_finished.load(std::memory_order_acquire); }

private:
    const int32_t _num_right_sinkers;
    // A CrossJoinLeftOperator waits for all the CrossJoinRightSinkOperators to be finished,
    // and then reads _build_chunks written by the CrossJoinRightSinkOperators.
    // _num_finished_right_sinkers is used to ensure CrossJoinLeftOperator can see all the parts
    // of _build_chunks, when it sees all the CrossJoinRightSinkOperators are finished.
    std::atomic<int32_t> _num_finished_right_sinkers = 0;
    std::atomic_bool _all_right_finished = false;

    // _build_chunks[i] contains all the rows from i-th CrossJoinRightSinkOperator.
    std::vector<vectorized::ChunkPtr> _build_chunks;

    bool _is_build_chunk_empty = false;
};

} // namespace starrocks::pipeline
