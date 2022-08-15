// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <algorithm>
#include <atomic>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {
class RuntimeFilterBuildDescriptor;
}

namespace starrocks::pipeline {
class RuntimeFilterHub;
struct CrossJoinContextParams {
    int32_t num_right_sinkers;
    int32_t plan_node_id;
    std::vector<ExprContext*> filters;
    RuntimeFilterHub* rf_hub;
    std::vector<vectorized::RuntimeFilterBuildDescriptor*> rf_descs;
};

class CrossJoinContext final : public ContextWithDependency {
public:
    explicit CrossJoinContext(CrossJoinContextParams params)
            : _num_right_sinkers(params.num_right_sinkers),
              _plan_node_id(params.plan_node_id),
              _build_chunks(_num_right_sinkers),
              _conjuncts_ctx(std::move(params.filters)),
              _rf_hub(params.rf_hub),
              _rf_descs(std::move(params.rf_descs)) {}

    void close(RuntimeState* state) override;

    bool is_build_chunk_empty() const { return _is_build_chunk_empty; }

    int32_t num_build_chunks() const { return _num_right_sinkers; }

    vectorized::Chunk* get_build_chunk(int32_t build_id) const { return _build_chunks[build_id].get(); }

    void set_build_chunk(const int32_t sinker_id, const vectorized::ChunkPtr& build_chunk) {
        _build_chunks[sinker_id] = build_chunk;
    }

    Status finish_one_right_sinker(RuntimeState* state) {
        if (_num_right_sinkers - 1 == _num_finished_right_sinkers.fetch_add(1)) {
            RETURN_IF_ERROR(_init_runtime_filter(state));
            _is_build_chunk_empty = std::all_of(
                    _build_chunks.begin(), _build_chunks.end(),
                    [](const vectorized::ChunkPtr& chunk) { return chunk == nullptr || chunk->is_empty(); });
            _all_right_finished.store(true, std::memory_order_release);
        }
        return Status::OK();
    }

    bool is_right_finished() const { return _all_right_finished.load(std::memory_order_acquire); }

private:
    Status _init_runtime_filter(RuntimeState* state);

    const int32_t _num_right_sinkers;

    const int32_t _plan_node_id;
    // A CrossJoinLeftOperator waits for all the CrossJoinRightSinkOperators to be finished,
    // and then reads _build_chunks written by the CrossJoinRightSinkOperators.
    // _num_finished_right_sinkers is used to ensure CrossJoinLeftOperator can see all the parts
    // of _build_chunks, when it sees all the CrossJoinRightSinkOperators are finished.
    std::atomic<int32_t> _num_finished_right_sinkers = 0;

    // _build_chunks[i] contains all the rows from i-th CrossJoinRightSinkOperator.
    std::vector<vectorized::ChunkPtr> _build_chunks;

    bool _is_build_chunk_empty = false;
    // finished flags
    std::atomic_bool _all_right_finished = false;

    // conjuncts in cross join, used for generate runtime_filter
    std::vector<ExprContext*> _conjuncts_ctx;

    RuntimeFilterHub* _rf_hub;

    std::vector<vectorized::RuntimeFilterBuildDescriptor*> _rf_descs;
};

} // namespace starrocks::pipeline
