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

struct NLJoinContextParams {
    int32_t num_left_probers;
    int32_t num_right_sinkers;
    int32_t plan_node_id;
    std::vector<ExprContext*> filters;
    RuntimeFilterHub* rf_hub;
    std::vector<vectorized::RuntimeFilterBuildDescriptor*> rf_descs;
};

class NLJoinContext final : public ContextWithDependency {
public:
    explicit NLJoinContext(NLJoinContextParams params)
            : _num_left_probers(params.num_left_probers),
              _num_right_sinkers(params.num_right_sinkers),
              _plan_node_id(params.plan_node_id),
              _input_chunks(_num_right_sinkers),
              _rf_conjuncts_ctx(std::move(params.filters)),
              _rf_hub(params.rf_hub),
              _rf_descs(std::move(params.rf_descs)) {}
    ~NLJoinContext() override = default;

    void close(RuntimeState* state) override;

    int32_t get_num_builders() const { return _num_right_sinkers; }
    bool is_build_chunk_empty() const { return _build_chunks.empty(); }
    int32_t num_build_chunks() const { return _build_chunks.size(); }
    size_t num_build_rows() const { return _num_build_rows; }

    vectorized::Chunk* get_build_chunk(int32_t index) const { return _build_chunks[index].get(); }

    int get_build_chunk_size() const { return _build_chunk_desired_size; }

    void append_build_chunk(int32_t sinker_id, const vectorized::ChunkPtr& build_chunk);

    Status finish_one_right_sinker(RuntimeState* state);
    Status finish_one_left_prober(RuntimeState* state);

    bool is_right_finished() const { return _all_right_finished.load(std::memory_order_acquire); }

    // Return true if it's the last prober, which need to perform the right join task
    bool finish_probe(int32_t driver_seq, const std::vector<uint8_t>& build_match_flags);

    const std::vector<uint8_t> get_shared_build_match_flag() const;

private:
    Status _init_runtime_filter(RuntimeState* state);

    const int32_t _num_left_probers;
    const int32_t _num_right_sinkers;
    const int32_t _plan_node_id;

    std::atomic<int32_t> _num_finished_right_sinkers = 0;
    std::atomic<int32_t> _num_finished_left_probers = 0;
    std::atomic_bool _all_right_finished = false;
    std::atomic_int64_t _num_build_rows = 0;

    // Join states
    mutable std::mutex _join_stage_mutex;                         // Protects join states
    std::vector<std::vector<vectorized::ChunkPtr>> _input_chunks; // Input chunks from each sink
    std::vector<vectorized::ChunkPtr> _build_chunks;              // Normalized chunks of _input_chunks
    int _build_chunk_desired_size = 0;
    int _num_post_probers = 0;
    std::vector<uint8_t> _shared_build_match_flag;

    // conjuncts in cross join, used for generate runtime_filter
    std::vector<ExprContext*> _rf_conjuncts_ctx;
    RuntimeFilterHub* _rf_hub;
    std::vector<vectorized::RuntimeFilterBuildDescriptor*> _rf_descs;
};

} // namespace starrocks::pipeline
