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

#include <algorithm>
#include <atomic>
#include <memory>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exprs/expr_context.h"
#include "runtime/chunk_cursor.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks {
class RuntimeFilterBuildDescriptor;
}

namespace starrocks::pipeline {

class RuntimeFilterHub;

struct NLJoinContextParams {
    int32_t plan_node_id;
    std::vector<ExprContext*> filters;
    RuntimeFilterHub* rf_hub;
    std::vector<RuntimeFilterBuildDescriptor*> rf_descs;
};

// Each nest loop join build corresponds to a build channel.
class NJJoinBuildInputChannel {
public:
    NJJoinBuildInputChannel(size_t chunk_size) : _chunk_size(chunk_size), _accumulator(chunk_size) {}
    void add_chunk(ChunkPtr build_chunk);
    void finalize();

    size_t num_rows() const { return _num_rows; }

    // Fragmented chunk. it can only be the last chunk in the chunk_list
    ChunkPtr incomplete_chunk() {
        if (!_input_chunks.empty() && _input_chunks.back()->num_rows() < _chunk_size) {
            auto chunk = _input_chunks.back();
            _input_chunks.pop_back();
            return chunk;
        }
        return nullptr;
    }

    template <class ChunkConsumer>
    void for_each_complete_chunk(ChunkConsumer&& consumer) {
        for (auto&& chunk : _input_chunks) {
            DCHECK_EQ(chunk->num_rows(), _chunk_size);
            consumer(std::move(chunk));
        }
    }

private:
    size_t _num_rows{};
    size_t _chunk_size;
    ChunkAccumulator _accumulator;
    std::vector<ChunkPtr> _input_chunks;
};

class NLJoinBuildChunkStreamBuilder {
public:
    NLJoinBuildChunkStreamBuilder() = default;

    Status init(RuntimeState* state, std::vector<std::unique_ptr<NJJoinBuildInputChannel>>& channels);

    std::vector<ChunkPtr> build();

private:
    std::vector<ChunkPtr> _build_chunks;
};

class NLJoinContext final : public ContextWithDependency {
public:
    explicit NLJoinContext(NLJoinContextParams params)
            : _plan_node_id(params.plan_node_id),
              _rf_conjuncts_ctx(std::move(params.filters)),
              _rf_hub(params.rf_hub),
              _rf_descs(std::move(params.rf_descs)) {}
    ~NLJoinContext() override = default;

    void close(RuntimeState* state) override;

    void incr_builder(RuntimeState* state);
    void incr_prober();
    void decr_prober(RuntimeState* state);

    int32_t get_num_builders() const { return _num_right_sinkers; }
    bool is_build_chunk_empty() const { return num_build_rows() == 0; }
    size_t num_build_rows() const { return _num_build_rows; }

    int32_t num_build_chunks() const { return _build_chunks.size(); }
    Chunk* get_build_chunk(int32_t index) const { return _build_chunks[index].get(); }

    int get_build_chunk_size() const { return _build_chunk_desired_size; }

    void append_build_chunk(int32_t sinker_id, const ChunkPtr& build_chunk);

    Status finish_one_right_sinker(int32_t sinker_id, RuntimeState* state);
    Status finish_one_left_prober(RuntimeState* state);

    bool is_right_finished() const { return _all_right_finished.load(std::memory_order_acquire); }

    // Return true if it's the last prober, which need to perform the right join task
    bool finish_probe(int32_t driver_seq, const std::vector<uint8_t>& build_match_flags);

    const std::vector<uint8_t> get_shared_build_match_flag() const;

private:
    Status _init_runtime_filter(RuntimeState* state);

    int32_t _num_left_probers = 0;
    int32_t _num_right_sinkers = 0;
    const int32_t _plan_node_id;

    std::atomic<int32_t> _num_finished_right_sinkers = 0;
    std::atomic<int32_t> _num_finished_left_probers = 0;
    std::atomic<int32_t> _num_closed_left_probers = 0;
    std::atomic_bool _all_right_finished = false;
    std::atomic_int64_t _num_build_rows = 0;

    // Join states
    mutable std::mutex _join_stage_mutex;                                 // Protects join states
    std::vector<std::unique_ptr<NJJoinBuildInputChannel>> _input_channel; // Input chunks from each sink
    NLJoinBuildChunkStreamBuilder _build_stream_builder;
    std::vector<ChunkPtr> _build_chunks; // Normalized chunks of _input_chunks
    int _build_chunk_desired_size = 0;
    int _num_post_probers = 0;
    std::vector<uint8_t> _shared_build_match_flag;

    // conjuncts in cross join, used for generate runtime_filter
    std::vector<ExprContext*> _rf_conjuncts_ctx;
    RuntimeFilterHub* _rf_hub;
    std::vector<RuntimeFilterBuildDescriptor*> _rf_descs;
};

} // namespace starrocks::pipeline
