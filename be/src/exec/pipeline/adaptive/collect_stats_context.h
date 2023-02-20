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

#include "column/vectorized_fwd.h"
#include "exec/pipeline/adaptive/adaptive_fwd.h"
#include "exec/pipeline/context_with_dependency.h"
#include "storage/chunk_helper.h"
#include "util/moodycamel/concurrentqueue.h"

namespace starrocks {

class Status;
template <typename T>
class StatusOr;

namespace pipeline {

enum class CollectStatsStateEnum { BLOCK = 0, PASSTHROUGH, ROUND_ROBIN };

/// CollectStatsContext is shared by CollectStatsSinkOperator (CsSink) and CollectStatsSourceOperator (CsSource).
///
/// It is used to adjust DOP after some streaming source operators, such as ScanNode and ExchangeNode.
/// [SourceOp->NextOp] will be modified to [SourceOp->CsSink] --(CsContext)--> [CsSource->NextOp].
///                                             Pipeline#1                          Pipeline#2
///
///                PassthroughState
///             /
/// BlockState
///             \
///                RoundRobinState
/// CsSink starts from BlockState and transforms to PassthroughState or RoundRobinState conditionally.
/// - BlockState blocks the input data and doesn't push it to NextOp.
/// - PassthroughState is transformed to,
///   when BlockState receives max_block_rows_per_driver_seq*DOP rows and SourceOp hasn't been not EOS.
///   - It doesn't adjust DOP of pipeline#2,
///   - and passes chunks from the i-th pipeline#1 driver to the i-th pipeline#2 driver.
/// - RoundRobinState is transformed to,
///   when SourceOp has been EOS before BlockState receives max_block_rows_per_driver_seq*DOP rows.
///   - It adjust DOP of pipeline#2 to compute_max_le_power2(num_rows/max_block_rows_per_driver_seq),
///   - and passes chunks from the i-th pipeline#1 driver to the j-th pipeline#2 driver, where j=i%new_dop.
class CollectStatsContext final : public ContextWithDependency {
public:
    CollectStatsContext(RuntimeState* const runtime_state, size_t dop, const AdaptiveDopParam& param);
    ~CollectStatsContext() override = default;

    std::string readable_state() const;

    void close(RuntimeState* state) override;

    bool need_input(int32_t driver_seq) const;
    bool has_output(int32_t driver_seq) const;
    bool is_downstream_finished(int32_t driver_seq) const;
    bool is_upstream_finished(int32_t driver_seq) const;

    Status push_chunk(int32_t driver_seq, ChunkPtr chunk);
    StatusOr<ChunkPtr> pull_chunk(int32_t driver_seq);
    Status set_finishing(int32_t driver_seq);
    Status set_finished(int32_t driver_seq);

    bool is_downstream_ready() const;
    size_t downstream_dop() const { return _downstream_dop; }

private:
    using BufferChunkQueue = std::queue<ChunkPtr>;

    CollectStatsStateRawPtr _get_state(CollectStatsStateEnum state) const;
    CollectStatsStateRawPtr _state_ref() const;
    void _set_state(CollectStatsStateEnum state_enum);
    void _transform_state(CollectStatsStateEnum state_enum, size_t downstream_dop);
    BufferChunkQueue& _buffer_chunk_queue(int32_t driver_seq);

private:
    friend class BlockState;
    friend class RoundRobinState;
    friend class PassthroughState;

    std::atomic<CollectStatsStateRawPtr> _state = nullptr;
    std::unordered_map<CollectStatsStateEnum, CollectStatsStatePtr> _state_payloads;

    // _upstream_dop and _downstream_dop are DOP of CollectStatsSinkOperator and CollectStatsSourceOperator.
    // They are both power of two, and upstream_dop is times of downstream_dop.
    const size_t _upstream_dop;
    size_t _downstream_dop;

    const size_t _max_block_rows_per_driver_seq;

    std::vector<BufferChunkQueue> _buffer_chunk_queue_per_driver_seq;
    std::vector<uint8_t> _is_finishing_per_driver_seq;
    std::vector<uint8_t> _is_finished_per_driver_seq;

    RuntimeState* const _runtime_state;
};

class CollectStatsState {
public:
    CollectStatsState(CollectStatsContext* const ctx) : _ctx(ctx) {}
    virtual ~CollectStatsState() = default;

    virtual std::string name() const = 0;

    virtual bool need_input(int32_t driver_seq) const = 0;
    virtual bool has_output(int32_t driver_seq) const = 0;
    virtual bool is_downstream_finished(int32_t driver_seq) const = 0;
    virtual bool is_upstream_finished(int32_t driver_seq) const = 0;

    virtual Status push_chunk(int32_t driver_seq, ChunkPtr chunk) = 0;
    virtual StatusOr<ChunkPtr> pull_chunk(int32_t driver_seq) = 0;
    virtual Status set_finishing(int32_t driver_seq) = 0;

protected:
    CollectStatsContext* const _ctx;
};

class BlockState final : public CollectStatsState {
public:
    BlockState(CollectStatsContext* const ctx)
            : CollectStatsState(ctx), _max_buffer_rows(ctx->_max_block_rows_per_driver_seq * ctx->_upstream_dop) {}
    ~BlockState() override = default;

    std::string name() const override;

    bool need_input(int32_t driver_seq) const override;
    bool has_output(int32_t driver_seq) const override;
    bool is_downstream_finished(int32_t driver_seq) const override;
    bool is_upstream_finished(int32_t driver_seq) const override;

    Status push_chunk(int32_t driver_seq, ChunkPtr chunk) override;
    StatusOr<ChunkPtr> pull_chunk(int32_t driver_seq) override;
    Status set_finishing(int32_t driver_seq) override;

private:
    std::atomic<int> _num_finished_seqs = 0;
    std::atomic<size_t> _num_rows = 0;
    const size_t _max_buffer_rows;
};

class PassthroughState final : public CollectStatsState {
public:
    PassthroughState(CollectStatsContext* const ctx);
    ~PassthroughState() override = default;

    std::string name() const override;

    bool need_input(int32_t driver_seq) const override;
    bool has_output(int32_t driver_seq) const override;
    bool is_downstream_finished(int32_t driver_seq) const override;
    bool is_upstream_finished(int32_t driver_seq) const override;

    Status push_chunk(int32_t driver_seq, ChunkPtr chunk) override;
    StatusOr<ChunkPtr> pull_chunk(int32_t driver_seq) override;
    Status set_finishing(int32_t driver_seq) override;

private:
    static constexpr size_t MAX_PASSTHROUGH_CHUNKS_PER_DRIVER_SEQ = 32;
    static constexpr size_t UNPLUG_THRESHOLD_PER_DRIVER_SEQ = MAX_PASSTHROUGH_CHUNKS_PER_DRIVER_SEQ / 2;

    using ChunkQueue = moodycamel::ConcurrentQueue<ChunkPtr>;
    std::vector<ChunkQueue> _in_chunk_queue_per_driver_seq;
    mutable std::vector<uint8_t> _unpluging_per_driver_seq;
};

class RoundRobinState final : public CollectStatsState {
public:
    RoundRobinState(CollectStatsContext* const ctx);
    ~RoundRobinState() override = default;

    std::string name() const override;

    bool need_input(int32_t driver_seq) const override;
    bool has_output(int32_t driver_seq) const override;
    bool is_downstream_finished(int32_t driver_seq) const override;
    bool is_upstream_finished(int32_t driver_seq) const override;

    Status push_chunk(int32_t driver_seq, ChunkPtr chunk) override;
    StatusOr<ChunkPtr> pull_chunk(int32_t driver_seq) override;
    Status set_finishing(int32_t driver_seq) override;

private:
    struct DriverInfo {
    public:
        DriverInfo(int32_t driver_seq, size_t chunk_size) : buffer_idx(driver_seq), accumulator(chunk_size) {}

        int buffer_idx;
        ChunkAccumulator accumulator;
    };

    std::vector<DriverInfo> _info_per_driver_seq;
};

} // namespace pipeline
} // namespace starrocks
