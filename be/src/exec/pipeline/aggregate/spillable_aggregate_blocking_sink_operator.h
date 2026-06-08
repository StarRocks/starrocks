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
#include <optional>
#include <utility>

#include "aggregate_blocking_sink_operator.h"
#include "base/concurrency/race_detect.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/object_pool.h"
#include "exec/aggregator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/primitives/spillable_flat_sink_mixin.h"
#include "exec/pipeline/spill_process_channel.h"
#include "exec/sorted_streaming_aggregator.h"
#include "exprs/sort_exec_exprs.h"
#include "runtime/runtime_state_fwd.h"

namespace starrocks::pipeline {
class SpillableAggregateBlockingSinkOperator : public AggregateBlockingSinkOperator,
                                               public SpillableFlatSinkMixin<SpillableAggregateBlockingSinkOperator> {
    friend class SpillableFlatSinkMixin<SpillableAggregateBlockingSinkOperator>;

public:
    template <class... Args>
    SpillableAggregateBlockingSinkOperator(AggregatorPtr aggregator, Args&&... args)
            : AggregateBlockingSinkOperator(std::move(aggregator), std::forward<Args>(args)...,
                                            "spillable_aggregate_blocking_sink") {}

    ~SpillableAggregateBlockingSinkOperator() override = default;

    bool need_input() const override;
    bool is_finished() const override;

    // The sink sleeps OUTPUT_FULL on need_input(): is_full() (writer full, woken by the flush-completion
    // defer on the sink list) or the spill-process channel still holding a task (woken by the channel
    // handshake). covered_wakeups() declares both reasons the prepare() sink-list subscription covers.
    BlockReason block_reason() const override;
    // Compile-time copy of the wakeup table: the declared coverage sits next to the class, and the
    // static_assert below keeps every reason this operator can name inside the mask.
    static constexpr uint32_t kCoveredWakeups = kSpillSinkCoveredWakeups;
    uint32_t covered_wakeups() const override { return kCoveredWakeups; }

    Status set_finishing(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status prepare_local_state(RuntimeState* state) override { return Status::OK(); }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    bool spillable() const override { return true; }

    void set_execute_mode(int performance_level) override {
        _spill_strategy = spill::SpillStrategy::SPILL_ALL;
        TRACE_SPILL_LOG << "AggregateBlockingSink, mark spill " << (void*)this;
    }

    size_t estimated_memory_reserved(const ChunkPtr& chunk) override {
        if (chunk && !chunk->is_empty()) {
            if (_aggregator->hash_map_variant().need_expand(chunk->num_rows())) {
                return chunk->memory_usage() + _aggregator->hash_map_memory_usage();
            }
            return chunk->memory_usage();
        }
        return 0;
    }

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

    // only the prepare/open phase calls are valid.
    SpillProcessChannelPtr spill_channel() { return _aggregator->spill_channel(); }

private:
    bool spilled() const { return _aggregator->spiller()->spilled(); }

    // Spiller/spill-channel pair that the SpillableFlatSinkMixin reads to compute
    // need_input()/block_reason(): this sink reaches them through its aggregator.
    const std::shared_ptr<spill::Spiller>& _spiller() const { return _aggregator->spiller(); }
    SpillProcessChannelPtr _spill_channel() const { return _aggregator->spill_channel(); }

private:
    Status _try_to_spill_by_force(RuntimeState* state, const ChunkPtr& chunk);

    Status _try_to_spill_by_auto(RuntimeState* state, const ChunkPtr& chunk);

    Status _spill_all_data(RuntimeState* state, bool should_spill_hash_table);

    void _add_streaming_chunk(ChunkPtr chunk);

    std::function<StatusOr<ChunkPtr>()> _build_spill_task(RuntimeState* state, bool should_spill_hash_table = true);

    DECLARE_ONCE_DETECTOR(_set_finishing_once);
    spill::SpillStrategy _spill_strategy = spill::SpillStrategy::NO_SPILL;

    std::queue<ChunkPtr> _streaming_chunks;
    size_t _streaming_rows = 0;
    size_t _streaming_bytes = 0;

    int32_t _continuous_low_reduction_chunk_num = 0;

    bool _is_finished = false;

    RuntimeProfile::Counter* _hash_table_spill_times = nullptr;

    static constexpr double HT_LOW_REDUCTION_THRESHOLD = 0.5;
    static constexpr int32_t HT_LOW_REDUCTION_CHUNK_LIMIT = 5;
};

static_assert(SpillableAggregateBlockingSinkOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_FLUSH));
static_assert(SpillableAggregateBlockingSinkOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_CHANNEL));

class SpillableAggregateBlockingSinkOperatorFactory : public AggregateBlockingSinkOperatorFactory {
public:
    SpillableAggregateBlockingSinkOperatorFactory(
            int32_t id, int32_t plan_node_id, AggregatorFactoryPtr aggregator_factory,
            const std::vector<RuntimeFilterBuildDescriptor*>& build_runtime_filters,
            SpillProcessChannelFactoryPtr spill_channel_factory)
            : AggregateBlockingSinkOperatorFactory(id, plan_node_id, std::move(aggregator_factory),
                                                   build_runtime_filters, "spillable_aggregate_blocking_sink"),
              _spill_channel_factory(std::move(spill_channel_factory)) {}

    ~SpillableAggregateBlockingSinkOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    // Event-scheduler opt-in, gated on enable_spill_agg_events (default false). The kill-switch demotes the
    // whole agg fragment back to the poller without a rebuild.
    bool support_event_scheduler() const override { return config::enable_spill_agg_events; }

private:
    ObjectPool _pool;
    SortExecExprs _sort_exprs;
    SortDescs _sort_desc;

    std::shared_ptr<spill::SpilledOptions> _spill_options;
    std::shared_ptr<spill::SpillerFactory> _spill_factory = std::make_shared<spill::SpillerFactory>();
    SpillProcessChannelFactoryPtr _spill_channel_factory;
};

} // namespace starrocks::pipeline
