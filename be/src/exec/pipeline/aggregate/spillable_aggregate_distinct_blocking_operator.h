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

#include "base/concurrency/race_detect.h"
#include "common/config_exec_flow_fwd.h"
#include "exec/aggregator_fwd.h"
#include "exec/pipeline/aggregate/aggregate_distinct_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_blocking_source_operator.h"
#include "exec/pipeline/operator_factory.h"
#include "exec/pipeline/primitives/block_reason.h"
#include "exec/pipeline/primitives/spillable_flat_sink_mixin.h"
#include "exec/pipeline/primitives/spillable_simple_source_mixin.h"
#include "exec/pipeline/source_operator.h"
#include "exprs/sort_exec_exprs.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {
class SpillableAggregateDistinctBlockingSinkOperator
        : public AggregateDistinctBlockingSinkOperator,
          public SpillableFlatSinkMixin<SpillableAggregateDistinctBlockingSinkOperator> {
    friend class SpillableFlatSinkMixin<SpillableAggregateDistinctBlockingSinkOperator>;

public:
    template <class... Args>
    SpillableAggregateDistinctBlockingSinkOperator(AggregatorPtr aggregator, Args&&... args)
            : AggregateDistinctBlockingSinkOperator(std::move(aggregator), std::forward<Args>(args)...,
                                                    "spillable_aggregate_distinct_blocking_sink") {}
    ~SpillableAggregateDistinctBlockingSinkOperator() override = default;

    bool need_input() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;

    // The sink sleeps OUTPUT_FULL on need_input(): is_full() (writer full, woken by the flush-completion
    // defer on the sink list) or the spill-process channel still holding a task (woken by the channel
    // handshake). covered_wakeups() declares both reasons the prepare() sink-list subscription covers.
    BlockReason block_reason() const override;
    static constexpr uint32_t kCoveredWakeups = kSpillSinkCoveredWakeups;
    uint32_t covered_wakeups() const override { return kCoveredWakeups; }

    Status prepare(RuntimeState* state) override;
    Status prepare_local_state(RuntimeState* state) override { return Status::OK(); }
    void close(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    bool spillable() const override { return true; }
    void set_execute_mode(int performance_level) override {
        _spill_strategy = spill::SpillStrategy::SPILL_ALL;
        TRACE_SPILL_LOG << "AggregateDistinctBlockingSink, mark spill " << (void*)this;
    }

    size_t estimated_memory_reserved(const ChunkPtr& chunk) override {
        if (chunk && !chunk->is_empty()) {
            if (_aggregator->hash_set_variant().need_expand(chunk->num_rows())) {
                return chunk->memory_usage() + _aggregator->hash_set_memory_usage();
            }
            return chunk->memory_usage();
        }
        return 0;
    }

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

    // only the prepare/open phase calls are valid.
    SpillProcessChannelPtr spill_channel() { return _aggregator->spill_channel(); }

private:
    // Spiller/spill-channel pair that the SpillableFlatSinkMixin reads to compute
    // need_input()/block_reason(): this sink reaches them through its aggregator. Defined
    // out-of-line where Aggregator is complete.
    const std::shared_ptr<spill::Spiller>& _spiller() const;
    SpillProcessChannelPtr _spill_channel() const;

    Status _spill_all_inputs(RuntimeState* state, const ChunkPtr& chunk);
    Status _spill_aggregated_data(RuntimeState* state);

    std::function<StatusOr<ChunkPtr>()> _build_spill_task(RuntimeState* state);
    spill::SpillStrategy _spill_strategy = spill::SpillStrategy::NO_SPILL;
    DECLARE_ONCE_DETECTOR(_set_finishing_once);
    bool _is_finished = false;
};

static_assert(SpillableAggregateDistinctBlockingSinkOperator::kCoveredWakeups &
              block_reason_bit(BlockReason::WAIT_FLUSH));
static_assert(SpillableAggregateDistinctBlockingSinkOperator::kCoveredWakeups &
              block_reason_bit(BlockReason::WAIT_CHANNEL));

class SpillableAggregateDistinctBlockingSinkOperatorFactory final : public OperatorFactory {
public:
    SpillableAggregateDistinctBlockingSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                                          AggregatorFactoryPtr aggregator_factory,
                                                          SpillProcessChannelFactoryPtr spill_channel_factory)
            : OperatorFactory(id, "spillable_aggregate_distinct_blocking_sink", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)),
              _spill_channel_factory(std::move(spill_channel_factory)) {}

    ~SpillableAggregateDistinctBlockingSinkOperatorFactory() override = default;

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
    AggregatorFactoryPtr _aggregator_factory;
    SpillProcessChannelFactoryPtr _spill_channel_factory;
};

class SpillableAggregateDistinctBlockingSourceOperator
        : public AggregateDistinctBlockingSourceOperator,
          public SpillableSimpleRestoreSourceMixin<SpillableAggregateDistinctBlockingSourceOperator> {
    friend class SpillableSimpleRestoreSourceMixin<SpillableAggregateDistinctBlockingSourceOperator>;

public:
    template <class... Args>
    SpillableAggregateDistinctBlockingSourceOperator(AggregatorPtr aggregator,
                                                     SortedStreamingAggregatorPtr stream_aggregator, Args&&... args)
            : AggregateDistinctBlockingSourceOperator(std::move(aggregator), std::forward<Args>(args)...,
                                                      "spillable_aggregate_distinct_blocking_source"),
              _stream_aggregator(std::move(stream_aggregator)) {}

    ~SpillableAggregateDistinctBlockingSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override;

    // The source sleeps INPUT_EMPTY on has_output(): when spilled and no restored chunk is buffered yet it
    // waits on restore IO, woken by the restore-completion defer on the spiller source list. covered_wakeups()
    // declares the one reason the prepare() source-list subscription covers.
    BlockReason block_reason() const override;
    static constexpr uint32_t kCoveredWakeups = kSpillSourceRestoreCoveredWakeups;
    uint32_t covered_wakeups() const override { return kCoveredWakeups; }

    Status set_finished(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    StatusOr<ChunkPtr> _pull_spilled_chunk(RuntimeState* state);

    // The spiller that the SpillableSimpleRestoreSourceMixin predicates read: this source reaches it
    // through its aggregator. spill::Spiller stays forward-declared here; only the shared_ptr ref crosses.
    const std::shared_ptr<spill::Spiller>& _spiller() const { return _aggregator->spiller(); }

    // The parking-reason predicate for has_output()'s spilled branch. Returns WAIT_RESTORE when the source
    // is parked waiting on restore IO, or nullopt on every branch where has_output() reports work is ready
    // or the source is not parked on a spill block. Distinct's has_output() is not computed directly from
    // this (it leads with the base AggregateDistinctBlockingSourceOperator::has_output() check
    // unconditionally), so _blocked_on()'s runnable disjunction matches every has_output() == true branch --
    // the base ready check, buffered accumulator/spiller output, a failed spiller, the cancel branch, and
    // the eos chunk -- and only the genuine restore park returns WAIT_RESTORE. block_reason() is
    // value_or(NONE), read only when has_output() == false.
    std::optional<BlockReason> _blocked_on() const;

    bool _is_finished = false;
    bool _has_last_chunk = true;
    ChunkPipelineAccumulator _accumulator;
    SortedStreamingAggregatorPtr _stream_aggregator = nullptr;
};

static_assert(SpillableAggregateDistinctBlockingSourceOperator::kCoveredWakeups &
              block_reason_bit(BlockReason::WAIT_RESTORE));

class SpillableAggregateDistinctBlockingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    SpillableAggregateDistinctBlockingSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                            AggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "spillable_aggregate_distinct_blocking_source", plan_node_id),
              _hash_aggregator_factory(std::move(aggregator_factory)) {}

    ~SpillableAggregateDistinctBlockingSourceOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    // Event-scheduler opt-in, gated on enable_spill_agg_events (default false; kill-switch demotes the agg
    // fragment to the poller without a rebuild).
    bool support_event_scheduler() const override { return config::enable_spill_agg_events; }

private:
    AggregatorFactoryPtr _hash_aggregator_factory;
    // _stream_aggregatory_factory is only used when spilling happens
    StreamingAggregatorFactoryPtr _stream_aggregator_factory;
};

} // namespace starrocks::pipeline
