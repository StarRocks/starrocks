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

#include "common/config_exec_flow_fwd.h"
#include "exec/aggregator_fwd.h"
#include "exec/pipeline/aggregate/aggregate_blocking_source_operator.h"
#include "exec/pipeline/primitives/spillable_simple_source_mixin.h"
#include "runtime/runtime_state_fwd.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {
class SpillableAggregateBlockingSourceOperator final
        : public AggregateBlockingSourceOperator,
          public SpillableSimpleRestoreSourceMixin<SpillableAggregateBlockingSourceOperator> {
    friend class SpillableSimpleRestoreSourceMixin<SpillableAggregateBlockingSourceOperator>;

public:
    template <class... Args>
    SpillableAggregateBlockingSourceOperator(const AggregatorPtr& aggregator,
                                             SortedStreamingAggregatorPtr stream_aggregator, Args&&... args)
            : AggregateBlockingSourceOperator(aggregator, std::forward<Args>(args)...,
                                              "spillable_aggregate_blocking_source"),
              _stream_aggregator(std::move(stream_aggregator)) {}

    ~SpillableAggregateBlockingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;

    // The source sleeps INPUT_EMPTY on has_output(): when spilled and no restored chunk is buffered yet
    // it waits on restore IO, woken by the restore-completion defer on the spiller source list. covered_wakeups()
    // declares the one reason the prepare() source-list subscription covers.
    BlockReason block_reason() const override;
    static constexpr uint32_t kCoveredWakeups = kSpillSourceRestoreCoveredWakeups;
    uint32_t covered_wakeups() const override { return kCoveredWakeups; }

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    StatusOr<ChunkPtr> _pull_spilled_chunk(RuntimeState* state);

    // The spiller that the SpillableSimpleRestoreSourceMixin predicates read: this source reaches it
    // through its aggregator. spill::Spiller stays forward-declared here; only the shared_ptr ref crosses.
    const std::shared_ptr<spill::Spiller>& _spiller() const { return _aggregator->spiller(); }

    // The single branching predicate that both has_output()'s spilled branch and block_reason() read. It
    // returns WAIT_RESTORE when the source is parked waiting on restore IO (spilled, status ok, no buffered
    // accumulator output and no spiller output data yet), or nullopt when it is runnable or not parked on a
    // spill block -- a finished or non-spilled source, a failed spiller (runnable via the
    // RETURN_TRUE_IF_SPILL_TASK_ERROR guard), or any buffered/restored output ready to pull. block_reason()
    // is value_or(NONE); it is read only on the parked branch, where it matches has_output()'s spilled branch.
    std::optional<BlockReason> _blocked_on() const;

    bool _is_finished = false;
    bool _has_last_chunk = true;
    ChunkPipelineAccumulator _accumulator;
    SortedStreamingAggregatorPtr _stream_aggregator = nullptr;
};

static_assert(SpillableAggregateBlockingSourceOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_RESTORE));

class SpillableAggregateBlockingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    SpillableAggregateBlockingSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                    AggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "spillable_aggregate_blocking_source", plan_node_id),
              _hash_aggregator_factory(std::move(aggregator_factory)) {}

    ~SpillableAggregateBlockingSourceOperatorFactory() override = default;

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
