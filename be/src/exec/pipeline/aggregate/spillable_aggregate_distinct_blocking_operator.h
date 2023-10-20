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

#include <utility>

#include "exec/aggregator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_blocking_source_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/source_operator.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {
// TODO: implements reset_state
class SpillableAggregateDistinctBlockingSinkOperator : public AggregateDistinctBlockingSinkOperator {
public:
    template <class... Args>
    SpillableAggregateDistinctBlockingSinkOperator(AggregatorPtr aggregator, Args&&... args)
            : AggregateDistinctBlockingSinkOperator(aggregator, std::forward<Args>(args)...,
                                                    "spillable_aggregate_distinct_blocking_sink") {}
    ~SpillableAggregateDistinctBlockingSinkOperator() override = default;

    bool need_input() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
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

private:
    Status _spill_all_inputs(RuntimeState* state, const ChunkPtr& chunk);
    Status _spill_aggregated_data(RuntimeState* state);

    std::function<StatusOr<ChunkPtr>()> _build_spill_task(RuntimeState* state);

    spill::SpillStrategy _spill_strategy = spill::SpillStrategy::NO_SPILL;
    bool _is_finished = false;
};

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

private:
    ObjectPool _pool;
    SortExecExprs _sort_exprs;
    SortDescs _sort_desc;

    std::shared_ptr<spill::SpilledOptions> _spill_options;
    std::shared_ptr<spill::SpillerFactory> _spill_factory = std::make_shared<spill::SpillerFactory>();
    AggregatorFactoryPtr _aggregator_factory;
    SpillProcessChannelFactoryPtr _spill_channel_factory;
};

// TODO: implements reset_state
class SpillableAggregateDistinctBlockingSourceOperator : public AggregateDistinctBlockingSourceOperator {
public:
    template <class... Args>
    SpillableAggregateDistinctBlockingSourceOperator(AggregatorPtr aggregator,
                                                     SortedStreamingAggregatorPtr stream_aggregator, Args&&... args)
            : AggregateDistinctBlockingSourceOperator(aggregator, std::forward<Args>(args)...,
                                                      "spillable_aggregate_distinct_blocking_source"),
              _stream_aggregator(std::move(stream_aggregator)) {}

    ~SpillableAggregateDistinctBlockingSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override;

    Status set_finished(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    [[nodiscard]] StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
private:
    StatusOr<ChunkPtr> _pull_spilled_chunk(RuntimeState* state);

    bool _is_finished = false;
    bool _has_last_chunk = true;
    ChunkPipelineAccumulator _accumulator;
    SortedStreamingAggregatorPtr _stream_aggregator = nullptr;
};

class SpillableAggregateDistinctBlockingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    SpillableAggregateDistinctBlockingSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                            AggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "spillable_aggregate_distinct_blocking_source", plan_node_id),
              _hash_aggregator_factory(std::move(aggregator_factory)) {}

    ~SpillableAggregateDistinctBlockingSourceOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    AggregatorFactoryPtr _hash_aggregator_factory;
    // _stream_aggregatory_factory is only used when spilling happens
    StreamingAggregatorFactoryPtr _stream_aggregator_factory;
};

} // namespace starrocks::pipeline
