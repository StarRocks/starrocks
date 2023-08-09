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
#include "aggregate_blocking_sink_operator.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/aggregator.h"
#include "exec/pipeline/operator.h"
#include "exec/sorted_streaming_aggregator.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
// TODO: implements reset_state
class SpillableAggregateBlockingSinkOperator : public AggregateBlockingSinkOperator {
public:
    template <class... Args>
    SpillableAggregateBlockingSinkOperator(AggregatorPtr aggregator, Args&&... args)
            : AggregateBlockingSinkOperator(aggregator, std::forward<Args>(args)...,
                                            "spillable_aggregate_blocking_sink") {}

    ~SpillableAggregateBlockingSinkOperator() override = default;

    bool need_input() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
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

private:
    bool spilled() const { return _aggregator->spiller()->spilled(); }

private:
    Status _spill_all_inputs(RuntimeState* state, const ChunkPtr& chunk);
    std::function<StatusOr<ChunkPtr>()> _build_spill_task(RuntimeState* state);
    spill::SpillStrategy _spill_strategy = spill::SpillStrategy::NO_SPILL;

    bool _is_finished = false;
};

class SpillableAggregateBlockingSinkOperatorFactory : public OperatorFactory {
public:
    SpillableAggregateBlockingSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                                  AggregatorFactoryPtr aggregator_factory,
                                                  SpillProcessChannelFactoryPtr spill_channel_factory)
            : OperatorFactory(id, "spillable_aggregate_blocking_sink", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)),
              _spill_channel_factory(std::move(spill_channel_factory)) {}

    ~SpillableAggregateBlockingSinkOperatorFactory() override = default;

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

} // namespace starrocks::pipeline