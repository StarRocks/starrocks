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

#include <atomic>
#include <unordered_map>
#include <utility>

#include "exec/aggregator.h"
#include "exec/pipeline/operator.h"
#include "runtime/runtime_state.h"
#include "util/race_detect.h"

namespace starrocks::pipeline {
class AggregateBlockingSinkOperatorFactory;
class AggregateBlockingSinkOperator : public Operator {
public:
    AggregateBlockingSinkOperator(AggregatorPtr aggregator, OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                  int32_t driver_sequence, std::atomic<int64_t>& shared_limit_countdown,
                                  const char* name = "aggregate_blocking_sink")
            : Operator(factory, id, name, plan_node_id, false, driver_sequence),
              _aggregator(std::move(aggregator)),
              _shared_limit_countdown(shared_limit_countdown) {
        _aggregator->set_aggr_phase(AggrPhase2);
        _aggregator->ref();
        _object_pool = std::make_unique<ObjectPool>();
    }

    ~AggregateBlockingSinkOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override { return !is_finished(); }
    bool is_finished() const override { return _is_finished || _aggregator->is_finished(); }
    Status set_finishing(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status prepare_local_state(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;
    AggregatorPtr& aggregator() { return _aggregator; }
    void set_agg_group_by_with_limit(bool v) { _agg_group_by_with_limit = v; }

protected:
    AggregateBlockingSinkOperatorFactory* factory() {
        return down_cast<AggregateBlockingSinkOperatorFactory*>(_factory);
    }
    void _build_in_runtime_filters(RuntimeState* state);

    DECLARE_ONCE_DETECTOR(_set_finishing_once);
    // It is used to perform aggregation algorithms shared by
    // AggregateBlockingSourceOperator. It is
    // - prepared at SinkOperator::prepare(),
    // - reffed at constructor() of both sink and source operator,
    // - unreffed at close() of both sink and source operator.
    AggregatorPtr _aggregator = nullptr;
    bool _agg_group_by_with_limit = false;

    std::vector<RuntimeFilter*> _runtime_filters;
    std::unique_ptr<ObjectPool> _object_pool = nullptr;

private:
    // Whether prev operator has no output
    std::atomic_bool _is_finished = false;
    bool _in_runtime_filter_built = false;
    // whether enable aggregate group by limit optimize
    std::atomic<int64_t>& _shared_limit_countdown;
};

class AggregateBlockingSinkOperatorFactory : public OperatorFactory {
public:
    AggregateBlockingSinkOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorFactoryPtr aggregator_factory,
                                         const std::vector<RuntimeFilterBuildDescriptor*>& build_runtime_filters,
                                         const SpillProcessChannelFactoryPtr& _)
            : OperatorFactory(id, "aggregate_blocking_sink", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)),
              _build_runtime_filters(build_runtime_filters) {}
    AggregateBlockingSinkOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorFactoryPtr aggregator_factory,
                                         const std::vector<RuntimeFilterBuildDescriptor*>& build_runtime_filters,
                                         const char* name)
            : OperatorFactory(id, name, plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)),
              _build_runtime_filters(build_runtime_filters) {}

    ~AggregateBlockingSinkOperatorFactory() override = default;

    const std::vector<RuntimeFilterBuildDescriptor*>& build_runtime_filters() { return _build_runtime_filters; }
    AggInRuntimeFilterMerger* in_filter_merger(size_t filter_id) const {
        return _in_filter_mergers.at(filter_id).get();
    }

    bool support_event_scheduler() const override { return true; }

    Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    AggregatorFactoryPtr& aggregator_factory() { return _aggregator_factory; }

    void set_runtime_filter_collector(RuntimeFilterHub* hub, int32_t plan_node_id,
                                      std::unique_ptr<RuntimeFilterCollector>&& collector);

protected:
    AggregatorFactoryPtr _aggregator_factory;
    std::once_flag _set_collector_flag;
    std::unordered_map<size_t, std::shared_ptr<AggInRuntimeFilterMerger>> _in_filter_mergers;
    const std::vector<RuntimeFilterBuildDescriptor*>& _build_runtime_filters;
};
} // namespace starrocks::pipeline
