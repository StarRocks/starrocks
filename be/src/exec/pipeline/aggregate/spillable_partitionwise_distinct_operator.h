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
// Copyright 2021-present StarRocks, Inc. All rights reserved.

#pragma once
#include <memory>

#include "exec/aggregator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_blocking_source_operator.h"
#include "exec/pipeline/source_operator.h"
#include "exec/query_cache/conjugate_operator.h"
#include "exec/spill/spill_components.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {
class SpillablePartitionWiseDistinctSourceOperator;
class SpillablePartitionWiseDistinctSourceOperatorFactory;

using SPWDistinctSourceOperatorRawPtr = SpillablePartitionWiseDistinctSourceOperator*;
using SPWDistinctSourceOperatorPtr = std::shared_ptr<SPWDistinctSourceOperatorRawPtr>;
using SPWDistinctSourceOperatorFactoryRawPtr = SpillablePartitionWiseDistinctSourceOperatorFactory*;
using SPWDistinctSourceOperatorFactoryPtr = std::shared_ptr<SPWDistinctSourceOperatorFactoryRawPtr>;
using DistinctSourceOperatorPtr = std::shared_ptr<AggregateDistinctBlockingSourceOperator>;
using ConjugateOperatorPtr = starrocks::query_cache::ConjugateOperatorPtr;
using DistinctSourceOperatorFactoryPtr = std::shared_ptr<AggregateDistinctBlockingSourceOperatorFactory>;
using ConjugateOperatorFactoryPtr = starrocks::query_cache::ConjugateOperatorFactoryPtr;

using DistinctSinkOperatorPtr = std::shared_ptr<AggregateDistinctBlockingSinkOperator>;
using DistinctSinkOperatorFactoryPtr = std::shared_ptr<AggregateDistinctBlockingSinkOperatorFactory>;
class SpillablePartitionWiseDistinctSinkOperator;
class SpillablePartitionWiseDistinctSinkOperatorFactory;
using SPWDistinctSinkOperatorRawPtr = SpillablePartitionWiseDistinctSinkOperator*;
using SPWDistinctSinkOperatorPtr = std::shared_ptr<SpillablePartitionWiseDistinctSinkOperator>;
using SPWDistinctSinkOperatorFactoryRawPtr = SpillablePartitionWiseDistinctSinkOperatorFactory*;
using SPWDistinctSinkOperatorFactoryPtr = std::shared_ptr<SpillablePartitionWiseDistinctSinkOperatorFactory>;

class SpillablePartitionWiseDistinctSourceOperator final : public SourceOperator {
public:
    SpillablePartitionWiseDistinctSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                 int32_t driver_sequence, const DistinctSourceOperatorPtr non_pw_agg,
                                                 ConjugateOperatorPtr pw_agg)
            : SourceOperator(factory, id, "spillable_partitionwise_distinct_source", plan_node_id, false,
                             driver_sequence),
              _non_pw_distinct(std::move(non_pw_agg)),
              _pw_distinct(std::move(pw_agg)) {}

    ~SpillablePartitionWiseDistinctSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status prepare_local_state(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    DistinctSourceOperatorPtr _non_pw_distinct;
    ConjugateOperatorPtr _pw_distinct;
    StatusOr<ChunkPtr> _pull_spilled_chunk(RuntimeState* state);

    std::vector<const SpillPartitionInfo*> _partitions;
    size_t _curr_partition_idx{0};
    std::shared_ptr<spill::SpillerReader> _curr_partition_reader;
    bool _curr_partition_eos{false};
    bool _is_finished{false};
};

class SpillablePartitionWiseDistinctSourceOperatorFactory final : public SourceOperatorFactory {
public:
    SpillablePartitionWiseDistinctSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                        AggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "spillable_partitionwise_distinct_source", plan_node_id),
              _non_pw_distinct_factory(std::make_shared<AggregateDistinctBlockingSourceOperatorFactory>(
                      id, plan_node_id, std::move(aggregator_factory))) {}

    ~SpillablePartitionWiseDistinctSourceOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    bool support_event_scheduler() const override { return false; }

    void set_pw_distinct_factory(ConjugateOperatorFactoryPtr&& pw_agg_factory) {
        _pw_distinct_factory = std::move(pw_agg_factory);
    }

private:
    DistinctSourceOperatorFactoryPtr _non_pw_distinct_factory;
    ConjugateOperatorFactoryPtr _pw_distinct_factory;
};

class SpillablePartitionWiseDistinctSinkOperator : public Operator {
public:
    SpillablePartitionWiseDistinctSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                               int32_t driver_sequence, DistinctSinkOperatorPtr&& agg_op)
            : Operator(factory, id, "spillable_partitionwise_distinct_sink", plan_node_id, false, driver_sequence),
              _distinct_op(std::move(agg_op)) {}

    ~SpillablePartitionWiseDistinctSinkOperator() override = default;

    bool need_input() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return Status::InternalError("Not support"); }

    bool has_output() const override { throw Status::InternalError("Not support"); }

    void close(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status prepare_local_state(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    bool spillable() const override { return true; }

    void set_execute_mode(int performance_level) override {
        _spill_strategy = spill::SpillStrategy::SPILL_ALL;
        TRACE_SPILL_LOG << "AggregateDistinctBlockingSink, mark spill " << (void*)this;
    }

    size_t estimated_memory_reserved(const ChunkPtr& chunk) override {
        if (chunk && !chunk->is_empty()) {
            if (_distinct_op->aggregator()->hash_set_variant().need_expand(chunk->num_rows())) {
                return chunk->memory_usage() + _distinct_op->aggregator()->hash_set_memory_usage();
            }
            return chunk->memory_usage();
        }
        return 0;
    }

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

    // only the prepare/open phase calls are valid.
    SpillProcessChannelPtr spill_channel() { return _distinct_op->aggregator()->spill_channel(); }

private:
    bool spilled() const { return _distinct_op->aggregator()->spiller()->spilled(); }

private:
    Status _spill_all_data(RuntimeState* state);

    ChunkPtr& _append_hash_column(ChunkPtr& chunk);

    DistinctSinkOperatorPtr _distinct_op;
    std::function<StatusOr<ChunkPtr>()> _build_spill_task(RuntimeState* state);

    DECLARE_ONCE_DETECTOR(_set_finishing_once);
    spill::SpillStrategy _spill_strategy = spill::SpillStrategy::NO_SPILL;

    bool _is_finished = false;

    RuntimeProfile::Counter* _hash_table_spill_times = nullptr;

    static constexpr double HT_LOW_REDUCTION_THRESHOLD = 0.5;
    static constexpr int32_t HT_LOW_REDUCTION_CHUNK_LIMIT = 5;
};

class SpillablePartitionWiseDistinctSinkOperatorFactory : public OperatorFactory {
public:
    SpillablePartitionWiseDistinctSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                                      AggregatorFactoryPtr aggregator_factory,
                                                      SpillProcessChannelFactoryPtr spill_channel_factory)
            : OperatorFactory(id, "spillable_partitionwise_distinct_sink", plan_node_id),
              _distinct_op_factory(std::make_shared<AggregateDistinctBlockingSinkOperatorFactory>(
                      id, plan_node_id, std::move(aggregator_factory), spill_channel_factory)),
              _spill_channel_factory(std::move(spill_channel_factory)) {}

    ~SpillablePartitionWiseDistinctSinkOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    bool support_event_scheduler() const override { return false; }

private:
    ObjectPool _pool;
    std::shared_ptr<spill::SpilledOptions> _spill_options;
    std::shared_ptr<spill::SpillerFactory> _spill_factory = std::make_shared<spill::SpillerFactory>();
    DistinctSinkOperatorFactoryPtr _distinct_op_factory;
    SpillProcessChannelFactoryPtr _spill_channel_factory;
};

} // namespace starrocks::pipeline