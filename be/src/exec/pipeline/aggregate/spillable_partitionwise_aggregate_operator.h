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
#include <optional>

#include "common/config_exec_flow_fwd.h"
#include "compute_env/spill/spill_components.h"
#include "exec/aggregator.h"
#include "exec/pipeline/aggregate/aggregate_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_blocking_source_operator.h"
#include "exec/pipeline/primitives/block_reason.h"
#include "exec/pipeline/primitives/spillable_flat_sink_mixin.h"
#include "exec/pipeline/primitives/spillable_partitionwise_source_mixin.h"
#include "exec/pipeline/source_operator.h"
#include "exec/query_cache/conjugate_operator.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {
class SpillablePartitionWiseAggregateSourceOperator;
class SpillablePartitionWiseAggregateSourceOperatorFactory;

using SPWAggregateSourceOperatorRawPtr = SpillablePartitionWiseAggregateSourceOperator*;
using SPWAggregateSourceOperatorPtr = std::shared_ptr<SPWAggregateSourceOperatorRawPtr>;
using SPWAggregateSourceOperatorFactoryRawPtr = SpillablePartitionWiseAggregateSourceOperatorFactory*;
using SPWAggregateSourceOperatorFactoryPtr = std::shared_ptr<SPWAggregateSourceOperatorFactoryRawPtr>;
using AggregateBlockingSourceOperatorPtr = std::shared_ptr<AggregateBlockingSourceOperator>;
using ConjugateOperatorPtr = starrocks::query_cache::ConjugateOperatorPtr;
using AggregateBlockingSourceOperatorFactoryPtr = std::shared_ptr<AggregateBlockingSourceOperatorFactory>;
using ConjugateOperatorFactoryPtr = starrocks::query_cache::ConjugateOperatorFactoryPtr;

using AggregateBlockingSinkOperatorPtr = std::shared_ptr<AggregateBlockingSinkOperator>;
using AggregateBlockingSinkOperatorFactoryPtr = std::shared_ptr<AggregateBlockingSinkOperatorFactory>;
class SpillablePartitionWiseAggregateSinkOperator;
class SpillablePartitionWiseAggregateSinkOperatorFactory;
using SPWAggregateSinkOperatorRawPtr = SpillablePartitionWiseAggregateSinkOperator*;
using SPWAggregateSinkOperatorPtr = std::shared_ptr<SpillablePartitionWiseAggregateSinkOperator>;
using SPWAggregateSinkOperatorFactoryRawPtr = SpillablePartitionWiseAggregateSinkOperatorFactory*;
using SPWAggregateSinkOperatorFactoryPtr = std::shared_ptr<SpillablePartitionWiseAggregateSinkOperatorFactory>;

class SpillablePartitionWiseAggregateSourceOperator final
        : public SourceOperator,
          public SpillablePartitionWiseSourceMixin<SpillablePartitionWiseAggregateSourceOperator> {
    friend class SpillablePartitionWiseSourceMixin<SpillablePartitionWiseAggregateSourceOperator>;

public:
    SpillablePartitionWiseAggregateSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                  int32_t driver_sequence,
                                                  AggregateBlockingSourceOperatorPtr non_pw_agg,
                                                  ConjugateOperatorPtr pw_agg)
            : SourceOperator(factory, id, "spillable_partitionwise_agg_source", plan_node_id, false, driver_sequence),
              _non_pw_agg(std::move(non_pw_agg)),
              _pw_agg(std::move(pw_agg)) {}

    ~SpillablePartitionWiseAggregateSourceOperator() override = default;

    bool has_output() const override { return spill_pw_source_has_output(); }
    bool is_finished() const override { return spill_pw_source_is_finished(); }

    // The source sleeps INPUT_EMPTY on has_output() on two axes (WAIT_FLUSH while the sink drains its final
    // flush, WAIT_RESTORE while a per-partition restore is in flight); the predicate and its rationale live
    // in SpillablePartitionWiseSourceMixin. covered_wakeups() declares both reasons.
    BlockReason block_reason() const override { return spill_pw_source_block_reason(); }
    static constexpr uint32_t kCoveredWakeups = kSpillPwSourceCoveredWakeups;
    uint32_t covered_wakeups() const override { return kCoveredWakeups; }

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status prepare_local_state(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    AggregateBlockingSourceOperatorPtr _non_pw_agg;
    ConjugateOperatorPtr _pw_agg;
    StatusOr<ChunkPtr> _pull_spilled_chunk(RuntimeState* state);

    // Return the parent spiller and the wrapped-op booleans that the SpillablePartitionWiseSourceMixin
    // predicates read, on this operator's own path. Defined out-of-line.
    const std::shared_ptr<spill::Spiller>& _spiller() const;
    bool _sink_complete() const;
    bool _conjugate_finished() const;
    bool _non_pw_has_output() const;
    bool _non_pw_finished() const;

    std::vector<const SpillPartitionInfo*> _partitions;
    size_t _curr_partition_idx{0};
    std::shared_ptr<spill::SpillerReader> _curr_partition_reader;
    bool _curr_partition_eos{false};
    bool _is_finished{false};
};

static_assert(SpillablePartitionWiseAggregateSourceOperator::kCoveredWakeups &
              block_reason_bit(BlockReason::WAIT_FLUSH));
static_assert(SpillablePartitionWiseAggregateSourceOperator::kCoveredWakeups &
              block_reason_bit(BlockReason::WAIT_RESTORE));

class SpillablePartitionWiseAggregateSourceOperatorFactory final : public SourceOperatorFactory {
public:
    SpillablePartitionWiseAggregateSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                         AggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "spillable_partitionwise_agg_source", plan_node_id),
              _non_pw_agg_factory(std::make_shared<AggregateBlockingSourceOperatorFactory>(
                      id, plan_node_id, std::move(aggregator_factory))) {}

    ~SpillablePartitionWiseAggregateSourceOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    // Event-scheduler opt-in, gated on enable_spill_agg_events (default false; kill-switch demotes the agg
    // fragment to the poller without a rebuild).
    bool support_event_scheduler() const override { return config::enable_spill_agg_events; }

    void set_pw_agg_factory(ConjugateOperatorFactoryPtr&& pw_agg_factory) {
        _pw_agg_factory = std::move(pw_agg_factory);
    }

private:
    AggregateBlockingSourceOperatorFactoryPtr _non_pw_agg_factory;
    ConjugateOperatorFactoryPtr _pw_agg_factory;
};

class SpillablePartitionWiseAggregateSinkOperator
        : public Operator,
          public SpillableFlatSinkMixin<SpillablePartitionWiseAggregateSinkOperator> {
    friend class SpillableFlatSinkMixin<SpillablePartitionWiseAggregateSinkOperator>;

public:
    SpillablePartitionWiseAggregateSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                int32_t driver_sequence, AggregateBlockingSinkOperatorPtr&& agg_op)
            : Operator(factory, id, "spillable_partitionwise_agg_sink", plan_node_id, false, driver_sequence),
              _agg_op(std::move(agg_op)) {}

    ~SpillablePartitionWiseAggregateSinkOperator() override = default;

    bool need_input() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;

    // The sink sleeps OUTPUT_FULL on need_input(): a queued spill-process task (WAIT_CHANNEL) or a full
    // writer (WAIT_FLUSH). covered_wakeups() declares both reasons the prepare() sink-list subscription on
    // the wrapped agg-op's spiller covers.
    BlockReason block_reason() const override;
    static constexpr uint32_t kCoveredWakeups = kSpillSinkCoveredWakeups;
    uint32_t covered_wakeups() const override { return kCoveredWakeups; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return Status::InternalError("Not support"); }

    bool has_output() const override { throw Status::InternalError("Not support"); }

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
            if (_agg_op->aggregator()->hash_map_variant().need_expand(chunk->num_rows())) {
                return chunk->memory_usage() + _agg_op->aggregator()->hash_map_memory_usage();
            }
            return chunk->memory_usage();
        }
        return 0;
    }

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

    // only the prepare/open phase calls are valid.
    SpillProcessChannelPtr spill_channel() { return _agg_op->aggregator()->spill_channel(); }

private:
    bool spilled() const { return _agg_op->aggregator()->spiller()->spilled(); }

    // Spiller/spill-channel pair that the SpillableFlatSinkMixin reads to compute
    // need_input()/block_reason(): reached through the wrapped agg-op's aggregator. Defined
    // out-of-line.
    const std::shared_ptr<spill::Spiller>& _spiller() const;
    SpillProcessChannelPtr _spill_channel() const;

private:
    Status _try_to_spill_by_force(RuntimeState* state, const ChunkPtr& chunk);

    Status _try_to_spill_by_auto(RuntimeState* state, const ChunkPtr& chunk);

    Status _spill_all_data(RuntimeState* state, bool should_spill_hash_table);

    void _add_streaming_chunk(ChunkPtr chunk);

    ChunkPtr& _append_hash_column(ChunkPtr& chunk);

    AggregateBlockingSinkOperatorPtr _agg_op;
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

static_assert(SpillablePartitionWiseAggregateSinkOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_FLUSH));
static_assert(SpillablePartitionWiseAggregateSinkOperator::kCoveredWakeups &
              block_reason_bit(BlockReason::WAIT_CHANNEL));

class SpillablePartitionWiseAggregateSinkOperatorFactory : public OperatorFactory {
public:
    SpillablePartitionWiseAggregateSinkOperatorFactory(
            int32_t id, int32_t plan_node_id, AggregatorFactoryPtr aggregator_factory,
            const std::vector<RuntimeFilterBuildDescriptor*>& build_runtime_filters,
            SpillProcessChannelFactoryPtr spill_channel_factory)
            : OperatorFactory(id, "spillable_partitionwise_agg_sink", plan_node_id),
              _agg_op_factory(std::make_shared<AggregateBlockingSinkOperatorFactory>(
                      id, plan_node_id, std::move(aggregator_factory), build_runtime_filters, spill_channel_factory)),
              _spill_channel_factory(std::move(spill_channel_factory)) {}

    ~SpillablePartitionWiseAggregateSinkOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    // Event-scheduler opt-in, gated on enable_spill_agg_events (default false; kill-switch demotes the agg
    // fragment to the poller without a rebuild).
    bool support_event_scheduler() const override { return config::enable_spill_agg_events; }

private:
    ObjectPool _pool;
    std::shared_ptr<spill::SpilledOptions> _spill_options;
    std::shared_ptr<spill::SpillerFactory> _spill_factory = std::make_shared<spill::SpillerFactory>();
    AggregateBlockingSinkOperatorFactoryPtr _agg_op_factory;
    SpillProcessChannelFactoryPtr _spill_channel_factory;
};

} // namespace starrocks::pipeline
