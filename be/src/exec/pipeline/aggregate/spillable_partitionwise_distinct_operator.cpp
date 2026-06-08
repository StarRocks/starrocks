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

#include "exec/pipeline/aggregate/spillable_partitionwise_distinct_operator.h"

#include "base/failpoint/fail_point.h"
#include "compute_env/spill/mem_tracker_guard.h"
#include "compute_env/spill/spiller.h"
#include "exec/pipeline/aggregate/spillable_aggregate_skew_compactor.h"
#include "exec/pipeline/query_context.h"
#include "exec/runtime/fragment_runtime_state.h"
#include "runtime/runtime_state_helper.h"

namespace starrocks::pipeline {

const std::shared_ptr<spill::Spiller>& SpillablePartitionWiseDistinctSinkOperator::_spiller() const {
    return _distinct_op->aggregator()->spiller();
}

SpillProcessChannelPtr SpillablePartitionWiseDistinctSinkOperator::_spill_channel() const {
    return _distinct_op->aggregator()->spill_channel();
}

bool SpillablePartitionWiseDistinctSinkOperator::need_input() const {
    return spill_sink_need_input();
}

BlockReason SpillablePartitionWiseDistinctSinkOperator::block_reason() const {
    return spill_sink_block_reason();
}

bool SpillablePartitionWiseDistinctSinkOperator::is_finished() const {
    if (!spilled()) {
        return _is_finished || _distinct_op->is_finished();
    }
    return _is_finished || _distinct_op->aggregator()->is_finished();
}

Status SpillablePartitionWiseDistinctSinkOperator::set_finishing(RuntimeState* state) {
    if (_is_finished) {
        return Status::OK();
    }
    ONCE_DETECT(_set_finishing_once);
    auto defer_set_finishing = DeferOp([this]() {
        _distinct_op->aggregator()->spill_channel()->set_finishing();
        _is_finished = true;
    });

    // cancel spill task
    if (state->is_cancelled()) {
        _distinct_op->aggregator()->spiller()->cancel();
    }

    if (!_distinct_op->aggregator()->spiller()->spilled()) {
        RETURN_IF_ERROR(_distinct_op->set_finishing(state));
        return Status::OK();
    }

    auto flush_function = [this](RuntimeState* state) {
        auto& spiller = _distinct_op->aggregator()->spiller();
        return spiller->flush(state, TRACKER_WITH_SPILLER_READER_GUARD(state, spiller));
    };

    _distinct_op->aggregator()->ref();
    auto set_call_back_function = [this](RuntimeState* state) {
        return _distinct_op->aggregator()->spiller()->set_flush_all_call_back(
                [this, state]() {
                    auto defer = DeferOp([&]() { _distinct_op->aggregator()->unref(state); });
                    RETURN_IF_ERROR(_distinct_op->set_finishing(state));
                    return Status::OK();
                },
                state, TRACKER_WITH_SPILLER_READER_GUARD(state, _distinct_op->aggregator()->spiller()));
    };

    SpillProcessTasksBuilder task_builder(state);
    task_builder.then(flush_function).finally(set_call_back_function);

    RETURN_IF_ERROR(_distinct_op->aggregator()->spill_channel()->execute(task_builder));

    return Status::OK();
}

void SpillablePartitionWiseDistinctSinkOperator::close(RuntimeState* state) {
    _distinct_op->close(state);
    Operator::close(state);
}

Status SpillablePartitionWiseDistinctSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(Operator::prepare_local_state(state));
    // Propagate this wrapper's observer into the wrapped distinct-op before its prepare(): the base
    // AggregateDistinctBlockingSinkOperator::prepare attaches the operator's _observer to the aggregator-pip
    // observable, and assign_observer only set _observer on the wrapper, not the sub-op. Without this the
    // sub-op would attach nullptr and notify_source/sink would null-deref under the event scheduler (crash
    // in release, not a silent no-op). Mirrors ConjugateOperator::prepare.
    _distinct_op->set_observer(observer());
    RETURN_IF_ERROR(_distinct_op->prepare(state));
    RETURN_IF_ERROR(_distinct_op->prepare_local_state(state));
    DCHECK(!_distinct_op->aggregator()->is_none_group_by_exprs());
    _distinct_op->aggregator()->spiller()->set_metrics(
            spill::SpillProcessMetrics(_unique_metrics.get(), RuntimeStateHelper::mutable_total_spill_bytes(state)));

    if (state->spill_mode() == TSpillMode::FORCE) {
        _spill_strategy = spill::SpillStrategy::SPILL_ALL;
    }
    _peak_revocable_mem_bytes = _unique_metrics->AddHighWaterMarkCounter(
            "PeakRevocableMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    _hash_table_spill_times = ADD_COUNTER(_unique_metrics.get(), "HashTableSpillTimes", TUnit::UNIT);

    // Subscribe this sink driver to the wrapped distinct-op spiller's sink list so flush/channel completions
    // wake the OUTPUT_FULL sleeper. Unconditional: the poller-mode gate lives inside subscribe_sink.
    _distinct_op->aggregator()->spiller()->observable().subscribe_sink(state, observer());

    return Status::OK();
}

Status SpillablePartitionWiseDistinctSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_distinct_op->push_chunk(state, chunk));
    // direct return if is_finished. (hash set reach limit)
    if (_distinct_op->is_finished()) return Status::OK();
    set_revocable_mem_bytes(_distinct_op->aggregator()->hash_set_memory_usage());
    if (_spill_strategy == spill::SpillStrategy::SPILL_ALL) {
        return _spill_all_data(state);
    }
    return Status::OK();
}

Status SpillablePartitionWiseDistinctSinkOperator::_spill_all_data(RuntimeState* state) {
    auto& aggregator = _distinct_op->aggregator();
    RETURN_IF(aggregator->hash_set_variant().size() == 0, Status::OK());
    aggregator->hash_set_variant().visit(
            [&](auto& hash_set_with_key) { aggregator->it_hash() = hash_set_with_key->hash_set.begin(); });
    CHECK(!aggregator->spill_channel()->has_task());
    RETURN_IF_ERROR(aggregator->spill_aggregate_data(state, _build_spill_task(state)));
    return Status::OK();
}

Status SpillablePartitionWiseDistinctSinkOperator::reset_state(RuntimeState* state,
                                                               const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    ONCE_RESET(_set_finishing_once);
    RETURN_IF_ERROR(_distinct_op->aggregator()->spiller()->reset_state(state));
    RETURN_IF_ERROR(_distinct_op->reset_state(state, refill_chunks));
    return Status::OK();
}

ChunkPtr& SpillablePartitionWiseDistinctSinkOperator::_append_hash_column(ChunkPtr& chunk) {
    const auto& group_by_exprs = _distinct_op->aggregator()->group_by_expr_ctxs();
    size_t num_rows = chunk->num_rows();
    auto hash_column = spill::SpillHashColumn::create(num_rows, HashUtil::FNV_SEED);
    auto& hash_values = hash_column->get_data();
    // TODO: use different hash method
    for (auto* expr : group_by_exprs) {
        auto slot_id = down_cast<const ColumnRef*>(expr->root())->slot_id();
        auto column = chunk->get_column_by_slot_id(slot_id);
        column->fnv_hash(hash_values.data(), 0, num_rows);
    }
    chunk->append_column(std::move(hash_column), Chunk::HASH_AGG_SPILL_HASH_SLOT_ID);
    return chunk;
}

std::function<StatusOr<ChunkPtr>()> SpillablePartitionWiseDistinctSinkOperator::_build_spill_task(RuntimeState* state) {
    auto chunk_provider = [this, state]() -> StatusOr<ChunkPtr> {
        auto& aggregator = _distinct_op->aggregator();
        if (!aggregator->is_ht_eos()) {
            auto chunk = std::make_shared<Chunk>();
            aggregator->convert_hash_set_to_chunk(state->chunk_size(), &chunk);
            return chunk;
        }
        RETURN_IF_ERROR(aggregator->reset_state(state, {}, nullptr));
        return Status::EndOfFile("no more data in current aggregator");
    };
    return [this, chunk_provider]() -> StatusOr<ChunkPtr> {
        auto maybe_chunk = chunk_provider();
        if (maybe_chunk.ok()) {
            auto chunk = std::move(maybe_chunk.value());
            if (!chunk) {
                return chunk;
            }
            return this->_append_hash_column(chunk);
        }
        return maybe_chunk;
    };
}

Status SpillablePartitionWiseDistinctSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    // init spill options
    _spill_options = std::make_shared<spill::SpilledOptions>(state->spill_partitionwise_agg_partition_num());
    _spill_options->spill_mem_table_bytes_size = state->spill_mem_table_size();
    _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    _spill_options->spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    _spill_options->block_manager = state->query_runtime_state()->query_spill_manager()->block_manager();
    _spill_options->name = "distinct-blocking-spill";
    _spill_options->splittable = false;
    _spill_options->enable_block_compaction = state->spill_enable_compaction();
    _spill_options->plan_node_id = _plan_node_id;
    _spill_options->encode_level = state->spill_encode_level();
    _spill_options->wg = state->fragment_runtime_state()->workgroup();
    _spill_options->enable_buffer_read = state->enable_spill_buffer_read();
    _spill_options->max_read_buffer_bytes = state->max_spill_read_buffer_bytes_per_driver();
    if (state->enable_spill_partitionwise_agg_skew_elimination()) {
        _spill_options->skew_chunk_compactor = make_spill_aggregate_skew_compactor(
                convert_to_aggregator_params(_distinct_op_factory->aggregator_factory()->t_node()));
    }

    return Status::OK();
}

OperatorPtr SpillablePartitionWiseDistinctSinkOperatorFactory::create(int32_t degree_of_parallelism,
                                                                      int32_t driver_sequence) {
    auto distinct_op = std::static_pointer_cast<AggregateDistinctBlockingSinkOperator>(
            _distinct_op_factory->create(degree_of_parallelism, driver_sequence));
    // create spiller
    auto spiller = _spill_factory->create(*_spill_options);
    // create spill process channel

    auto spill_channel = _spill_channel_factory->get_or_create(driver_sequence);

    spill_channel->set_spiller(spiller);
    distinct_op->aggregator()->set_spiller(spiller);
    distinct_op->aggregator()->set_spill_channel(std::move(spill_channel));
    return make_shared<SpillablePartitionWiseDistinctSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                   std::move(distinct_op));
}

Status SpillablePartitionWiseDistinctSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    // Propagate this wrapper's observer into the wrapped sub-ops before their prepare(): the base
    // AggregateDistinctBlockingSourceOperator::prepare attaches the operator's _observer to the
    // aggregator-pip observable, and assign_observer only set _observer on the wrapper. Without this the
    // sub-op would attach nullptr and notify_source would null-deref under the event scheduler (crash in
    // release). _pw_distinct is a ConjugateOperator -- it re-propagates to its own sub-ops in its prepare(),
    // but it still needs a real observer of its own. Mirrors ConjugateOperator::prepare.
    _non_pw_distinct->set_observer(observer());
    _pw_distinct->set_observer(observer());
    RETURN_IF_ERROR(_non_pw_distinct->prepare(state));
    RETURN_IF_ERROR(_pw_distinct->prepare(state));

    // Subscribe this source driver to the parent spiller's source list. All transient per-partition restore
    // readers complete_io on this same parent spiller, so one source-list subscription covers every restore
    // wakeup. Unconditional: the poller-mode gate lives inside subscribe_source.
    _non_pw_distinct->aggregator()->spiller()->observable().subscribe_source(state, observer());
    return Status::OK();
}

const std::shared_ptr<spill::Spiller>& SpillablePartitionWiseDistinctSourceOperator::_spiller() const {
    return _non_pw_distinct->aggregator()->spiller();
}

bool SpillablePartitionWiseDistinctSourceOperator::_sink_complete() const {
    return _non_pw_distinct->aggregator()->is_sink_complete();
}

bool SpillablePartitionWiseDistinctSourceOperator::_conjugate_finished() const {
    return _pw_distinct->is_finished();
}

Status SpillablePartitionWiseDistinctSourceOperator::prepare_local_state(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare_local_state(state));
    RETURN_IF_ERROR(_non_pw_distinct->prepare_local_state(state));
    RETURN_IF_ERROR(_pw_distinct->prepare_local_state(state));

    return Status::OK();
}

void SpillablePartitionWiseDistinctSourceOperator::close(RuntimeState* state) {
    _pw_distinct->close(state);
    _non_pw_distinct->close(state);
    return SourceOperator::close(state);
}

bool SpillablePartitionWiseDistinctSourceOperator::_non_pw_has_output() const {
    return _non_pw_distinct->has_output();
}

bool SpillablePartitionWiseDistinctSourceOperator::_non_pw_finished() const {
    return _non_pw_distinct->is_finished();
}

Status SpillablePartitionWiseDistinctSourceOperator::set_finishing(RuntimeState* state) {
    if (state->is_cancelled()) {
        _non_pw_distinct->aggregator()->spiller()->cancel();
    }
    RETURN_IF_ERROR(_non_pw_distinct->set_finishing(state));
    RETURN_IF_ERROR(_pw_distinct->set_finishing(state));
    return Status::OK();
}

Status SpillablePartitionWiseDistinctSourceOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    RETURN_IF_ERROR(_non_pw_distinct->set_finished(state));
    RETURN_IF_ERROR(_pw_distinct->set_finished(state));
    return Status::OK();
}

StatusOr<ChunkPtr> SpillablePartitionWiseDistinctSourceOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_non_pw_distinct->aggregator()->spiller()->task_status());
    if (!_non_pw_distinct->aggregator()->spiller()->spilled()) {
        return _non_pw_distinct->pull_chunk(state);
    }
    ASSIGN_OR_RETURN(auto res, _pull_spilled_chunk(state));
    return res;
}

Status SpillablePartitionWiseDistinctSourceOperator::reset_state(RuntimeState* state,
                                                                 const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    _partitions.clear();
    _curr_partition_reader.reset();
    _curr_partition_idx = 0;
    _curr_partition_eos = false;
    RETURN_IF_ERROR(_non_pw_distinct->reset_state(state, refill_chunks));
    RETURN_IF_ERROR(_pw_distinct->reset_state(state, refill_chunks));
    return Status::OK();
}

StatusOr<ChunkPtr> SpillablePartitionWiseDistinctSourceOperator::_pull_spilled_chunk(RuntimeState* state) {
    auto& spiller = _non_pw_distinct->aggregator()->spiller();
    // retrieve all partitions
    if (_partitions.empty()) {
        spiller->get_all_partitions(&_partitions);
        DCHECK(!_partitions.empty());
    }

    // processed all partitions
    if (_curr_partition_idx >= _partitions.size()) {
        return nullptr;
    }

    // initialize current partition reader at first
    if (!_curr_partition_reader) {
        _curr_partition_eos = false;
        _curr_partition_reader = std::move(spiller->get_partition_spill_readers({_partitions[_curr_partition_idx]})[0]);
    }

    // if current partition has un-processed data, we try read the data out and push it to pw_agg
    if (!_curr_partition_eos) {
        if (!_curr_partition_reader->has_restore_task()) {
            RETURN_IF_ERROR(_curr_partition_reader->trigger_restore(
                    state, RESOURCE_TLS_MEMTRACER_GUARD(state, std::weak_ptr(_curr_partition_reader))));
        }
        if (_curr_partition_reader->has_output_data()) {
            auto maybe_chunk = _curr_partition_reader->restore(
                    state, RESOURCE_TLS_MEMTRACER_GUARD(state, std::weak_ptr(_curr_partition_reader)));
            if (maybe_chunk.ok() && maybe_chunk.value() && !maybe_chunk.value()->is_empty()) {
                DCHECK(_pw_distinct->need_input() && !_pw_distinct->is_finished());
                RETURN_IF_ERROR(_pw_distinct->push_chunk(state, maybe_chunk.value()));
            } else if (maybe_chunk.status().is_end_of_file()) {
                _curr_partition_eos = true;
                RETURN_IF_ERROR(_pw_distinct->set_finishing(state));
            } else if (!maybe_chunk.ok()) {
                return maybe_chunk.status();
            }
        }
        return nullptr;
    } else if (!_pw_distinct->is_finished()) {
        // all data of the current partition is push to _pw_distinct, so we can pull chunk from it
        DCHECK(!_pw_distinct->need_input() && _pw_distinct->has_output());
        return _pw_distinct->pull_chunk(state);
    } else {
        // the _pw_distinct has processed all the data of the current partition, so we switch to next partition
        DCHECK(_curr_partition_eos && _pw_distinct->is_finished());
        DCHECK(!_curr_partition_reader->has_restore_task());
        DCHECK(_curr_partition_reader->restore_finished());
        //switch to next partition
        ++_curr_partition_idx;
        _curr_partition_eos = false;
        _curr_partition_reader.reset();
        RETURN_IF_ERROR(_pw_distinct->reset_state(state, {}));
    }
    return nullptr;
}

Status SpillablePartitionWiseDistinctSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperatorFactory::prepare(state));
    RETURN_IF_ERROR(_non_pw_distinct_factory->prepare(state));
    RETURN_IF_ERROR(_pw_distinct_factory->prepare(state));
    return Status::OK();
}

OperatorPtr SpillablePartitionWiseDistinctSourceOperatorFactory::create(int32_t degree_of_parallelism,
                                                                        int32_t driver_sequence) {
    return std::make_shared<SpillablePartitionWiseDistinctSourceOperator>(
            this, _id, _plan_node_id, driver_sequence,
            std::static_pointer_cast<AggregateDistinctBlockingSourceOperator>(
                    _non_pw_distinct_factory->create(degree_of_parallelism, driver_sequence)),
            std::static_pointer_cast<query_cache::ConjugateOperator>(
                    _pw_distinct_factory->create(degree_of_parallelism, driver_sequence)));
}

} // namespace starrocks::pipeline
