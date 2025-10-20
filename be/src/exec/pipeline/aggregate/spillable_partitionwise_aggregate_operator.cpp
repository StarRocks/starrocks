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

#include "exec/pipeline/aggregate/spillable_partitionwise_aggregate_operator.h"

namespace starrocks::pipeline {

bool SpillablePartitionWiseAggregateSinkOperator::need_input() const {
    return !is_finished() && !_agg_op->aggregator()->is_full() && !_agg_op->aggregator()->spill_channel()->has_task();
}

bool SpillablePartitionWiseAggregateSinkOperator::is_finished() const {
    if (!spilled()) {
        return _is_finished || _agg_op->is_finished();
    }
    return _is_finished || _agg_op->aggregator()->is_finished();
}

Status SpillablePartitionWiseAggregateSinkOperator::set_finishing(RuntimeState* state) {
    if (_is_finished) {
        return Status::OK();
    }
    ONCE_DETECT(_set_finishing_once);
    auto defer_set_finishing = DeferOp([this]() {
        _agg_op->aggregator()->spill_channel()->set_finishing_if_not_reuseable();
        _is_finished = true;
    });

    // cancel spill task
    if (state->is_cancelled()) {
        _agg_op->aggregator()->spiller()->cancel();
    }

    if (!_agg_op->aggregator()->spiller()->spilled() && _streaming_chunks.empty()) {
        RETURN_IF_ERROR(_agg_op->set_finishing(state));
        return Status::OK();
    }
    if (!_agg_op->aggregator()->spill_channel()->has_task()) {
        if (_agg_op->aggregator()->hash_map_variant().size() > 0 || !_streaming_chunks.empty()) {
            _agg_op->aggregator()->it_hash() = _agg_op->aggregator()->state_allocator().begin();
            _agg_op->aggregator()->spill_channel()->add_spill_task(_build_spill_task(state));
        }
    }

    auto flush_function = [this](RuntimeState* state) {
        auto& spiller = _agg_op->aggregator()->spiller();
        return spiller->flush(state, TRACKER_WITH_SPILLER_READER_GUARD(state, spiller));
    };

    _agg_op->aggregator()->ref();
    auto set_call_back_function = [this](RuntimeState* state) {
        return _agg_op->aggregator()->spiller()->set_flush_all_call_back(
                [this, state]() {
                    auto defer = DeferOp([&]() { _agg_op->aggregator()->unref(state); });
                    RETURN_IF_ERROR(_agg_op->set_finishing(state));
                    return Status::OK();
                },
                state, TRACKER_WITH_SPILLER_READER_GUARD(state, _agg_op->aggregator()->spiller()));
    };

    SpillProcessTasksBuilder task_builder(state);
    task_builder.then(flush_function).finally(set_call_back_function);

    RETURN_IF_ERROR(_agg_op->aggregator()->spill_channel()->execute(task_builder));

    return Status::OK();
}

void SpillablePartitionWiseAggregateSinkOperator::close(RuntimeState* state) {
    _agg_op->close(state);
    Operator::close(state);
}

Status SpillablePartitionWiseAggregateSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_agg_op->prepare(state));
    DCHECK(!_agg_op->aggregator()->is_none_group_by_exprs());
    _agg_op->aggregator()->spiller()->set_metrics(
            spill::SpillProcessMetrics(_unique_metrics.get(), state->mutable_total_spill_bytes()));

    if (state->spill_mode() == TSpillMode::FORCE) {
        _spill_strategy = spill::SpillStrategy::SPILL_ALL;
    }

    if (state->enable_spill_partitionwise_agg_skew_elimination()) {
        auto* agg_op_factory = dynamic_cast<AggregateBlockingSinkOperatorFactory*>(_agg_op->get_factory());
        _agg_op->aggregator()->spiller()->options().opt_aggregator_params =
                convert_to_aggregator_params(agg_op_factory->aggregator_factory()->t_node());
    }
    _peak_revocable_mem_bytes = _unique_metrics->AddHighWaterMarkCounter(
            "PeakRevocableMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    _hash_table_spill_times = ADD_COUNTER(_unique_metrics.get(), "HashTableSpillTimes", TUnit::UNIT);
    _agg_op->set_agg_group_by_with_limit(false);
    _agg_op->aggregator()->params()->enable_pipeline_share_limit = false;

    return Status::OK();
}

Status SpillablePartitionWiseAggregateSinkOperator::prepare_local_state(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare_local_state(state));
    return _agg_op->prepare(state);
}

Status SpillablePartitionWiseAggregateSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }

    if (_spill_strategy == spill::SpillStrategy::NO_SPILL) {
        RETURN_IF_ERROR(_agg_op->push_chunk(state, chunk));
        set_revocable_mem_bytes(_agg_op->aggregator()->hash_map_memory_usage());
        return Status::OK();
    }

    if (state->enable_agg_spill_preaggregation()) {
        return _try_to_spill_by_auto(state, chunk);
    } else {
        return _try_to_spill_by_force(state, chunk);
    }
    return Status::OK();
}

Status SpillablePartitionWiseAggregateSinkOperator::reset_state(RuntimeState* state,
                                                                const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    ONCE_RESET(_set_finishing_once);
    RETURN_IF_ERROR(_agg_op->aggregator()->spiller()->reset_state(state));
    RETURN_IF_ERROR(_agg_op->reset_state(state, refill_chunks));
    return Status::OK();
}

Status SpillablePartitionWiseAggregateSinkOperator::_try_to_spill_by_force(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_agg_op->push_chunk(state, chunk));
    set_revocable_mem_bytes(_agg_op->aggregator()->hash_map_memory_usage());
    return _spill_all_data(state, true);
}

void SpillablePartitionWiseAggregateSinkOperator::_add_streaming_chunk(ChunkPtr chunk) {
    _streaming_rows += chunk->num_rows();
    _streaming_bytes += chunk->memory_usage();
    _streaming_chunks.push(std::move(chunk));
}

Status SpillablePartitionWiseAggregateSinkOperator::_try_to_spill_by_auto(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_agg_op->aggregator()->evaluate_groupby_exprs(chunk.get()));
    const auto chunk_size = chunk->num_rows();

    const size_t ht_mem_usage = _agg_op->aggregator()->hash_map_memory_usage();
    bool ht_need_expansion = _agg_op->aggregator()->hash_map_variant().need_expand(chunk_size);
    const size_t max_mem_usage = state->spill_mem_table_size();

    auto spiller = _agg_op->aggregator()->spiller();

    // goal: control buffered data memory usage, aggregate data as much as possible before spill
    // this strategy is similar to the LIMITED_MEM mode in agg streaming

    bool always_streaming = false;
    bool always_selection_streaming = false;

    FAIL_POINT_TRIGGER_EXECUTE(spill_always_streaming, {
        if (_agg_op->aggregator()->hash_map_variant().size() != 0) {
            always_streaming = true;
        }
    });
    FAIL_POINT_TRIGGER_EXECUTE(spill_always_selection_streaming, {
        if (_agg_op->aggregator()->hash_map_variant().size() != 0) {
            always_selection_streaming = true;
        }
    });

    // if hash table don't need expand or it's still very small after expansion, just put all data into it
    bool build_hash_table =
            !ht_need_expansion || (ht_need_expansion && _streaming_bytes + ht_mem_usage * 2 <= max_mem_usage);
    build_hash_table = build_hash_table && !always_selection_streaming;
    if (_streaming_bytes + ht_mem_usage > max_mem_usage || always_streaming) {
        // if current memory usage exceeds limit,
        // use force streaming mode and spill all data
        SCOPED_TIMER(_agg_op->aggregator()->streaming_timer());
        ChunkPtr res = std::make_shared<Chunk>();
        RETURN_IF_ERROR(_agg_op->aggregator()->output_chunk_by_streaming(chunk.get(), &res, true));
        _add_streaming_chunk(res);
        return _spill_all_data(state, true);
    } else if (build_hash_table) {
        SCOPED_TIMER(_agg_op->aggregator()->agg_compute_timer());
        TRY_CATCH_ALLOC_SCOPE_START()
        _agg_op->aggregator()->build_hash_map(chunk_size);
        _agg_op->aggregator()->try_convert_to_two_level_map();
        RETURN_IF_ERROR(_agg_op->aggregator()->compute_batch_agg_states(chunk.get(), chunk_size));
        TRY_CATCH_ALLOC_SCOPE_END()

        _agg_op->aggregator()->update_num_input_rows(chunk_size);
        RETURN_IF_ERROR(_agg_op->aggregator()->check_has_error());
        _continuous_low_reduction_chunk_num = 0;
    } else {
        TRY_CATCH_ALLOC_SCOPE_START()
        // selective preaggregation
        {
            SCOPED_TIMER(_agg_op->aggregator()->agg_compute_timer());
            _agg_op->aggregator()->build_hash_map_with_selection(chunk_size);
        }

        size_t hit_count = SIMD::count_zero(_agg_op->aggregator()->streaming_selection());
        // very poor aggregation
        if (hit_count == 0) {
            // put all data into buffer
            ChunkPtr tmp = std::make_shared<Chunk>();
            RETURN_IF_ERROR(_agg_op->aggregator()->output_chunk_by_streaming(chunk.get(), &tmp, true));
            _add_streaming_chunk(std::move(tmp));
        } else if (hit_count == _agg_op->aggregator()->streaming_selection().size()) {
            // very high reduction
            SCOPED_TIMER(_agg_op->aggregator()->agg_compute_timer());
            RETURN_IF_ERROR(_agg_op->aggregator()->compute_batch_agg_states(chunk.get(), chunk_size));
        } else {
            // middle case
            {
                SCOPED_TIMER(_agg_op->aggregator()->agg_compute_timer());
                RETURN_IF_ERROR(
                        _agg_op->aggregator()->compute_batch_agg_states_with_selection(chunk.get(), chunk_size));
            }
            {
                SCOPED_TIMER(_agg_op->aggregator()->streaming_timer());
                ChunkPtr res = std::make_shared<Chunk>();
                RETURN_IF_ERROR(
                        _agg_op->aggregator()->output_chunk_by_streaming_with_selection(chunk.get(), &res, true));
                _add_streaming_chunk(std::move(res));
            }
        }
        if (hit_count * 1.0 / chunk_size <= HT_LOW_REDUCTION_THRESHOLD) {
            _continuous_low_reduction_chunk_num++;
        }

        _agg_op->aggregator()->update_num_input_rows(hit_count);
        TRY_CATCH_ALLOC_SCOPE_END()
        RETURN_IF_ERROR(_agg_op->aggregator()->check_has_error());
    }

    // finally, check memory usage of streaming_chunks and hash table, decide whether to spill
    size_t revocable_mem_bytes = _streaming_bytes + _agg_op->aggregator()->hash_map_memory_usage();
    set_revocable_mem_bytes(revocable_mem_bytes);
    if (revocable_mem_bytes > max_mem_usage) {
        // If the aggregation degree of HT_LOW_REDUCTION_CHUNK_LIMIT consecutive chunks is less than HT_LOW_REDUCTION_THRESHOLD,
        // it is meaningless to keep the hash table in memory, just spill it.
        bool should_spill_hash_table = _continuous_low_reduction_chunk_num >= HT_LOW_REDUCTION_CHUNK_LIMIT ||
                                       _agg_op->aggregator()->hash_map_memory_usage() > max_mem_usage;
        if (should_spill_hash_table) {
            _continuous_low_reduction_chunk_num = 0;
        }
        // spill all data
        return _spill_all_data(state, should_spill_hash_table);
    }

    return Status::OK();
}

ChunkPtr& SpillablePartitionWiseAggregateSinkOperator::_append_hash_column(ChunkPtr& chunk) {
    const auto& group_by_exprs = _agg_op->aggregator()->group_by_expr_ctxs();
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

Status SpillablePartitionWiseAggregateSinkOperator::_spill_all_data(RuntimeState* state, bool should_spill_hash_table) {
    RETURN_IF(_agg_op->aggregator()->hash_map_variant().size() == 0, Status::OK());
    if (should_spill_hash_table) {
        _agg_op->aggregator()->it_hash() = _agg_op->aggregator()->state_allocator().begin();
    }
    CHECK(!_agg_op->aggregator()->spill_channel()->has_task());
    RETURN_IF_ERROR(
            _agg_op->aggregator()->spill_aggregate_data(state, _build_spill_task(state, should_spill_hash_table)));
    return Status::OK();
}

std::function<StatusOr<ChunkPtr>()> SpillablePartitionWiseAggregateSinkOperator::_build_spill_task(
        RuntimeState* state, bool should_spill_hash_table) {
    auto chunk_provider = [this, state, should_spill_hash_table]() -> StatusOr<ChunkPtr> {
        if (!_streaming_chunks.empty()) {
            auto chunk = _streaming_chunks.front();
            _streaming_chunks.pop();
            return chunk;
        }
        if (should_spill_hash_table) {
            if (!_agg_op->aggregator()->is_ht_eos()) {
                auto chunk = std::make_shared<Chunk>();
                RETURN_IF_ERROR(_agg_op->aggregator()->convert_hash_map_to_chunk(state->chunk_size(), &chunk, true));
                return chunk;
            }
            COUNTER_UPDATE(_agg_op->aggregator()->input_row_count(), _agg_op->aggregator()->num_input_rows());
            COUNTER_UPDATE(_agg_op->aggregator()->rows_returned_counter(),
                           _agg_op->aggregator()->hash_map_variant().size());
            COUNTER_UPDATE(_hash_table_spill_times, 1);
            RETURN_IF_ERROR(_agg_op->aggregator()->reset_state(state, {}, nullptr));
        }
        _streaming_rows = 0;
        _streaming_bytes = 0;
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

Status SpillablePartitionWiseAggregateSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    // init spill options
    _spill_options = std::make_shared<spill::SpilledOptions>(state->spill_partitionwise_agg_partition_num());

    _spill_options->spill_mem_table_bytes_size = state->spill_mem_table_size();
    if (state->enable_agg_spill_preaggregation()) {
        _spill_options->mem_table_pool_size = std::max(1, state->spill_mem_table_num() - 1);
    } else {
        _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    }
    _spill_options->spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    _spill_options->block_manager = state->query_ctx()->spill_manager()->block_manager();
    _spill_options->name = "agg-blocking-spill";
    _spill_options->splittable = false;
    _spill_options->enable_block_compaction = state->spill_enable_compaction();
    _spill_options->plan_node_id = _plan_node_id;
    _spill_options->encode_level = state->spill_encode_level();
    _spill_options->wg = state->fragment_ctx()->workgroup();
    _spill_options->enable_buffer_read = state->enable_spill_buffer_read();
    _spill_options->max_read_buffer_bytes = state->max_spill_read_buffer_bytes_per_driver();

    return Status::OK();
}

OperatorPtr SpillablePartitionWiseAggregateSinkOperatorFactory::create(int32_t degree_of_parallelism,
                                                                       int32_t driver_sequence) {
    auto agg_op = std::static_pointer_cast<AggregateBlockingSinkOperator>(
            _agg_op_factory->create(degree_of_parallelism, driver_sequence));
    // create spiller
    auto spiller = _spill_factory->create(*_spill_options);
    // create spill process channel

    auto spill_channel = _spill_channel_factory->get_or_create(driver_sequence);

    spill_channel->set_spiller(spiller);
    agg_op->aggregator()->set_spiller(spiller);
    agg_op->aggregator()->set_spill_channel(std::move(spill_channel));
    return make_shared<SpillablePartitionWiseAggregateSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                    std::move(agg_op));
}

Status SpillablePartitionWiseAggregateSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    RETURN_IF_ERROR(_non_pw_agg->prepare(state));
    RETURN_IF_ERROR(_pw_agg->prepare(state));
    return Status::OK();
}

Status SpillablePartitionWiseAggregateSourceOperator::prepare_local_state(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare_local_state(state));
    RETURN_IF_ERROR(_non_pw_agg->prepare_local_state(state));
    RETURN_IF_ERROR(_pw_agg->prepare_local_state(state));
    return Status::OK();
}

void SpillablePartitionWiseAggregateSourceOperator::close(RuntimeState* state) {
    _pw_agg->close(state);
    _non_pw_agg->close(state);
    return SourceOperator::close(state);
}

bool SpillablePartitionWiseAggregateSourceOperator::has_output() const {
    auto& spiller = _non_pw_agg->aggregator()->spiller();
    bool has_spilled = spiller->spilled();

    if (!has_spilled) {
        return _non_pw_agg->has_output();
    }

    // is_sink_complete returning true indicates that sink operator has finished all spilling tasks
    // and source operator can process spill partitions one by one safely.
    if (!_non_pw_agg->aggregator()->is_sink_complete()) {
        return false;
    }

    // at first time, we must call pull_chunk to acquire all partitions
    if (_partitions.empty()) {
        return true;
    }
    // if we processed all partitions, then no data to output
    if (_curr_partition_idx >= _partitions.size()) {
        return false;
    }

    // if current partition reader is not created, or no async store task trigger, or has output data.
    // we must invoke pull_chunk to try to obtain chunk from current partition reader and push it to pw_agg
    if (!_curr_partition_reader || !_curr_partition_reader->has_restore_task() ||
        _curr_partition_reader->has_output_data()) {
        return true;
    }

    // pw_agg receives all data of the current partition, then it should output result.
    if (_curr_partition_eos && !_pw_agg->is_finished()) {
        return true;
    }

    return false;
}

bool SpillablePartitionWiseAggregateSourceOperator::is_finished() const {
    if (_is_finished) {
        return true;
    }
    auto& spiller = _non_pw_agg->aggregator()->spiller();
    if (!spiller->spilled()) {
        return _non_pw_agg->is_finished();
    } else {
        return !_partitions.empty() && _curr_partition_idx >= _partitions.size();
    }
}

Status SpillablePartitionWiseAggregateSourceOperator::set_finishing(RuntimeState* state) {
    if (state->is_cancelled()) {
        _non_pw_agg->aggregator()->spiller()->cancel();
    }
    RETURN_IF_ERROR(_non_pw_agg->set_finishing(state));
    RETURN_IF_ERROR(_pw_agg->set_finishing(state));
    return Status::OK();
}

Status SpillablePartitionWiseAggregateSourceOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    RETURN_IF_ERROR(_non_pw_agg->set_finished(state));
    RETURN_IF_ERROR(_pw_agg->set_finished(state));
    return Status::OK();
}

StatusOr<ChunkPtr> SpillablePartitionWiseAggregateSourceOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_non_pw_agg->aggregator()->spiller()->task_status());
    if (!_non_pw_agg->aggregator()->spiller()->spilled()) {
        return _non_pw_agg->pull_chunk(state);
    }
    ASSIGN_OR_RETURN(auto res, _pull_spilled_chunk(state));
    return res;
}

Status SpillablePartitionWiseAggregateSourceOperator::reset_state(RuntimeState* state,
                                                                  const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    _partitions.clear();
    _curr_partition_reader.reset();
    _curr_partition_idx = 0;
    _curr_partition_eos = false;
    RETURN_IF_ERROR(_non_pw_agg->reset_state(state, refill_chunks));
    RETURN_IF_ERROR(_pw_agg->reset_state(state, refill_chunks));
    return Status::OK();
}

StatusOr<ChunkPtr> SpillablePartitionWiseAggregateSourceOperator::_pull_spilled_chunk(RuntimeState* state) {
    auto& spiller = _non_pw_agg->aggregator()->spiller();
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
                DCHECK(_pw_agg->need_input() && !_pw_agg->is_finished());
                RETURN_IF_ERROR(_pw_agg->push_chunk(state, std::move(maybe_chunk.value())));
            } else if (maybe_chunk.status().is_end_of_file()) {
                _curr_partition_eos = true;
                RETURN_IF_ERROR(_pw_agg->set_finishing(state));
            } else if (!maybe_chunk.ok()) {
                return maybe_chunk.status();
            }
        }
        return nullptr;
    } else if (!_pw_agg->is_finished()) {
        // all data of the current partition is push to _pw_agg, so we can pull chunk from it
        DCHECK(!_pw_agg->need_input() && _pw_agg->has_output());
        return _pw_agg->pull_chunk(state);
    } else {
        // the _pw_agg has processed all the data of the current partition, so we switch to next partition
        DCHECK(_curr_partition_eos && _pw_agg->is_finished());
        DCHECK(!_curr_partition_reader->has_restore_task());
        DCHECK(_curr_partition_reader->restore_finished());
        //switch to next partition
        ++_curr_partition_idx;
        _curr_partition_eos = false;
        _curr_partition_reader.reset();
        RETURN_IF_ERROR(_pw_agg->reset_state(state, {}));
    }
    return nullptr;
}

Status SpillablePartitionWiseAggregateSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperatorFactory::prepare(state));
    RETURN_IF_ERROR(_non_pw_agg_factory->prepare(state));
    RETURN_IF_ERROR(_pw_agg_factory->prepare(state));
    return Status::OK();
}

OperatorPtr SpillablePartitionWiseAggregateSourceOperatorFactory::create(int32_t degree_of_parallelism,
                                                                         int32_t driver_sequence) {
    return std::make_shared<SpillablePartitionWiseAggregateSourceOperator>(
            this, _id, _plan_node_id, driver_sequence,
            std::static_pointer_cast<AggregateBlockingSourceOperator>(
                    _non_pw_agg_factory->create(degree_of_parallelism, driver_sequence)),
            std::static_pointer_cast<query_cache::ConjugateOperator>(
                    _pw_agg_factory->create(degree_of_parallelism, driver_sequence)));
}

} // namespace starrocks::pipeline
