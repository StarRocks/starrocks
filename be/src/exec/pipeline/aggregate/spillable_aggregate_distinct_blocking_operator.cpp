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

#include "exec/pipeline/aggregate/spillable_aggregate_distinct_blocking_operator.h"

#include <utility>

#include "exec/sorted_streaming_aggregator.h"
#include "exec/spill/spiller.hpp"

namespace starrocks::pipeline {
bool SpillableAggregateDistinctBlockingSinkOperator::need_input() const {
    return !is_finished() && !_aggregator->is_full() && !_aggregator->spill_channel()->has_task();
}

bool SpillableAggregateDistinctBlockingSinkOperator::is_finished() const {
    return AggregateDistinctBlockingSinkOperator::is_finished() || _is_finished;
}

Status SpillableAggregateDistinctBlockingSinkOperator::set_finishing(RuntimeState* state) {
    auto defer_set_finishing = DeferOp([this]() {
        _aggregator->spill_channel()->set_finishing();
        _is_finished = true;
    });

    if (state->is_cancelled()) {
        _aggregator->spiller()->cancel();
    }

    if (!_aggregator->spiller()->spilled()) {
        RETURN_IF_ERROR(AggregateDistinctBlockingSinkOperator::set_finishing(state));
        return Status::OK();
    }

    if (!_aggregator->spill_channel()->has_task()) {
        if (_aggregator->hash_set_variant().size() > 0 || !_streaming_chunks.empty()) {
            _aggregator->hash_set_variant().visit(
                    [&](auto& hash_set_with_key) { _aggregator->it_hash() = hash_set_with_key->hash_set.begin(); });
            _aggregator->spill_channel()->add_spill_task(_build_spill_task(state, true));
        }
    }

    auto io_executor = _aggregator->spill_channel()->io_executor();
    auto flush_function = [this](RuntimeState* state, auto io_executor) {
        auto spiller = _aggregator->spiller();
        return spiller->flush(state, *io_executor, TRACKER_WITH_SPILLER_GUARD(state, spiller));
    };

    _aggregator->ref();
    auto set_call_back_function = [this](RuntimeState* state, auto io_executor) {
        return _aggregator->spiller()->set_flush_all_call_back(
                [this, state]() {
                    auto defer = DeferOp([&]() { _aggregator->unref(state); });
                    RETURN_IF_ERROR(AggregateDistinctBlockingSinkOperator::set_finishing(state));
                    return Status::OK();
                },
                state, *io_executor, TRACKER_WITH_SPILLER_GUARD(state, _aggregator->spiller()));
    };

    SpillProcessTasksBuilder task_builder(state, io_executor);
    task_builder.then(flush_function).finally(set_call_back_function);

    RETURN_IF_ERROR(_aggregator->spill_channel()->execute(task_builder));

    return Status::OK();
}

void SpillableAggregateDistinctBlockingSinkOperator::close(RuntimeState* state) {
    AggregateDistinctBlockingSinkOperator::close(state);
}

Status SpillableAggregateDistinctBlockingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateDistinctBlockingSinkOperator::prepare(state));
    DCHECK(!_aggregator->is_none_group_by_exprs());
    _aggregator->spiller()->set_metrics(
            spill::SpillProcessMetrics(_unique_metrics.get(), state->mutable_total_spill_bytes()));
    if (state->spill_mode() == TSpillMode::FORCE) {
        _spill_strategy = spill::SpillStrategy::SPILL_ALL;
    }

    _peak_revocable_mem_bytes = _unique_metrics->AddHighWaterMarkCounter(
            "PeakRevocableMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    _hash_set_spill_times = ADD_COUNTER(_unique_metrics.get(), "HashSetSpillTimes", TUnit::UNIT);

    return Status::OK();
}

Status SpillableAggregateDistinctBlockingSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }

    if (_spill_strategy == spill::SpillStrategy::NO_SPILL) {
        RETURN_IF_ERROR(AggregateDistinctBlockingSinkOperator::push_chunk(state, chunk));
        set_revocable_mem_bytes(_aggregator->hash_set_memory_usage());
    } else {
        return _try_to_spill_by_auto(state, chunk);
    }
    return Status::OK();
}

void SpillableAggregateDistinctBlockingSinkOperator::_add_streaming_chunk(ChunkPtr chunk) {
    _streaming_bytes += chunk->memory_usage();
    _streaming_chunks.push(std::move(chunk));
}

Status SpillableAggregateDistinctBlockingSinkOperator::_try_to_spill_by_auto(RuntimeState* state,
                                                                             const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(chunk.get()));

    const auto chunk_size = chunk->num_rows();

    const auto hs_mem_usage = _aggregator->hash_set_memory_usage();
    bool hs_needs_expansion = _aggregator->hash_set_variant().need_expand(chunk_size);

    const size_t max_mem_usage = state->spill_mem_table_size();

    auto io_executor = _aggregator->spill_channel()->io_executor();
    auto spiller = _aggregator->spiller();

    if (_streaming_bytes + hs_mem_usage > max_mem_usage) {
        SCOPED_TIMER(_aggregator->streaming_timer());
        ChunkPtr res = std::make_shared<Chunk>();
        RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &res));
        _add_streaming_chunk(res);
    } else if (!hs_needs_expansion || _streaming_bytes + hs_mem_usage * 2 <= max_mem_usage) {
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_set(chunk_size));
        TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_set());
        _continuous_low_reduction_chunk_num = 0;
    } else {
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_set_with_selection(chunk_size));
        }
        {
            SCOPED_TIMER(_aggregator->streaming_timer());
            size_t hit_count = SIMD::count_zero(_aggregator->streaming_selection());
            ChunkPtr res = std::make_shared<Chunk>();
            if (hit_count == 0) {
                RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &res));
                _add_streaming_chunk(res);
            } else if (hit_count != _aggregator->streaming_selection().size()) {
                RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming_with_selection(chunk.get(), &res));
                _add_streaming_chunk(res);
            }
            if (hit_count * 1.0 / chunk_size <= HS_LOW_REDUCTION_THRESHOLD) {
                _continuous_low_reduction_chunk_num++;
            }
        }
    }
    size_t revocable_mem_bytes = _streaming_bytes + _aggregator->hash_set_memory_usage();
    set_revocable_mem_bytes(revocable_mem_bytes);
    if (revocable_mem_bytes > max_mem_usage) {
        bool should_spill_hash_set = _continuous_low_reduction_chunk_num >= HS_LOW_REDUCTION_CHUNK_LIMIT ||
                                     _aggregator->hash_set_memory_usage() >= max_mem_usage;
        if (should_spill_hash_set) {
            _continuous_low_reduction_chunk_num = 0;
        }
        return _spill_all_data(state, should_spill_hash_set);
    }
    return Status::OK();
}

Status SpillableAggregateDistinctBlockingSinkOperator::_spill_all_data(RuntimeState* state,
                                                                       bool should_spill_hash_set) {
    if (should_spill_hash_set) {
        _aggregator->hash_set_variant().visit(
                [&](auto& hash_set_with_key) { _aggregator->it_hash() = hash_set_with_key->hash_set.begin(); });
    }
    CHECK(!_aggregator->spill_channel()->has_task());
    RETURN_IF_ERROR(_aggregator->spill_aggregate_data(state, _build_spill_task(state, should_spill_hash_set)));
    return Status::OK();
}

std::function<StatusOr<ChunkPtr>()> SpillableAggregateDistinctBlockingSinkOperator::_build_spill_task(
        RuntimeState* state, bool should_spill_hash_set) {
    return [this, state, should_spill_hash_set]() -> StatusOr<ChunkPtr> {
        if (!_streaming_chunks.empty()) {
            auto chunk = _streaming_chunks.front();
            _streaming_chunks.pop();
            return chunk;
        }
        if (should_spill_hash_set) {
            if (!_aggregator->is_ht_eos()) {
                auto chunk = std::make_shared<Chunk>();
                _aggregator->convert_hash_set_to_chunk(state->chunk_size(), &chunk);
                return chunk;
            }
            COUNTER_UPDATE(_aggregator->input_row_count(), _aggregator->num_input_rows());
            COUNTER_UPDATE(_aggregator->rows_returned_counter(), _aggregator->hash_set_variant().size());
            COUNTER_UPDATE(_hash_set_spill_times, 1);
            RETURN_IF_ERROR(_aggregator->reset_state(state, {}, nullptr));
        }
        _streaming_bytes = 0;
        return Status::EndOfFile("no more data in current aggregator");
    };
}

Status SpillableAggregateDistinctBlockingSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    // init sort expr
    const auto& group_by_expr = _aggregator_factory->aggregator_param()->grouping_exprs;
    RETURN_IF_ERROR(_sort_exprs.init(group_by_expr, nullptr, &_pool, state));
    _sort_desc = SortDescs::asc_null_first(group_by_expr.size());

    RETURN_IF_ERROR(_sort_exprs.prepare(state, {}, {}));
    RETURN_IF_ERROR(_sort_exprs.open(state));

    // init spill options
    _spill_options = std::make_shared<spill::SpilledOptions>(&_sort_exprs, &_sort_desc);

    _spill_options->spill_mem_table_bytes_size = state->spill_mem_table_size();
    _spill_options->mem_table_pool_size = std::max(1, state->spill_mem_table_num() - 1);
    _spill_options->spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    _spill_options->block_manager = state->query_ctx()->spill_manager()->block_manager();
    _spill_options->name = "agg-distinct-blocking-spill";
    _spill_options->plan_node_id = _plan_node_id;
    _spill_options->encode_level = state->spill_encode_level();

    return Status::OK();
}

OperatorPtr SpillableAggregateDistinctBlockingSinkOperatorFactory::create(int32_t degree_of_parallelism,
                                                                          int32_t driver_sequence) {
    auto aggregator = _aggregator_factory->get_or_create(driver_sequence);

    auto op = std::make_shared<SpillableAggregateDistinctBlockingSinkOperator>(aggregator, this, _id, _plan_node_id,
                                                                               driver_sequence);
    // create spiller
    auto spiller = _spill_factory->create(*_spill_options);
    // create spill process channel
    auto spill_channel = _spill_channel_factory->get_or_create(driver_sequence);

    spill_channel->set_spiller(spiller);
    aggregator->set_spiller(spiller);
    aggregator->set_spill_channel(std::move(spill_channel));

    return op;
}

Status SpillableAggregateDistinctBlockingSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateDistinctBlockingSourceOperator::prepare(state));
    RETURN_IF_ERROR(_stream_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get()));
    RETURN_IF_ERROR(_stream_aggregator->open(state));
    return Status::OK();
}

bool SpillableAggregateDistinctBlockingSourceOperator::has_output() const {
    if (AggregateDistinctBlockingSourceOperator::has_output()) {
        return true;
    }
    if (!_aggregator->spiller()->spilled()) {
        return false;
    }
    if (_accumulator.has_output()) {
        return true;
    }
    // has output data from spiller.
    if (_aggregator->spiller()->has_output_data()) {
        return true;
    }
    if (_aggregator->spiller()->is_cancel()) {
        return true;
    }
    // has eos chunk
    if (_aggregator->is_spilled_eos() && _has_last_chunk) {
        return true;
    }
    return false;
}

bool SpillableAggregateDistinctBlockingSourceOperator::is_finished() const {
    if (_is_finished) {
        return true;
    }
    if (!_aggregator->spiller()->spilled()) {
        return AggregateDistinctBlockingSourceOperator::is_finished();
    }
    if (_accumulator.has_output()) {
        return false;
    }
    if (_aggregator->spiller()->is_cancel()) {
        return true;
    }
    return _aggregator->is_spilled_eos() && !_has_last_chunk;
}

Status SpillableAggregateDistinctBlockingSourceOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    RETURN_IF_ERROR(AggregateDistinctBlockingSourceOperator::set_finished(state));
    return Status::OK();
}

StatusOr<ChunkPtr> SpillableAggregateDistinctBlockingSourceOperator::pull_chunk(RuntimeState* state) {
    if (!_aggregator->spiller()->spilled()) {
        return AggregateDistinctBlockingSourceOperator::pull_chunk(state);
    }

    ASSIGN_OR_RETURN(auto res, _pull_spilled_chunk(state));

    if (res != nullptr) {
        const int64_t old_size = res->num_rows();
        RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_stream_aggregator->conjunct_ctxs(), res.get()));
        _stream_aggregator->update_num_rows_returned(-(old_size - static_cast<int64_t>(res->num_rows())));
    }

    return res;
}

StatusOr<ChunkPtr> SpillableAggregateDistinctBlockingSourceOperator::_pull_spilled_chunk(RuntimeState* state) {
    DCHECK(_accumulator.need_input());
    ChunkPtr res;

    if (_accumulator.has_output()) {
        auto accumulated = std::move(_accumulator.pull());
        return accumulated;
    }

    if (!_aggregator->is_spilled_eos()) {
        auto executor = _aggregator->spill_channel()->io_executor();
        auto& spiller = _aggregator->spiller();
        ASSIGN_OR_RETURN(auto chunk, spiller->restore(state, *executor, TRACKER_WITH_SPILLER_GUARD(state, spiller)));
        if (chunk->is_empty()) {
            return chunk;
        }
        RETURN_IF_ERROR(_stream_aggregator->evaluate_groupby_exprs(chunk.get()));
        RETURN_IF_ERROR(_stream_aggregator->evaluate_agg_fn_exprs(chunk.get(), true));
        ASSIGN_OR_RETURN(res, _stream_aggregator->streaming_compute_distinct(chunk->num_rows()));
        _accumulator.push(std::move(res));

    } else if (_has_last_chunk) {
        _has_last_chunk = false;
        ASSIGN_OR_RETURN(res, _stream_aggregator->pull_eos_chunk());
        if (res != nullptr && !res->is_empty()) {
            _accumulator.push(std::move(res));
        }
        _accumulator.finalize();
    }

    if (_accumulator.has_output()) {
        auto accumulated = std::move(_accumulator.pull());
        return accumulated;
    }

    return nullptr;
}

void SpillableAggregateDistinctBlockingSourceOperator::close(RuntimeState* state) {
    AggregateDistinctBlockingSourceOperator::close(state);
}

Status SpillableAggregateDistinctBlockingSourceOperatorFactory::prepare(RuntimeState* state) {
    _stream_aggregator_factory = std::make_shared<StreamingAggregatorFactory>(_hash_aggregator_factory->t_node());
    _stream_aggregator_factory->set_aggr_mode(_hash_aggregator_factory->aggr_mode());
    return Status::OK();
}

OperatorPtr SpillableAggregateDistinctBlockingSourceOperatorFactory::create(int32_t degree_of_parallelism,
                                                                            int32_t driver_sequence) {
    return std::make_shared<SpillableAggregateDistinctBlockingSourceOperator>(
            _hash_aggregator_factory->get_or_create(driver_sequence),
            _stream_aggregator_factory->get_or_create(driver_sequence), this, _id, _plan_node_id, driver_sequence);
}

} // namespace starrocks::pipeline
