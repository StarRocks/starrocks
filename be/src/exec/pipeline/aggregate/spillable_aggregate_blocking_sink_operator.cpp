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

#include "exec/pipeline/aggregate/spillable_aggregate_blocking_sink_operator.h"

#include <glog/logging.h>

#include <memory>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/aggregate/aggregate_blocking_sink_operator.h"
#include "exec/pipeline/query_context.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "gen_cpp/InternalService_types.h"
#include "runtime/current_thread.h"
#include "storage/chunk_helper.h"
#include "util/failpoint/fail_point.h"
#include "util/race_detect.h"

DEFINE_FAIL_POINT(spill_always_streaming);
DEFINE_FAIL_POINT(spill_always_selection_streaming);

namespace starrocks::pipeline {
bool SpillableAggregateBlockingSinkOperator::need_input() const {
    return !is_finished() && !_aggregator->is_full() && !_aggregator->spill_channel()->has_task();
}

bool SpillableAggregateBlockingSinkOperator::is_finished() const {
    if (!spilled()) {
        return _is_finished || AggregateBlockingSinkOperator::is_finished();
    }
    return _is_finished || _aggregator->is_finished();
}

Status SpillableAggregateBlockingSinkOperator::set_finishing(RuntimeState* state) {
    if (_is_finished) {
        return Status::OK();
    }
    ONCE_DETECT(_set_finishing_once);
    auto defer_set_finishing = DeferOp([this]() {
        _aggregator->spill_channel()->set_finishing_if_not_reuseable();
        _is_finished = true;
    });

    // cancel spill task
    if (state->is_cancelled()) {
        _aggregator->spiller()->cancel();
    }

    if (!_aggregator->spiller()->spilled() && _streaming_chunks.empty()) {
        RETURN_IF_ERROR(AggregateBlockingSinkOperator::set_finishing(state));
        return Status::OK();
    }
    if (!_aggregator->spill_channel()->has_task()) {
        if (_aggregator->hash_map_variant().size() > 0 || !_streaming_chunks.empty()) {
            _aggregator->hash_map_variant().visit(
                    [&](auto& hash_map_with_key) { _aggregator->it_hash() = _aggregator->_state_allocator.begin(); });
            _aggregator->spill_channel()->add_spill_task(_build_spill_task(state));
        }
    }

    auto flush_function = [this](RuntimeState* state) {
        auto& spiller = _aggregator->spiller();
        return spiller->flush(state, TRACKER_WITH_SPILLER_READER_GUARD(state, spiller));
    };

    _aggregator->ref();
    auto set_call_back_function = [this](RuntimeState* state) {
        return _aggregator->spiller()->set_flush_all_call_back(
                [this, state]() {
                    auto defer = DeferOp([&]() { _aggregator->unref(state); });
                    RETURN_IF_ERROR(AggregateBlockingSinkOperator::set_finishing(state));
                    return Status::OK();
                },
                state, TRACKER_WITH_SPILLER_READER_GUARD(state, _aggregator->spiller()));
    };

    SpillProcessTasksBuilder task_builder(state);
    task_builder.then(flush_function).finally(set_call_back_function);

    RETURN_IF_ERROR(_aggregator->spill_channel()->execute(task_builder));

    return Status::OK();
}

void SpillableAggregateBlockingSinkOperator::close(RuntimeState* state) {
    AggregateBlockingSinkOperator::close(state);
}

Status SpillableAggregateBlockingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBlockingSinkOperator::prepare(state));
    DCHECK(!_aggregator->is_none_group_by_exprs());
    _aggregator->spiller()->set_metrics(
            spill::SpillProcessMetrics(_unique_metrics.get(), state->mutable_total_spill_bytes()));

    if (state->spill_mode() == TSpillMode::FORCE) {
        _spill_strategy = spill::SpillStrategy::SPILL_ALL;
    }
    _peak_revocable_mem_bytes = _unique_metrics->AddHighWaterMarkCounter(
            "PeakRevocableMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    _hash_table_spill_times = ADD_COUNTER(_unique_metrics.get(), "HashTableSpillTimes", TUnit::UNIT);
    _agg_group_by_with_limit = false;
    _aggregator->params()->enable_pipeline_share_limit = false;

    return Status::OK();
}

Status SpillableAggregateBlockingSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }

    if (_spill_strategy == spill::SpillStrategy::NO_SPILL) {
        RETURN_IF_ERROR(AggregateBlockingSinkOperator::push_chunk(state, chunk));
        set_revocable_mem_bytes(_aggregator->hash_map_memory_usage());
        return Status::OK();
    }

    if (state->enable_agg_spill_preaggregation()) {
        return _try_to_spill_by_auto(state, chunk);
    } else {
        return _try_to_spill_by_force(state, chunk);
    }
    return Status::OK();
}

Status SpillableAggregateBlockingSinkOperator::reset_state(RuntimeState* state,
                                                           const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    ONCE_RESET(_set_finishing_once);
    RETURN_IF_ERROR(_aggregator->spiller()->reset_state(state));
    RETURN_IF_ERROR(AggregateBlockingSinkOperator::reset_state(state, refill_chunks));
    return Status::OK();
}

Status SpillableAggregateBlockingSinkOperator::_try_to_spill_by_force(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(AggregateBlockingSinkOperator::push_chunk(state, chunk));
    set_revocable_mem_bytes(_aggregator->hash_map_memory_usage());
    return _spill_all_data(state, true);
}

void SpillableAggregateBlockingSinkOperator::_add_streaming_chunk(ChunkPtr chunk) {
    _streaming_rows += chunk->num_rows();
    _streaming_bytes += chunk->memory_usage();
    _streaming_chunks.push(std::move(chunk));
}

Status SpillableAggregateBlockingSinkOperator::_try_to_spill_by_auto(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(chunk.get()));
    const auto chunk_size = chunk->num_rows();

    const size_t ht_mem_usage = _aggregator->hash_map_memory_usage();
    bool ht_need_expansion = _aggregator->hash_map_variant().need_expand(chunk_size);
    const size_t max_mem_usage = state->spill_mem_table_size();

    auto spiller = _aggregator->spiller();

    // goal: control buffered data memory usage, aggregate data as much as possible before spill
    // this strategy is similar to the LIMITED_MEM mode in agg streaming

    bool always_streaming = false;
    bool always_selection_streaming = false;

    FAIL_POINT_TRIGGER_EXECUTE(spill_always_streaming, {
        if (_aggregator->hash_map_variant().size() != 0) {
            always_streaming = true;
        }
    });
    FAIL_POINT_TRIGGER_EXECUTE(spill_always_selection_streaming, {
        if (_aggregator->hash_map_variant().size() != 0) {
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
        SCOPED_TIMER(_aggregator->streaming_timer());
        ChunkPtr res = std::make_shared<Chunk>();
        RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &res));
        _add_streaming_chunk(res);
        return _spill_all_data(state, true);
    } else if (build_hash_table) {
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        TRY_CATCH_ALLOC_SCOPE_START()
        _aggregator->build_hash_map(chunk_size);
        _aggregator->try_convert_to_two_level_map();
        RETURN_IF_ERROR(_aggregator->compute_batch_agg_states(chunk.get(), chunk_size));
        TRY_CATCH_ALLOC_SCOPE_END()

        _aggregator->update_num_input_rows(chunk_size);
        RETURN_IF_ERROR(_aggregator->check_has_error());
        _continuous_low_reduction_chunk_num = 0;
    } else {
        TRY_CATCH_ALLOC_SCOPE_START()
        // selective preaggregation
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            _aggregator->build_hash_map_with_selection(chunk_size);
        }

        size_t hit_count = SIMD::count_zero(_aggregator->streaming_selection());
        // very poor aggregation
        if (hit_count == 0) {
            // put all data into buffer
            ChunkPtr tmp = std::make_shared<Chunk>();
            RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &tmp));
            _add_streaming_chunk(std::move(tmp));
        } else if (hit_count == _aggregator->streaming_selection().size()) {
            // very high reduction
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            RETURN_IF_ERROR(_aggregator->compute_batch_agg_states(chunk.get(), chunk_size));
        } else {
            // middle case
            {
                SCOPED_TIMER(_aggregator->agg_compute_timer());
                RETURN_IF_ERROR(_aggregator->compute_batch_agg_states_with_selection(chunk.get(), chunk_size));
            }
            {
                SCOPED_TIMER(_aggregator->streaming_timer());
                ChunkPtr res = std::make_shared<Chunk>();
                RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming_with_selection(chunk.get(), &res));
                _add_streaming_chunk(std::move(res));
            }
        }
        if (hit_count * 1.0 / chunk_size <= HT_LOW_REDUCTION_THRESHOLD) {
            _continuous_low_reduction_chunk_num++;
        }

        _aggregator->update_num_input_rows(hit_count);
        TRY_CATCH_ALLOC_SCOPE_END()
        RETURN_IF_ERROR(_aggregator->check_has_error());
    }

    // finally, check memory usage of streaming_chunks and hash table, decide whether to spill
    size_t revocable_mem_bytes = _streaming_bytes + _aggregator->hash_map_memory_usage();
    set_revocable_mem_bytes(revocable_mem_bytes);
    if (revocable_mem_bytes > max_mem_usage) {
        // If the aggregation degree of HT_LOW_REDUCTION_CHUNK_LIMIT consecutive chunks is less than HT_LOW_REDUCTION_THRESHOLD,
        // it is meaningless to keep the hash table in memory, just spill it.
        bool should_spill_hash_table = _continuous_low_reduction_chunk_num >= HT_LOW_REDUCTION_CHUNK_LIMIT ||
                                       _aggregator->hash_map_memory_usage() > max_mem_usage;
        if (should_spill_hash_table) {
            _continuous_low_reduction_chunk_num = 0;
        }
        // spill all data
        return _spill_all_data(state, should_spill_hash_table);
    }

    return Status::OK();
}

Status SpillableAggregateBlockingSinkOperator::_spill_all_data(RuntimeState* state, bool should_spill_hash_table) {
    RETURN_IF(_aggregator->hash_map_variant().size() == 0, Status::OK());
    if (should_spill_hash_table) {
        _aggregator->hash_map_variant().visit(
                [&](auto& hash_map_with_key) { _aggregator->it_hash() = _aggregator->_state_allocator.begin(); });
    }
    CHECK(!_aggregator->spill_channel()->has_task());
    RETURN_IF_ERROR(_aggregator->spill_aggregate_data(state, _build_spill_task(state, should_spill_hash_table)));
    return Status::OK();
}

std::function<StatusOr<ChunkPtr>()> SpillableAggregateBlockingSinkOperator::_build_spill_task(
        RuntimeState* state, bool should_spill_hash_table) {
    return [this, state, should_spill_hash_table]() -> StatusOr<ChunkPtr> {
        if (!_streaming_chunks.empty()) {
            auto chunk = _streaming_chunks.front();
            _streaming_chunks.pop();
            return chunk;
        }
        if (should_spill_hash_table) {
            if (!_aggregator->is_ht_eos()) {
                auto chunk = std::make_shared<Chunk>();
                RETURN_IF_ERROR(_aggregator->convert_hash_map_to_chunk(state->chunk_size(), &chunk, true));
                return chunk;
            }
            COUNTER_UPDATE(_aggregator->input_row_count(), _aggregator->num_input_rows());
            COUNTER_UPDATE(_aggregator->rows_returned_counter(), _aggregator->hash_map_variant().size());
            COUNTER_UPDATE(_hash_table_spill_times, 1);
            RETURN_IF_ERROR(_aggregator->reset_state(state, {}, nullptr));
        }
        _streaming_rows = 0;
        _streaming_bytes = 0;
        return Status::EndOfFile("no more data in current aggregator");
    };
}

Status SpillableAggregateBlockingSinkOperatorFactory::prepare(RuntimeState* state) {
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
    if (state->enable_agg_spill_preaggregation()) {
        _spill_options->mem_table_pool_size = std::max(1, state->spill_mem_table_num() - 1);
    } else {
        _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    }
    _spill_options->spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    _spill_options->block_manager = state->query_ctx()->spill_manager()->block_manager();
    _spill_options->name = "agg-blocking-spill";
    _spill_options->enable_block_compaction = state->spill_enable_compaction();
    _spill_options->plan_node_id = _plan_node_id;
    _spill_options->encode_level = state->spill_encode_level();
    _spill_options->wg = state->fragment_ctx()->workgroup();
    _spill_options->enable_buffer_read = state->enable_spill_buffer_read();
    _spill_options->max_read_buffer_bytes = state->max_spill_read_buffer_bytes_per_driver();

    return Status::OK();
}

OperatorPtr SpillableAggregateBlockingSinkOperatorFactory::create(int32_t degree_of_parallelism,
                                                                  int32_t driver_sequence) {
    auto aggregator = _aggregator_factory->get_or_create(driver_sequence);

    auto op = std::make_shared<SpillableAggregateBlockingSinkOperator>(
            aggregator, this, _id, _plan_node_id, driver_sequence, _aggregator_factory->get_shared_limit_countdown());
    // create spiller
    auto spiller = _spill_factory->create(*_spill_options);
    // create spill process channel
    auto spill_channel = _spill_channel_factory->get_or_create(driver_sequence);

    spill_channel->set_spiller(spiller);
    aggregator->set_spiller(spiller);
    aggregator->set_spill_channel(std::move(spill_channel));

    return op;
}

} // namespace starrocks::pipeline