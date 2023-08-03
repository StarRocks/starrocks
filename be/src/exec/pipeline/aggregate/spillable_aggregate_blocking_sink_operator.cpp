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
    auto defer_set_finishing = DeferOp([this]() {
        _aggregator->spill_channel()->set_finishing();
        _is_finished = true;
    });

    // cancel spill task
    if (state->is_cancelled()) {
        _aggregator->spiller()->cancel();
    }

    if (!_aggregator->spiller()->spilled()) {
        RETURN_IF_ERROR(AggregateBlockingSinkOperator::set_finishing(state));
        return Status::OK();
    }

    auto io_executor = _aggregator->spill_channel()->io_executor();

    auto flush_function = [this](RuntimeState* state, auto io_executor) {
        return _aggregator->spiller()->flush(state, *io_executor, RESOURCE_TLS_MEMTRACER_GUARD(state));
    };

    _aggregator->ref();
    auto set_call_back_function = [this](RuntimeState* state, auto io_executor) {
        return _aggregator->spiller()->set_flush_all_call_back(
                [this, state]() {
                    auto defer = DeferOp([&]() { _aggregator->unref(state); });
                    RETURN_IF_ERROR(AggregateBlockingSinkOperator::set_finishing(state));
                    return Status::OK();
                },
                state, *io_executor, RESOURCE_TLS_MEMTRACER_GUARD(state));
    };

    SpillProcessTasksBuilder task_builder(state, io_executor);
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
    _aggregator->spiller()->set_metrics(spill::SpillProcessMetrics(_unique_metrics.get()));

    if (state->spill_mode() == TSpillMode::FORCE) {
        _spill_strategy = spill::SpillStrategy::SPILL_ALL;
    }
    _peak_revocable_mem_bytes = _unique_metrics->AddHighWaterMarkCounter(
            "PeakRevocableMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    return Status::OK();
}

Status SpillableAggregateBlockingSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(AggregateBlockingSinkOperator::push_chunk(state, chunk));
    set_revocable_mem_bytes(_aggregator->hash_map_memory_usage());
    if (_spill_strategy == spill::SpillStrategy::SPILL_ALL) {
        return _spill_all_inputs(state, chunk);
    }
    return Status::OK();
}

Status SpillableAggregateBlockingSinkOperator::_spill_all_inputs(RuntimeState* state, const ChunkPtr& chunk) {
    // spill all data
    DCHECK(!_aggregator->is_none_group_by_exprs());
    _aggregator->hash_map_variant().visit(
            [&](auto& hash_map_with_key) { _aggregator->it_hash() = _aggregator->_state_allocator.begin(); });
    CHECK(!_aggregator->spill_channel()->has_task());
    RETURN_IF_ERROR(_aggregator->spill_aggregate_data(state, _build_spill_task(state)));
    return Status::OK();
}

std::function<StatusOr<ChunkPtr>()> SpillableAggregateBlockingSinkOperator::_build_spill_task(RuntimeState* state) {
    return [this, state]() -> StatusOr<ChunkPtr> {
        bool use_intermediate_as_output = true;
        if (!_aggregator->is_ht_eos()) {
            auto chunk = std::make_shared<Chunk>();
            RETURN_IF_ERROR(
                    _aggregator->convert_hash_map_to_chunk(state->chunk_size(), &chunk, &use_intermediate_as_output));
            return chunk;
        }
        RETURN_IF_ERROR(_aggregator->reset_state(state, {}, nullptr));
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
    _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    _spill_options->spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    _spill_options->block_manager = state->query_ctx()->spill_manager()->block_manager();
    _spill_options->name = "agg-blocking-spill";
    _spill_options->plan_node_id = _plan_node_id;
    _spill_options->encode_level = state->spill_encode_level();

    return Status::OK();
}

OperatorPtr SpillableAggregateBlockingSinkOperatorFactory::create(int32_t degree_of_parallelism,
                                                                  int32_t driver_sequence) {
    auto aggregator = _aggregator_factory->get_or_create(driver_sequence);

    auto op = std::make_shared<SpillableAggregateBlockingSinkOperator>(aggregator, this, _id, _plan_node_id,
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

} // namespace starrocks::pipeline