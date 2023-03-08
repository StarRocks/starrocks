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
#include "gen_cpp/InternalService_types.h"
#include "runtime/current_thread.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {
bool SpillableAggregateBlockingSinkOperator::need_input() const {
    return !is_finished() && !_aggregator->is_full();
}

bool SpillableAggregateBlockingSinkOperator::is_finished() const {
    return AggregateBlockingSinkOperator::is_finished() && _is_finished;
}

Status SpillableAggregateBlockingSinkOperator::set_finishing(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBlockingSinkOperator::set_finishing(state));

    // ugly code
    // TODO: fixme
    auto io_executor = _aggregator->spill_channel()->io_executor();
    RETURN_IF_ERROR(_aggregator->spiller()->flush(state, *io_executor, MemTrackerGuard(tls_mem_tracker)));

    auto set_call_back_function = [this](RuntimeState* state, auto io_executor) {
        _aggregator->spill_channel()->set_finishing();
        return _aggregator->spiller()->set_flush_all_call_back(
                [this]() {
                    _is_finished = true;
                    return Status::OK();
                },
                state, *io_executor, MemTrackerGuard(tls_mem_tracker));
    };
    RETURN_IF_ERROR(set_call_back_function(state, io_executor));

    return Status::OK();
}

void SpillableAggregateBlockingSinkOperator::close(RuntimeState* state) {
    AggregateBlockingSinkOperator::close(state);
}

Status SpillableAggregateBlockingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBlockingSinkOperator::prepare(state));
    DCHECK(!_aggregator->is_none_group_by_exprs());
    _aggregator->spiller()->set_metrics(SpillProcessMetrics(_unique_metrics.get()));

    if (state->spill_mode() == TSpillMode::FORCE) {
        _spill_strategy = SpillStrategy::SPILL_ALL;
    }

    return Status::OK();
}

Status SpillableAggregateBlockingSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(chunk.get()));
    if (_spill_strategy == SpillStrategy::NO_SPILL) {
        return AggregateBlockingSinkOperator::push_chunk(state, chunk);
    } else if (_spill_strategy == SpillStrategy::SPILL_ALL) {
        return _spill_all_inputs(state, chunk);
    } else {
        return Status::OK();
    }
}

Status SpillableAggregateBlockingSinkOperator::_spill_all_inputs(RuntimeState* state, const ChunkPtr& chunk) {
    // spill all data
    auto spillable = std::make_shared<Chunk>();
    RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &spillable));

    auto executor = _aggregator->spill_channel()->io_executor();
    RETURN_IF_ERROR(_aggregator->spiller()->spill(state, spillable, *executor, MemTrackerGuard(tls_mem_tracker)));

    return Status::OK();
}

Status SpillableAggregateBlockingSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    auto* spill_manager = state->query_ctx()->spill_manager();

    // init sort expr
    const auto& group_by_expr = _aggregator_factory->aggregator_param()->grouping_exprs;
    RETURN_IF_ERROR(_sort_exprs.init(group_by_expr, nullptr, &_pool, state));
    _sort_desc = SortDescs::asc_null_first(group_by_expr.size());

    RETURN_IF_ERROR(_sort_exprs.prepare(state, {}, {}));
    RETURN_IF_ERROR(_sort_exprs.open(state));

    // init spill options
    _spill_options = std::make_shared<SpilledOptions>(&_sort_exprs, &_sort_desc);

    _spill_options->spill_file_size = state->spill_mem_table_size();
    _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    _spill_options->spill_type = SpillFormaterType::SPILL_BY_COLUMN;
    // init chunk builder
    _spill_options->chunk_builder = [&, state]() {
        auto intermediate_tuple_id = _aggregator_factory->aggregator_param()->intermediate_tuple_id;
        auto desc = state->desc_tbl().get_tuple_descriptor(intermediate_tuple_id);
        return _aggregator_factory->aggregator_param()->create_result_chunk(true, *desc);
    };
    _spill_options->path_provider_factory = spill_manager->provider(fmt::format("agg-spill-{}", _plan_node_id));

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