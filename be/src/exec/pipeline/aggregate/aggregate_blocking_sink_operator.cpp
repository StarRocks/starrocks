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

#include "aggregate_blocking_sink_operator.h"

#include <atomic>
#include <memory>
#include <variant>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/agg_runtime_filter_builder.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/race_detect.h"

namespace starrocks::pipeline {

Status AggregateBlockingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _aggregator->attach_sink_observer(state, this->_observer);
    return Status::OK();
}

Status AggregateBlockingSinkOperator::prepare_local_state(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare_local_state(state));
    // since prepare_local_state is executed in multi-thread, use a private owned pool to avoid Lock Contention
    RETURN_IF_ERROR(_aggregator->prepare(state, _object_pool.get(), _unique_metrics.get()));
    RETURN_IF_ERROR(_aggregator->open(state));

    _agg_group_by_with_limit = (!_aggregator->is_none_group_by_exprs() &&     // has group by keys
                                _aggregator->limit() != -1 &&                 // has limit
                                _aggregator->conjunct_ctxs().empty() &&       // no 'having' clause
                                _aggregator->get_aggr_phase() == AggrPhase2); // phase 2, keep it to make things safe
    return Status::OK();
}

void AggregateBlockingSinkOperator::close(RuntimeState* state) {
    auto* counter = ADD_COUNTER(_unique_metrics, "HashTableMemoryUsage", TUnit::BYTES);
    COUNTER_SET(counter, _aggregator->hash_map_memory_usage());
    _aggregator->unref(state);
    Operator::close(state);
}

Status AggregateBlockingSinkOperator::set_finishing(RuntimeState* state) {
    if (_is_finished) return Status::OK();
    ONCE_DETECT(_set_finishing_once);
    auto notify = _aggregator->defer_notify_source();
    auto defer = DeferOp([this]() {
        COUNTER_UPDATE(_aggregator->input_row_count(), _aggregator->num_input_rows());
        _aggregator->sink_complete();
        _is_finished = true;
    });

    // skip processing if cancelled
    if (state->is_cancelled()) {
        return Status::OK();
    }

    if (!_aggregator->is_none_group_by_exprs()) {
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
        // If hash map is empty, we don't need to return value
        if (_aggregator->hash_map_variant().size() == 0) {
            _aggregator->set_ht_eos();
        }
        _aggregator->it_hash() = _aggregator->state_allocator().begin();

    } else if (_aggregator->is_none_group_by_exprs()) {
        // for aggregate no group by, if _num_input_rows is 0,
        // In update phase, we directly return empty chunk.
        // In merge phase, we will handle it.
        if (_aggregator->num_input_rows() == 0 && !_aggregator->needs_finalize()) {
            _aggregator->set_ht_eos();
        }
    }

    return Status::OK();
}

Status AggregateBlockingSinkOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    ONCE_RESET(_set_finishing_once);
    return _aggregator->reset_state(state, refill_chunks, this);
}

StatusOr<ChunkPtr> AggregateBlockingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status AggregateBlockingSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(chunk.get()));

    const auto chunk_size = chunk->num_rows();
    DCHECK_LE(chunk_size, state->chunk_size());

    SCOPED_TIMER(_aggregator->agg_compute_timer());
    TRY_CATCH_ALLOC_SCOPE_START()
    // try to build hash table if has group by keys
    if (!_aggregator->is_none_group_by_exprs()) {
        _aggregator->build_hash_map(chunk_size, _shared_limit_countdown, _agg_group_by_with_limit);
        _aggregator->try_convert_to_two_level_map();
    }

    // batch compute aggregate states
    if (_aggregator->is_none_group_by_exprs()) {
        RETURN_IF_ERROR(_aggregator->compute_single_agg_state(chunk.get(), chunk_size));
    } else {
        if (_agg_group_by_with_limit) {
            // use `_aggregator->streaming_selection()` here to mark whether needs to filter key when compute agg states,
            // it's generated in `build_hash_map`
            size_t zero_count = SIMD::count_zero(_aggregator->streaming_selection().data(), chunk_size);
            if (zero_count == chunk_size) {
                RETURN_IF_ERROR(_aggregator->compute_batch_agg_states(chunk.get(), chunk_size));
            } else {
                RETURN_IF_ERROR(_aggregator->compute_batch_agg_states_with_selection(chunk.get(), chunk_size));
            }
        } else {
            RETURN_IF_ERROR(_aggregator->compute_batch_agg_states(chunk.get(), chunk_size));
        }
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    _build_in_runtime_filters(state);
    _aggregator->update_num_input_rows(chunk_size);
    RETURN_IF_ERROR(_aggregator->check_has_error());

    return Status::OK();
}

void AggregateBlockingSinkOperator::_build_in_runtime_filters(RuntimeState* state) {
    if (!_agg_group_by_with_limit || _shared_limit_countdown.load(std::memory_order_acquire) > 0 ||
        _in_runtime_filter_built) {
        return;
    }
    std::list<RuntimeFilterBuildDescriptor*> merged_runtime_filters;
    const auto& build_runtime_filters = factory()->build_runtime_filters();
    for (size_t i = 0; i < build_runtime_filters.size(); ++i) {
        auto desc = build_runtime_filters[i];
        auto* runtime_filter = _aggregator->build_in_filters(state, build_runtime_filters[i]);
        auto* merger = factory()->in_filter_merger(build_runtime_filters[i]->filter_id());
        if (merger->merge(_driver_sequence, desc, runtime_filter)) {
            desc->set_runtime_filter(merger->merged_runtime_filter());
            merged_runtime_filters.emplace_back(desc);
        }
    }
    state->runtime_filter_port()->publish_runtime_filters(merged_runtime_filters);
    _in_runtime_filter_built = true;
}

Status AggregateBlockingSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    return Status::OK();
}

OperatorPtr AggregateBlockingSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    const auto& build_runtime_filters = this->build_runtime_filters();
    if (!build_runtime_filters.empty() && _in_filter_mergers.empty()) {
        for (auto desc : build_runtime_filters) {
            _in_filter_mergers.emplace(desc->filter_id(),
                                       std::make_shared<AggInRuntimeFilterMerger>(degree_of_parallelism));
        }
    }

    // init operator
    auto aggregator = _aggregator_factory->get_or_create(driver_sequence);
    auto op = std::make_shared<AggregateBlockingSinkOperator>(aggregator, this, _id, _plan_node_id, driver_sequence,
                                                              _aggregator_factory->get_shared_limit_countdown());
    return op;
}

} // namespace starrocks::pipeline
