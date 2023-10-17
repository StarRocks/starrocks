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

#include "exec/pipeline/aggregate/spillable_aggregate_blocking_source_operator.h"

#include <algorithm>

#include "common/status.h"
#include "exec/pipeline/aggregate/aggregate_blocking_source_operator.h"

namespace starrocks::pipeline {
Status SpillableAggregateBlockingSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBlockingSourceOperator::prepare(state));
    RETURN_IF_ERROR(_stream_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get()));
    RETURN_IF_ERROR(_stream_aggregator->open(state));
    return Status::OK();
}

void SpillableAggregateBlockingSourceOperator::close(RuntimeState* state) {
    AggregateBlockingSourceOperator::close(state);
    _stream_aggregator->close(state);
}

bool SpillableAggregateBlockingSourceOperator::has_output() const {
    bool has_spilled = _aggregator->spiller()->spilled();

    if (!has_spilled && AggregateBlockingSourceOperator::has_output()) {
        return true;
    }

    if (!has_spilled) {
        return false;
    }
    if (_accumulator.has_output()) {
        return true;
    }
    // has output data from spiller.
    if (_aggregator->spiller()->has_output_data()) {
        return true;
    }
    // has eos chunk
    if (_aggregator->is_spilled_eos() && _has_last_chunk) {
        return true;
    }
    return false;
}

bool SpillableAggregateBlockingSourceOperator::is_finished() const {
    if (_is_finished) {
        return true;
    }
    if (!_aggregator->spiller()->spilled()) {
        return AggregateBlockingSourceOperator::is_finished();
    }
    if (_accumulator.has_output()) {
        return false;
    }
    if (_aggregator->spiller()->is_cancel()) {
        return true;
    }
    return _aggregator->is_spilled_eos() && !_has_last_chunk;
}

Status SpillableAggregateBlockingSourceOperator::set_finishing(RuntimeState* state) {
    if (state->is_cancelled()) {
        _aggregator->spiller()->cancel();
    }
    return Status::OK();
}

Status SpillableAggregateBlockingSourceOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    RETURN_IF_ERROR(AggregateBlockingSourceOperator::set_finished(state));
    return Status::OK();
}

StatusOr<ChunkPtr> SpillableAggregateBlockingSourceOperator::pull_chunk(RuntimeState* state) {
    if (!_aggregator->spiller()->spilled()) {
        return AggregateBlockingSourceOperator::pull_chunk(state);
    }
    ASSIGN_OR_RETURN(auto res, _pull_spilled_chunk(state));

    if (res != nullptr) {
        const int64_t old_size = res->num_rows();
        RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_stream_aggregator->conjunct_ctxs(), res.get()));
        _stream_aggregator->update_num_rows_returned(-(old_size - static_cast<int64_t>(res->num_rows())));
    }

    return res;
}

Status SpillableAggregateBlockingSourceOperator::reset_state(RuntimeState* state,
                                                             const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    _has_last_chunk = true;
    _accumulator.reset_state();
    return Status::OK();
}

StatusOr<ChunkPtr> SpillableAggregateBlockingSourceOperator::_pull_spilled_chunk(RuntimeState* state) {
    DCHECK(_accumulator.need_input());
    ChunkPtr res;

    if (_accumulator.has_output()) {
        auto accumulated = std::move(_accumulator.pull());
        return accumulated;
    }

    auto& spiller = _aggregator->spiller();

    if (!_aggregator->is_spilled_eos()) {
        DCHECK(_accumulator.need_input());
        auto executor = _aggregator->spill_channel()->io_executor();
        ASSIGN_OR_RETURN(auto chunk,
                         spiller->restore(state, *executor, TRACKER_WITH_SPILLER_READER_GUARD(state, spiller)));
        if (chunk->is_empty()) {
            return chunk;
        }
        RETURN_IF_ERROR(_stream_aggregator->evaluate_groupby_exprs(chunk.get()));
        RETURN_IF_ERROR(_stream_aggregator->evaluate_agg_fn_exprs(chunk.get(), true));
        ASSIGN_OR_RETURN(res, _stream_aggregator->streaming_compute_agg_state(chunk->num_rows(), false));
        _accumulator.push(std::move(res));

    } else if (_has_last_chunk) {
        DCHECK(_accumulator.need_input());
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

Status SpillableAggregateBlockingSourceOperatorFactory::prepare(RuntimeState* state) {
    _stream_aggregator_factory = std::make_shared<StreamingAggregatorFactory>(_hash_aggregator_factory->t_node());
    _stream_aggregator_factory->set_aggr_mode(_hash_aggregator_factory->aggr_mode());
    return Status::OK();
}

OperatorPtr SpillableAggregateBlockingSourceOperatorFactory::create(int32_t degree_of_parallelism,
                                                                    int32_t driver_sequence) {
    return std::make_shared<SpillableAggregateBlockingSourceOperator>(
            _hash_aggregator_factory->get_or_create(driver_sequence),
            _stream_aggregator_factory->get_or_create(driver_sequence), this, _id, _plan_node_id, driver_sequence);
}
} // namespace starrocks::pipeline