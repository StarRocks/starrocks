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
#include "compute_env/spill/mem_tracker_guard.h"
#include "compute_env/spill/spiller.h"
#include "exec/pipeline/aggregate/aggregate_blocking_source_operator.h"

namespace starrocks::pipeline {
Status SpillableAggregateBlockingSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBlockingSourceOperator::prepare(state));
    RETURN_IF_ERROR(_stream_aggregator->prepare(state, _unique_metrics.get()));
    RETURN_IF_ERROR(_stream_aggregator->open(state));
    _accumulator.set_max_size(state->chunk_size());

    // Subscribe this source driver to the spiller's source list so restore /
    // flush-all completions wake the INPUT_EMPTY sleeper. Unconditional: the
    // poller-mode gate lives inside subscribe_source. observer() is valid here.
    _aggregator->spiller()->observable().subscribe_source(state, observer());
    return Status::OK();
}

std::optional<BlockReason> SpillableAggregateBlockingSourceOperator::_blocked_on() const {
    // A finished or non-spilled source is not parked on a spill block (the non-spilled base output is
    // routed by has_output() above this). A failed spiller is runnable -- the run carries the status out
    // through the RETURN_TRUE_IF_SPILL_TASK_ERROR guard, not a reason -> nullopt. Buffered accumulator
    // output, spiller output data, or the eos chunk are all runnable (has_output() returns true on the
    // spilled branch). Otherwise the source is waiting on the restore IO the source-list subscription is
    // woken by -> WAIT_RESTORE. block_reason() reads this only when parked (has_output() == false), where
    // none of the runnable conditions hold, so the spilled-branch answer matches has_output()'s spilled branch.
    if (_is_finished || !_aggregator->spiller()->spilled()) {
        return std::nullopt;
    }
    if (!_aggregator->spiller()->task_status().ok()) {
        return std::nullopt;
    }
    if (_accumulator.has_output() || _aggregator->spiller()->has_output_data() ||
        (_aggregator->is_spilled_eos() && _has_last_chunk)) {
        return std::nullopt;
    }
    // Passed through named<R, kCoveredWakeups>(): a restore park outside this source's source-list coverage
    // mask does not compile, so the value spill_source_block_reason() returns is always a covered reason.
    return named<BlockReason::WAIT_RESTORE, kCoveredWakeups>();
}

BlockReason SpillableAggregateBlockingSourceOperator::block_reason() const {
    return spill_source_block_reason();
}

void SpillableAggregateBlockingSourceOperator::close(RuntimeState* state) {
    AggregateBlockingSourceOperator::close(state);
    _stream_aggregator->close(state);
    DCHECK(is_finished());
    DCHECK(!has_output());
}

bool SpillableAggregateBlockingSourceOperator::has_output() const {
    if (_is_finished) {
        return false;
    }
    if (!_aggregator->spiller()->spilled()) {
        return AggregateBlockingSourceOperator::has_output();
    }
    // Spilled: runnable unless parked on restore IO. _blocked_on() puts the buffered-output / spiller-
    // output-data / error / eos checks in its runnable (nullopt) branch, so the source has output here
    // exactly when it is not blocked on WAIT_RESTORE.
    return !_blocked_on().has_value();
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
    RETURN_IF_ERROR(_aggregator->spiller()->task_status());
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
    ChunkPtr res;

    if (_accumulator.has_output()) {
        auto accumulated = std::move(_accumulator.pull());
        return accumulated;
    }

    auto& spiller = _aggregator->spiller();

    if (!_aggregator->is_spilled_eos()) {
        DCHECK(_accumulator.need_input());
        ASSIGN_OR_RETURN(auto chunk, spiller->restore(state, TRACKER_WITH_SPILLER_READER_GUARD(state, spiller)));
        if (chunk->is_empty()) {
            return chunk;
        }
        RETURN_IF_ERROR(_stream_aggregator->evaluate_groupby_exprs(chunk.get()));
        RETURN_IF_ERROR(_stream_aggregator->evaluate_agg_fn_exprs(chunk.get(), true));
        ASSIGN_OR_RETURN(res, _stream_aggregator->streaming_compute_agg_state(chunk->num_rows(), false));
        _accumulator.push(res);

    } else if (_has_last_chunk) {
        DCHECK(_accumulator.need_input());
        _has_last_chunk = false;
        ASSIGN_OR_RETURN(res, _stream_aggregator->pull_eos_chunk());
        if (res != nullptr && !res->is_empty()) {
            _accumulator.push(res);
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
