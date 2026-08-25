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

#include "spillable_analytic_sink_operator.h"

#include "compute_env/query/fragment_runtime_state.h"
#include "compute_env/query/query_runtime_state.h"
#include "exec/pipeline/query_context.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status SpillableAnalyticSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AnalyticSinkOperator::prepare(state));
    // Analytor::prepare drops the streams when the plan shape is not eligible.
    if (_analytor->partition_streams_enabled()) {
        RETURN_IF_ERROR(_analytor->input_run()->init_metrics(_unique_metrics.get()));
        _unique_metrics->add_info_string("AnalyticSpill", "true");
    }
    return Status::OK();
}

Status SpillableAnalyticSinkOperator::set_finishing(RuntimeState* state) {
    if (!_analytor->partition_streams_enabled()) {
        return AnalyticSinkOperator::set_finishing(state);
    }

    auto notify = _analytor->defer_notify_source();
    _is_finished = true;

    if (state->is_cancelled()) {
        _analytor->descriptor_queue().close_producer();
        _analytor->input_run()->close_producer();
        return Status::OK();
    }
    // Bounded work, no IO barrier: seal the open partition, publish the last
    // descriptors and close both producers.
    return _analytor->store_finish_input(state);
}

Status SpillableAnalyticSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AnalyticSinkOperatorFactory::prepare(state));
    _state = state;
    // The memory limit IS the spill policy: force flushes everything, auto
    // keeps blocks resident up to the threshold. max_unconsumed_bytes must be
    // unbounded on both streams: the source can only consume rows whose
    // partition sealed, so an open partition must keep writing (and spilling)
    // without waiting for consumption — a bounded unconsumed gate would
    // deadlock on the first partition larger than it. The sealed backlog is
    // bounded by the analytor's ready-backlog limits instead.
    _input_run_options.plan_node_id = _plan_node_id;
    _input_run_options.encode_level = state->spill_encode_level();
    _input_run_options.block_manager = state->query_runtime_state()->query_spill_manager()->block_manager();
    _input_run_options.wg = state->fragment_runtime_state()->workgroup();
    _input_run_options.max_unconsumed_bytes = std::numeric_limits<size_t>::max();
    if (state->spill_mode() == TSpillMode::FORCE) {
        _input_run_options.memory_limit = 0;
    } else {
        _input_run_options.memory_limit =
                static_cast<size_t>(state->spill_mem_table_size()) * std::max(1, state->spill_mem_table_num());
    }
    // The descriptor side needs no options: it is a dedicated in-memory queue
    // owned by the analytor (see AnalyticDescriptorQueue) — it only holds
    // sealed-partition records, which the source can always drain, so its
    // capacity is the analytor's ready-backlog limits, not a spill policy.
    return Status::OK();
}

OperatorPtr SpillableAnalyticSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto analytor = _analytor_factory->create(driver_sequence);
    if (!analytor->partition_streams_enabled()) {
        auto input_run = std::make_shared<MemLimitedChunkQueue>(_state, 1, _input_run_options);
        // Register both ends at construction time (single-threaded pipeline
        // build phase): the source pipeline may poll before the sink
        // pipeline's prepare runs, and an unopened producer would read as a
        // premature end-of-stream. The descriptor queue needs no
        // registration — it is a plain member of the analytor.
        input_run->open_producer();
        input_run->open_consumer(0);
        analytor->set_input_run(std::move(input_run));
    }
    return std::make_shared<SpillableAnalyticSinkOperator>(this, _id, _plan_node_id, driver_sequence, _tnode,
                                                           std::move(analytor));
}

} // namespace starrocks::pipeline
