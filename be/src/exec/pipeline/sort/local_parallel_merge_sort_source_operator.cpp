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

#include "exec/pipeline/sort/local_parallel_merge_sort_source_operator.h"

#include <algorithm>
#include <sstream>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_context_cancel.h"
#include "exec/pipeline/primitives/pipeline_observer.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status LocalParallelMergeSortSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _sort_context->ref();
    _sort_context->attach_source_observer(state, observer());
    // Subscribe to every partition spiller's source list so flush-all ("partition ready") and restore
    // completions of any partition wake this INPUT_EMPTY sleeper. In the gathered branch all M source
    // drivers share one context and each subscribes, so a partition emits M notifies; the observer's atomic
    // event bitmask coalesces them. N subscriptions per driver, one per spilled partition.
    _sort_context->subscribe_source_to_spillers(state, observer());
    _merger->bind_profile(_merge_parallel_id, _unique_metrics.get());
    _merger->attach_observer(state, observer());
    return Status::OK();
}

void LocalParallelMergeSortSourceOperator::close(RuntimeState* state) {
    _sort_context->unref(state);
    Operator::close(state);
}

bool LocalParallelMergeSortSourceOperator::has_output() const {
    // Report ready on a partition spiller task error so pull_chunk() propagates it; the merger's pending/stage
    // gates never observe it. See LocalMergeSortSourceOperator::has_output().
    if (!_sort_context->spiller_task_status().ok()) {
        return true;
    }
    if (!_sort_context->is_partition_sort_finished()) {
        return false;
    }
    if (_is_finished) {
        return false;
    }
    if (_merger->is_current_stage_finished(_merge_parallel_id, false)) {
        return false;
    }
    if (_merger->is_pending(_merge_parallel_id)) {
        return false;
    }
    return true;
}

bool LocalParallelMergeSortSourceOperator::is_finished() const {
    return _is_finished;
}

StatusOr<ChunkPtr> LocalParallelMergeSortSourceOperator::pull_chunk(RuntimeState* state) {
    // Propagate the spiller task error that made has_output() report ready, before pulling from the merger.
    RETURN_IF_ERROR(_sort_context->spiller_task_status());
    ChunkPtr chunk = _merger->try_get_next(_merge_parallel_id);

    if (_merger->is_finished()) {
        _is_finished = true;
    }

    return chunk;
}

Status LocalParallelMergeSortSourceOperator::set_finished(RuntimeState* state) {
    auto defer = _sort_context->defer_notify_sink();
    _sort_context->cancel();
    return _sort_context->set_finished();
}

BlockReason LocalParallelMergeSortSourceOperator::block_reason() const {
    // Mixed park model. The restore axis is woken by the partition spillers' source list (covered by
    // prepare()'s subscribe_source_to_spillers) and has two sub-cases: still building -- a sink's flush-all
    // completion advances is_partition_sort_finished, the callback running on the spiller IO thread -- or the
    // steady state, where all sinks are done but a spilled partition is not yet restored, so a leaf chunk
    // provider stays pending (chunks_sorter->has_output() == false) until that partition's restore completion
    // fires on its spiller source list. Both are exactly !is_partition_sort_finished() || !is_partition_ready().
    // The merge-path stage-coordination axis -- all sinks done, every partition restored, the merger still
    // cascading stages -- is woken by the merger's own observable (attach_observer), not a spiller, and the
    // enum has no WAIT_MERGE, so it stays generic NONE (the merger pip wakes it, as the non-spill parallel
    // sort already runs). A false WAIT_RESTORE on a drained merge-park is harmless (still in the covered mask);
    // a false NONE on a restore-park would silently leave the wakeup net unarmed, so name restore whenever
    // any partition could still send the source a spiller wakeup.
    // Every return passes through named<R, kCoveredWakeups>(): WAIT_RESTORE must sit in this source's
    // coverage mask or it does not compile; NONE is the always-legal "not parked" answer.
    if (_is_finished || is_finished()) {
        return named<BlockReason::NONE, kCoveredWakeups>();
    }
    if (has_output()) {
        return named<BlockReason::NONE, kCoveredWakeups>();
    }
    if (!_sort_context->is_partition_sort_finished() || !_sort_context->is_partition_ready()) {
        return named<BlockReason::WAIT_RESTORE, kCoveredWakeups>();
    }
    return named<BlockReason::NONE, kCoveredWakeups>();
}

Status LocalParallelMergeSortSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperatorFactory::prepare(state));
    _state = state;
    return Status::OK();
}

OperatorPtr LocalParallelMergeSortSourceOperatorFactory::create(int32_t degree_of_parallelism,
                                                                int32_t driver_sequence) {
    auto sort_context = _sort_context_factory->create(driver_sequence);

    auto chunk_provider_factory = [state = _state](ChunksSorter* chunks_sorter) {
        return ([state, chunks_sorter](bool only_check_if_has_data, ChunkPtr* chunk, bool* eos) {
            if (!chunks_sorter->has_output()) {
                return false;
            }
            if (!only_check_if_has_data) {
                auto status = chunks_sorter->get_next(chunk, eos);
                // Propagate non-EOF errors instead of silently dropping them.
                // Without this, a spiller restore failure leaves the merger waiting
                // on a leaf provider that will never become ready, and the query hangs.
                if (!status.ok() && !status.is_end_of_file()) {
                    cancel_fragment_context(state->fragment_ctx(), status);
                    *eos = true;
                    return false;
                }
            }
            return true;
        });
    };

    if (_is_gathered) {
        if (_mergers.empty()) {
            std::vector<merge_path::MergePathChunkProvider> chunk_providers;
            for (int i = 0; i < degree_of_parallelism; i++) {
                auto* chunks_sorter = sort_context->get_chunks_sorter(i);
                DCHECK(chunks_sorter != nullptr);
                chunk_providers.emplace_back(chunk_provider_factory(chunks_sorter));
            }
            _mergers.push_back(std::make_unique<merge_path::MergePathCascadeMerger>(
                    _state->chunk_size(), degree_of_parallelism, sort_context->sort_exprs(), sort_context->sort_descs(),
                    _tuple_desc, sort_context->topn_type(), sort_context->offset(), sort_context->limit(),
                    chunk_providers, _late_materialize_mode));
        }
        return std::make_shared<LocalParallelMergeSortSourceOperator>(
                this, _id, _plan_node_id, driver_sequence, sort_context.get(), _is_gathered, _mergers[0].get());
    } else {
        std::vector<merge_path::MergePathChunkProvider> chunk_providers;
        auto* chunks_sorter = sort_context->get_chunks_sorter(0);
        DCHECK(chunks_sorter != nullptr);
        chunk_providers.emplace_back(chunk_provider_factory(chunks_sorter));
        _mergers.push_back(std::make_unique<merge_path::MergePathCascadeMerger>(
                _state->chunk_size(), 1, sort_context->sort_exprs(), sort_context->sort_descs(), _tuple_desc,
                sort_context->topn_type(), sort_context->offset(), sort_context->limit(), chunk_providers));
        return std::make_shared<LocalParallelMergeSortSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                      sort_context.get(), _is_gathered,
                                                                      _mergers[driver_sequence].get());
    }
}

} // namespace starrocks::pipeline
