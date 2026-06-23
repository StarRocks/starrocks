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

#include "exec/pipeline/sort/local_merge_sort_source_operator.h"

#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status LocalMergeSortSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _sort_context->ref();
    _sort_context->attach_source_observer(state, observer());
    // Subscribe to every partition spiller's source list so flush-all ("partition ready") and restore
    // completions of any partition wake this INPUT_EMPTY sleeper. The pip-list subscription above only
    // carries the non-spill "partition ready" axis; on the spill path that wakeup is emitted by the
    // partition's spiller, which the source is otherwise not subscribed to. N subscriptions, one per spilled
    // partition.
    _sort_context->subscribe_source_to_spillers(state, observer());
    return Status::OK();
}
void LocalMergeSortSourceOperator::close(RuntimeState* state) {
    _sort_context->unref(state);
    Operator::close(state);
}

StatusOr<ChunkPtr> LocalMergeSortSourceOperator::pull_chunk(RuntimeState* state) {
    return _sort_context->pull_chunk();
}

Status LocalMergeSortSourceOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

Status LocalMergeSortSourceOperator::set_finished(RuntimeState* state) {
    auto defer = _sort_context->defer_notify_sink();
    _sort_context->cancel();
    return _sort_context->set_finished();
}

BlockReason LocalMergeSortSourceOperator::block_reason() const {
    // Parked INPUT_EMPTY exactly when has_output() == false and not finished. The only non-terminal gates of
    // has_output() (is_partition_sort_finished / is_partition_ready) are both woken by the partition
    // spillers' flush-all / restore completions, which prepare()'s subscribe_source_to_spillers covers ->
    // WAIT_RESTORE. Runnable or finished -> NONE. Both returns go through named<R, kCoveredWakeups>(), so a
    // reason outside this source's coverage mask would not compile.
    if (_is_finished || is_finished()) {
        return named<BlockReason::NONE, kCoveredWakeups>();
    }
    if (has_output()) {
        return named<BlockReason::NONE, kCoveredWakeups>();
    }
    return named<BlockReason::WAIT_RESTORE, kCoveredWakeups>();
}

bool LocalMergeSortSourceOperator::has_output() const {
    // A partition spiller's task error is observed only here: it arrives as a source wakeup but neither
    // is_partition_ready() nor is_finished() reflect it, so report ready to let pull_chunk() propagate it
    // instead of re-parking WAIT_RESTORE forever. The agg/NLJ analogue is RETURN_TRUE_IF_SPILL_TASK_ERROR
    // (one spiller); sort scans its N partition spillers and reuses the Status in pull_chunk().
    if (!_sort_context->spiller_task_status().ok()) {
        return true;
    }
    return _sort_context->is_partition_sort_finished() && !_sort_context->is_output_finished() &&
           _sort_context->is_partition_ready();
}

bool LocalMergeSortSourceOperator::is_finished() const {
    return _sort_context->is_partition_sort_finished() && _sort_context->is_output_finished();
}
OperatorPtr LocalMergeSortSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto sort_context = _sort_context_factory->create(driver_sequence);
    return std::make_shared<LocalMergeSortSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                          sort_context.get());
}

} // namespace starrocks::pipeline
