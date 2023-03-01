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
    _sort_context->cancel();
    return _sort_context->set_finished();
}

bool LocalMergeSortSourceOperator::has_output() const {
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
