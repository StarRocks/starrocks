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

#include "analytic_sink_operator.h"

#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status AnalyticSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    // _analytor is shared by sink operator and source operator
    // we must only prepare it at sink operator
    RETURN_IF_ERROR(_analytor->prepare(state, state->obj_pool(), _unique_metrics.get()));
    RETURN_IF_ERROR(_analytor->open(state));

    return Status::OK();
}

void AnalyticSinkOperator::close(RuntimeState* state) {
    _analytor->unref(state);
    Operator::close(state);
}

Status AnalyticSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    // skip processing if cancelled
    if (state->is_cancelled()) {
        return Status::OK();
    }

    return _analytor->finish_process(state);
}

StatusOr<ChunkPtr> AnalyticSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status AnalyticSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _analytor->process(state, chunk);
}

} // namespace starrocks::pipeline
