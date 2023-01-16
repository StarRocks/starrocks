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

#include "exec/pipeline/chunk_accumulate_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status ChunkAccumulateOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _acc.push(chunk);
    return Status::OK();
}

StatusOr<ChunkPtr> ChunkAccumulateOperator::pull_chunk(RuntimeState*) {
    return std::move(_acc.pull());
}

Status ChunkAccumulateOperator::set_finishing(RuntimeState* state) {
    _acc.finalize();
    return Status::OK();
}

Status ChunkAccumulateOperator::set_finished(RuntimeState*) {
    _acc.reset();
    _acc.finalize();

    return Status::OK();
}

Status ChunkAccumulateOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _acc.reset_state();

    return Status::OK();
}

} // namespace starrocks::pipeline
