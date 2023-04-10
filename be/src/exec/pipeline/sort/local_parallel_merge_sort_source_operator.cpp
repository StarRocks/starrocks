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

#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status LocalParallelMergeSortSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _sort_context->ref();
    _merger->bind_profile(_driver_sequence, _unique_metrics.get());
    return Status::OK();
}

void LocalParallelMergeSortSourceOperator::close(RuntimeState* state) {
    _sort_context->unref(state);
    Operator::close(state);
}

StatusOr<ChunkPtr> LocalParallelMergeSortSourceOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr chunk = _merger->try_get_next(_driver_sequence);

    if (_merger->is_finished()) {
        _is_finished = true;
    }

    return chunk;
}

Status LocalParallelMergeSortSourceOperator::set_finished(RuntimeState* state) {
    if (_driver_sequence == 0) {
        return _sort_context->set_finished();
    }
    return Status::OK();
}

bool LocalParallelMergeSortSourceOperator::has_output() const {
    if (!_sort_context->is_partition_sort_finished()) {
        return false;
    }
    if (_is_finished) {
        return false;
    }
    if (_merger->is_current_stage_finished(_driver_sequence)) {
        return false;
    }
    return true;
}

bool LocalParallelMergeSortSourceOperator::is_finished() const {
    return _is_finished;
}

Status LocalParallelMergeSortSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperatorFactory::prepare(state));
    _state = state;
    return Status::OK();
}

OperatorPtr LocalParallelMergeSortSourceOperatorFactory::create(int32_t degree_of_parallelism,
                                                                int32_t driver_sequence) {
    auto sort_context = _sort_context_factory->create(driver_sequence);

    if (_merger == nullptr) {
        std::vector<merge_path::MergePathChunkProvider> chunk_providers;
        for (int i = 0; i < degree_of_parallelism; i++) {
            auto* chunks_sorter = sort_context->get_chunks_sorter(i);
            DCHECK(chunks_sorter != nullptr);
            chunk_providers.emplace_back([chunks_sorter](bool only_check_if_has_data, ChunkPtr* chunk, bool* eos) {
                if (only_check_if_has_data) {
                    return true;
                }
                chunks_sorter->get_next(chunk, eos);
                return true;
            });
        }
        _merger = std::make_unique<merge_path::MergePathCascadeMerger>(
                degree_of_parallelism, sort_context->sort_exprs(), sort_context->sort_descs(), chunk_providers,
                _state->chunk_size());
    }

    return std::make_shared<LocalParallelMergeSortSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                  sort_context.get(), _merger.get());
}

} // namespace starrocks::pipeline
