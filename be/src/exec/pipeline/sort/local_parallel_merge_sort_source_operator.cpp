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
    _merger->bind_profile(_merge_parallel_id, _unique_metrics.get());
    return Status::OK();
}

void LocalParallelMergeSortSourceOperator::close(RuntimeState* state) {
    _sort_context->unref(state);
    Operator::close(state);
}

bool LocalParallelMergeSortSourceOperator::has_output() const {
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
    ChunkPtr chunk = _merger->try_get_next(_merge_parallel_id);

    if (_merger->is_finished()) {
        _is_finished = true;
    }

    return chunk;
}

Status LocalParallelMergeSortSourceOperator::set_finished(RuntimeState* state) {
    _sort_context->cancel();
    return _sort_context->set_finished();
}

Status LocalParallelMergeSortSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperatorFactory::prepare(state));
    _state = state;
    return Status::OK();
}

OperatorPtr LocalParallelMergeSortSourceOperatorFactory::create(int32_t degree_of_parallelism,
                                                                int32_t driver_sequence) {
    auto sort_context = _sort_context_factory->create(driver_sequence);

    auto chunk_provider_factory = [](ChunksSorter* chunks_sorter) {
        return ([chunks_sorter](bool only_check_if_has_data, ChunkPtr* chunk, bool* eos) {
            if (!chunks_sorter->has_output()) {
                return false;
            }
            if (!only_check_if_has_data) {
                // @TODO should handle error
                (void)chunks_sorter->get_next(chunk, eos);
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
                    chunk_providers));
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
