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

#pragma once

#include <mutex>
#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/sort/sort_context.h"
#include "exec/pipeline/source_operator.h"
#include "exec/sort_exec_exprs.h"
#include "exec/sorting/merge_path.h"

namespace starrocks::pipeline {
class SortContext;

// +---------------------------+               +--------------------------------------+
// | PartitionSortSinkOperator | ------------> | LocalParallelMergeSortSourceOperator | ------------> output streams
// +---------------------------+               +--------------------------------------+
// +---------------------------+               +--------------------------------------+
// | PartitionSortSinkOperator | ------------> | LocalParallelMergeSortSourceOperator | no output stream
// +---------------------------+               +--------------------------------------+
// +---------------------------+               +--------------------------------------+
// | PartitionSortSinkOperator | ------------> | LocalParallelMergeSortSourceOperator | no output stream
// +---------------------------+               +--------------------------------------+
//
// There will be as many LocalParallelMergeSortSourceOperators as degree of parallelism, and
// all the LocalParallelMergeSortSourceOperators will be involved in the parallel merge processing,
// and only one LocalParallelMergeSortSourceOperator, the operator with driver_sequence = 0 for simplicity,
// will output data.
//
// All the parallel merge processing is organized in the component named MergePathCascadeMerger, which
// can be easily integrated into the pipeline engine. The only thing for this operator need to to is to call
// the method MergePathCascadeMerger::try_get_next.
//
class LocalParallelMergeSortSourceOperator final : public SourceOperator {
public:
    LocalParallelMergeSortSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                         int32_t driver_sequence, SortContext* sort_context,
                                         merge_path::MergePathCascadeMerger* merge_path_merger)
            : SourceOperator(factory, id, "local_parallel_merge_source", plan_node_id, driver_sequence),
              _sort_context(sort_context),
              _merger(merge_path_merger) {}

    ~LocalParallelMergeSortSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    void add_morsel(Morsel* morsel) {}

    Status set_finished(RuntimeState* state) override;

private:
    bool _is_finished = false;
    SortContext* _sort_context;
    merge_path::MergePathCascadeMerger* _merger;
};

class LocalParallelMergeSortSourceOperatorFactory final : public SourceOperatorFactory {
public:
    LocalParallelMergeSortSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                std::shared_ptr<SortContextFactory> sort_context_factory)
            : SourceOperatorFactory(id, "local_parallel_merge_source", plan_node_id),
              _sort_context_factory(std::move(sort_context_factory)) {}

    ~LocalParallelMergeSortSourceOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    RuntimeState* _state;

    // share data with multiple partition sort sink opeartor through _sort_context.
    std::shared_ptr<SortContextFactory> _sort_context_factory;
    std::unique_ptr<merge_path::MergePathCascadeMerger> _merger;
};

} // namespace starrocks::pipeline
