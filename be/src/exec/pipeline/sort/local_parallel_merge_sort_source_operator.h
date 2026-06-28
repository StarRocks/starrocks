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
#include "compute_env/sorting/merge_path.h"
#include "exec/pipeline/primitives/block_reason.h"
#include "exec/pipeline/sort/sort_context.h"
#include "exec/pipeline/source_operator.h"
#include "exprs/sort_exec_exprs.h"

namespace starrocks::pipeline {
class ScanMorsel;
using Morsel = ScanMorsel;
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
                                         int32_t driver_sequence, SortContext* sort_context, bool is_gathered,
                                         merge_path::MergePathCascadeMerger* merge_path_merger)
            : SourceOperator(factory, id, "local_parallel_merge_source", plan_node_id, false, driver_sequence),
              _sort_context(sort_context),
              _merger(merge_path_merger),
              _merge_parallel_id(is_gathered ? driver_sequence : 0) {}

    ~LocalParallelMergeSortSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_mutable() const override { return true; }

    bool is_finished() const override;

    // Mixed park model: the restore axis (still building, or a spilled partition not yet restored so a leaf
    // chunk provider is pending) is woken by the partition spillers' source list that
    // subscribe_source_to_spillers covers -> WAIT_RESTORE; the merge-path stage-coordination axis is woken by
    // the merger's own observable, not a spiller (no WAIT_MERGE in the enum) -> NONE. covered_wakeups()
    // declares only the spiller-covered reason; block_reason() names restore whenever a partition could still
    // send the source a spiller wakeup. Note: this source is is_mutable(), so it parks LOCAL_WAITING (busy
    // local-queue re-poll) and is promoted to INPUT_EMPTY -- where the park-time check reads this net -- only
    // after the local-wait timeout; the net therefore arms on restore-parks that outlast the spin.
    BlockReason block_reason() const override;
    static constexpr uint32_t kCoveredWakeups = block_reason_bit(BlockReason::WAIT_RESTORE);
    uint32_t covered_wakeups() const override { return kCoveredWakeups; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    void add_morsel(Morsel* morsel) {}

    Status set_finished(RuntimeState* state) override;

private:
    SortContext* const _sort_context;
    merge_path::MergePathCascadeMerger* const _merger;
    const int32_t _merge_parallel_id;
    bool _is_finished = false;
};

static_assert(LocalParallelMergeSortSourceOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_RESTORE));

class LocalParallelMergeSortSourceOperatorFactory final : public SourceOperatorFactory {
public:
    LocalParallelMergeSortSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                std::shared_ptr<SortContextFactory> sort_context_factory)
            : SourceOperatorFactory(id, "local_parallel_merge_source", plan_node_id),
              _sort_context_factory(std::move(sort_context_factory)) {}

    ~LocalParallelMergeSortSourceOperatorFactory() override = default;
    bool support_event_scheduler() const override { return true; }

    Status prepare(RuntimeState* state) override;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    void set_tuple_desc(const TupleDescriptor* tuple_desc) { _tuple_desc = tuple_desc; }
    void set_is_gathered(const bool is_gathered) { _is_gathered = is_gathered; }
    void set_materialized_mode(TLateMaterializeMode::type mode) { _late_materialize_mode = mode; }

private:
    const TupleDescriptor* _tuple_desc;
    bool _is_gathered = true;
    RuntimeState* _state;
    TLateMaterializeMode::type _late_materialize_mode = TLateMaterializeMode::AUTO;

    // share data with multiple partition sort sink opeartor through _sort_context.
    std::shared_ptr<SortContextFactory> _sort_context_factory;
    std::vector<std::unique_ptr<merge_path::MergePathCascadeMerger>> _mergers;
};

} // namespace starrocks::pipeline
