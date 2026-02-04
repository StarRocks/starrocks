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

#include "exec/spill/block_manager.h"
#include "exec/spill/data_stream.h"
#include "exec/spill/spiller_factory.h"
#include "util/runtime_profile.h"

namespace starrocks {

class LoadChunkSpiller;
class ChunkIterator;
using ChunkIteratorPtr = std::shared_ptr<ChunkIterator>;

namespace spill {
class BlockGroup;
} // namespace spill

namespace lake {
class TabletInternalParallelMergeTask;
class TabletWriter;
} // namespace lake

/**
 * Encapsulates a single merge task unit for pipeline execution.
 *
 * WHY THIS STRUCT: Groups all resources needed for one merge task to be executed
 * independently in a pipeline operator. The block_groups ownership is critical.
 */
struct LoadSpillPipelineMergeTask {
    // LIFETIME MANAGEMENT: Holds shared ownership of block groups to prevent premature
    // destruction. The merge_itr reads from these block groups asynchronously during
    // pipeline execution, so block groups must outlive the iterator. Without this,
    // we'd have use-after-free bugs when blocks are reclaimed before iterator finishes.
    std::vector<std::shared_ptr<spill::BlockGroup>> block_groups;

    // Iterator that performs the actual merge of spilled data chunks. Supports both
    // sorted merge (for aggregation/ordering) and union (for DUP_KEYS tables).
    ChunkIteratorPtr merge_itr;

    // Metrics for monitoring merge workload distribution across pipeline tasks.
    // Used to ensure roughly equal work distribution and for performance analysis.
    size_t total_block_groups = 0;
    size_t total_block_bytes = 0;

    // Release block group in advance to free load spill disk space
    void release_block_groups() { block_groups.clear(); }
};

/**
 * Iterator that generates parallel merge tasks from spilled block groups.
 *
 * WHY ITERATOR PATTERN: Enables lazy task generation and natural integration with
 * pipeline execution model. Instead of creating all merge tasks upfront (high memory),
 * we generate tasks on-demand as pipeline operators become available. This provides
 * better memory control and load balancing.
 *
 * MERGE ROUNDS EXPLAINED:
 * - Non-final round: Merges spilled blocks back to fewer, larger blocks (reduce fanout)
 * - Final round: Merges spilled blocks directly to tablet writer (final destination)
 *
 * This multi-round strategy prevents exceeding max file descriptor limits and reduces
 * merge complexity when dealing with huge datasets split across thousands of blocks.
 */
class LoadSpillPipelineMergeIterator {
public:
    /**
     * @param spiller - Source of spilled block groups to merge
     * @param parent_writer - Tablet writer to clone for each parallel task
     * @param quit_flag - Shared cancellation flag for all generated tasks
     * @param final_round - If true, merge directly to tablet; if false, merge to intermediate blocks
     */
    LoadSpillPipelineMergeIterator(LoadChunkSpiller* spiller, lake::TabletWriter* parent_writer,
                                   std::atomic<bool>* quit_flag, bool final_round);
    ~LoadSpillPipelineMergeIterator() = default;

    // Returns current merge task, or nullptr if iteration exhausted
    std::shared_ptr<lake::TabletInternalParallelMergeTask> current_task() { return _current_task; }

    // Initialize iterator by generating first task
    void init() { _next(); }

    // Advance to next task (generates new task on-demand)
    void next() { _next(); }

    // Check if more tasks are available. Pipeline operators use this for work scheduling.
    bool has_more() const { return _current_task != nullptr && _status.ok(); }

    // Returns error status if task generation failed (e.g., OOM, I/O error)
    const Status& status() const { return _status; }

private:
    void _next();

    /**
     * Generates next merge task by pulling block groups from spiller.
     *
     * BATCHING STRATEGY: Groups multiple block groups into single task based on
     * config::load_spill_max_merge_bytes to balance parallelism vs merge efficiency.
     * Too small = excessive overhead; too large = poor load balancing.
     */
    Status _generate_next_task();

    LoadChunkSpiller* _spiller = nullptr;
    lake::TabletWriter* _parent_writer = nullptr;

    // Shared quit flag for cancellation support across all tasks
    std::atomic<bool>* _quit_flag = nullptr;

    // If true, this is the final merge round writing to tablet. If false, intermediate
    // round writing back to spill blocks for next iteration.
    bool _final_round = false;

    // WHY NEEDED: Determines merge iterator type. DUP_KEYS tables use union iterator
    // (simple concatenation), while AGG/UNIQUE keys use merge iterator (sorted merge
    // with aggregation). This optimization saves CPU cycles for duplicate key tables
    // that don't need ordering/deduplication.
    bool _do_agg = false;

    // Currently available task for pipeline operator to consume
    std::shared_ptr<lake::TabletInternalParallelMergeTask> _current_task;

    // Tracks errors during task generation (sticky - once failed, stays failed)
    Status _status;
};

} // namespace starrocks