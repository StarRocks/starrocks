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

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "common/status.h"

namespace starrocks {

namespace lake {
class TabletInternalParallelMergeTask;
class TabletWriter;
} // namespace lake

/**
 * Context for managing parallel merge tasks in pipeline execution during load spill operations.
 *
 * WHY THIS EXISTS: During large data loads, memory pressure causes data to spill to disk.
 * The merge phase (reading spilled data and writing to final tablet) needs to support pipeline
 * execution for better parallelism and resource utilization. This context collects merge tasks
 * from multiple pipeline operators and coordinates their final merge into the tablet writer.
 *
 * DESIGN RATIONALE:
 * - Each merge task operates on a subset of spilled block groups using a cloned writer
 * - Tasks can execute in parallel without lock contention during the merge phase
 * - Final merge_task_results() sequentially merges all task results into the parent writer
 * - This two-phase approach (parallel execution + serial merge) maximizes throughput while
 *   maintaining data correctness
 *
 * THREAD SAFETY: add_merge_task() is thread-safe and can be called from multiple pipeline
 * operators concurrently. The mutex only protects vector modification, not task execution.
 */
class LoadSpillPipelineMergeContext {
public:
    LoadSpillPipelineMergeContext(lake::TabletWriter* writer) : _writer(writer) {}
    ~LoadSpillPipelineMergeContext() = default;

    /**
     * Register a merge task for later result collection.
     *
     * THREAD SAFETY: Can be called concurrently from multiple pipeline operators.
     * The mutex ensures safe vector modification without data races.
     */
    void add_merge_task(const std::shared_ptr<lake::TabletInternalParallelMergeTask>& task);

    /**
     * Merge results from all registered tasks into the parent writer.
     *
     * IMPORTANT: Must be called after all tasks have completed execution. This performs
     * sequential merge to maintain data ordering and consistency. The serial merge here
     * is acceptable because actual data processing (sorting, aggregation) already happened
     * in parallel during task execution.
     *
     * @return Error if any task failed or if merging fails
     */
    Status merge_task_results();

    /**
     * Provides quit flag for early termination of merge tasks.
     *
     * WHY ATOMIC: Tasks check this flag during expensive merge operations to support
     * cancellation (e.g., user abort, query timeout). Atomic ensures visibility across
     * threads without mutex overhead on the hot read path.
     */
    std::atomic<bool>* quit_flag() { return &_quit_flag; }

private:
    // Parent writer that owns the final tablet data. All task results merge into this.
    lake::TabletWriter* _writer;

    // Collection of parallel merge tasks. Each task processes a subset of spilled data
    // using a cloned writer. Vector grows as pipeline operators generate tasks.
    std::vector<std::shared_ptr<lake::TabletInternalParallelMergeTask>> _merge_tasks;

    // Protects concurrent modifications to _merge_tasks vector only. Does NOT protect
    // task execution itself, allowing true parallelism during the merge phase.
    std::mutex _merge_tasks_mutex;

    // Shared quit flag checked by all merge tasks for cancellation support.
    // Set to true when load operation is cancelled/aborted.
    std::atomic<bool> _quit_flag{false};
};

}; // namespace starrocks