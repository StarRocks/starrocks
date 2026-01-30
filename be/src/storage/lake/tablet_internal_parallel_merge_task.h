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

#include "common/status.h"
#include "util/runtime_profile.h"
#include "util/threadpool.h"

namespace starrocks {

class ChunkIterator;
class MemTracker;
class Schema;
class LoadSpillPipelineMergeTask;

namespace lake {

class TabletWriter;

/**
 * Runnable task that executes a single merge operation in parallel.
 *
 * OWNERSHIP MODEL CHANGE (for pipeline execution):
 * - Takes unique_ptr ownership of both writer and task (previously raw pointers)
 * - WHY: Enables true parallel execution where each task owns its resources independently
 * - LIFETIME: Task holds all resources until completion, then transfers writer back via
 *   writer() accessor for result merging in LoadSpillPipelineMergeContext
 *
 * THREAD SAFETY:
 * - Each task operates on independent writer clone and task data (no shared state)
 * - Only _quit_flag is shared (atomic for safe concurrent access)
 * - Status updates are thread-safe via atomic operations in Status class
 */
class TabletInternalParallelMergeTask : public Runnable {
public:
    /**
     * @param writer - Cloned writer for this task (takes ownership)
     * @param task - Merge task containing iterator and block groups (takes ownership)
     * @param schema - Table schema (borrowed, outlives task)
     * @param quit_flag - Shared cancellation flag (nullptr or points to context's atomic)
     * @param write_io_timer - Shared I/O metrics counter (borrowed)
     */
    TabletInternalParallelMergeTask(std::unique_ptr<TabletWriter> writer,
                                    std::unique_ptr<LoadSpillPipelineMergeTask> task, const Schema* schema,
                                    std::atomic<bool>* quit_flag, RuntimeProfile::Counter* write_io_timer);

    ~TabletInternalParallelMergeTask();

    /**
     * Executes merge: reads from task's iterator, writes to writer.
     * Checks quit_flag periodically for early termination.
     */
    void run() override;

    void cancel() override;

    /**
     * Update task status and notify other tasks to quit if error occurred.
     * Thread-safe: can be called from any thread.
     */
    void update_status(const Status& st);

    const Status& status() const { return _status; }

    /**
     * Returns writer ownership for result consolidation.
     * Called after task completes to merge results into parent writer.
     */
    const std::unique_ptr<TabletWriter>& writer() const { return _writer; }

private:
    // Owned writer clone for independent parallel writes
    std::unique_ptr<TabletWriter> _writer;

    // Owned merge task containing iterator and block groups.
    // Ownership ensures block groups aren't destroyed before iterator finishes reading.
    std::unique_ptr<LoadSpillPipelineMergeTask> _task;

    // Memory tracker for this merge operation
    std::unique_ptr<MemTracker> _merge_mem_tracker;

    // Borrowed schema pointer (valid for task lifetime)
    const Schema* _schema = nullptr;

    // Task execution result
    Status _status;

    // Shared quit flag for cancellation (may be nullptr if cancellation not supported).
    // When set to true by any task, all tasks abort early.
    std::atomic<bool>* _quit_flag = nullptr;

    // Shared I/O metrics counter (borrowed)
    RuntimeProfile::Counter* _write_io_timer = nullptr;
};

} // namespace lake
} // namespace starrocks