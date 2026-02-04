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

#include "storage/load_spill_pipeline_merge_context.h"

#include "storage/lake/tablet_internal_parallel_merge_task.h"
#include "storage/lake/tablet_writer.h"
#include "storage/load_spill_block_manager.h"
#include "storage/storage_engine.h"
#include "util/threadpool.h"

namespace starrocks {

LoadSpillPipelineMergeContext::~LoadSpillPipelineMergeContext() {
    _quit_flag.store(true);
    if (_token != nullptr) {
        _token->shutdown();
    }
}

void LoadSpillPipelineMergeContext::create_thread_pool_token() {
    std::lock_guard<std::mutex> lg(_merge_tasks_mutex);
    if (_token == nullptr) {
        _token = StorageEngine::instance()
                         ->load_spill_block_merge_executor()
                         ->create_tablet_internal_parallel_merge_token();
    }
}

void LoadSpillPipelineMergeContext::add_merge_task(const std::shared_ptr<lake::TabletInternalParallelMergeTask>& task) {
    // THREAD SAFETY: Lock required because multiple pipeline operators may concurrently
    // register tasks. std::vector is not thread-safe for concurrent push_back operations.
    // Lock scope is minimal (only protects vector modification, not task creation/execution)
    // to avoid serialization bottleneck during parallel task generation.
    std::lock_guard<std::mutex> lg(_merge_tasks_mutex);
    _merge_tasks.push_back(task);
}

Status LoadSpillPipelineMergeContext::merge_task_results() {
    // Hold lock during entire merge operation to prevent concurrent add_merge_task() calls
    // while iterating. This is safe because merge_task_results() is only called after
    // all tasks complete, so no new tasks should be added anyway.
    std::lock_guard<std::mutex> lg(_merge_tasks_mutex);
    if (_token != nullptr) {
        _token->wait();
    }

    for (const auto& task : _merge_tasks) {
        // IMPORTANT: Check task status first to fail fast if any parallel merge task failed.
        // This prevents wasting time merging partial results from failed operations.
        // Task failures could include: OOM during merge, disk I/O errors, user cancellation.
        RETURN_IF_ERROR(task->status());

        // Merge task's writer results into parent writer. Each task wrote to a cloned
        // writer instance during parallel execution. Now we consolidate all results.
        // NOTE: This merge is sequential (not parallel) to maintain data ordering and
        // consistency. The performance impact is acceptable because the heavy lifting
        // (sorting, aggregation, I/O) already happened in parallel during task execution.
        RETURN_IF_ERROR(_writer->merge_other_writer(task->writer().get()));
    }
    return Status::OK();
}

} // namespace starrocks