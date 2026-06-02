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

#include "exec/spill/task_executor.h"

#include <any>

#include "exec/workgroup/pipeline_executor_set.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"

namespace starrocks::spill {

Status IOTaskExecutor::submit(workgroup::ScanTask task) {
    const auto& task_ctx = task.get_work_context();
    bool use_local_io_executor = true;
    if (task_ctx.task_context_data.has_value()) {
        auto io_ctx = std::any_cast<SpillIOTaskContextPtr>(task_ctx.task_context_data);
        use_local_io_executor = io_ctx->use_local_io_executor;
    }
    auto* pool = get_executor(task.workgroup.get(), use_local_io_executor);
    if (pool->submit(std::move(task))) {
        return Status::OK();
    } else {
        return Status::InternalError("offer task failed");
    }
}

void IOTaskExecutor::force_submit(workgroup::ScanTask task) {
    const auto& task_ctx = task.get_work_context();
    auto io_ctx = std::any_cast<SpillIOTaskContextPtr>(task_ctx.task_context_data);
    auto* pool = get_executor(task.workgroup.get(), io_ctx->use_local_io_executor);
    pool->force_submit(std::move(task));
}

workgroup::ScanExecutor* IOTaskExecutor::get_executor(workgroup::WorkGroup* wg, bool use_local_io_executor) {
    return use_local_io_executor ? wg->executors()->scan_executor() : wg->executors()->connector_scan_executor();
}

} // namespace starrocks::spill
