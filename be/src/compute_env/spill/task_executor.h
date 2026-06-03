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

#include <memory>
#include <utility>

#include "common/status.h"
#include "compute_env/workgroup/scan_task.h"
#include "compute_env/workgroup/work_group_fwd.h"

namespace starrocks::spill {

struct SpillIOTaskContext {
    bool use_local_io_executor = true;
};
using SpillIOTaskContextPtr = std::shared_ptr<SpillIOTaskContext>;

struct IOTaskExecutor {
    static Status submit(workgroup::ScanTask task);
    static void force_submit(workgroup::ScanTask task);

private:
    static workgroup::ScanExecutor* get_executor(workgroup::WorkGroup* wg, bool use_local_io_executor);
};

struct SyncTaskExecutor {
    static Status submit(workgroup::ScanTask task) {
        do {
            task.run();
        } while (!task.is_finished());
        return Status::OK();
    }

    static void force_submit(workgroup::ScanTask task) { (void)submit(std::move(task)); }
};

} // namespace starrocks::spill
