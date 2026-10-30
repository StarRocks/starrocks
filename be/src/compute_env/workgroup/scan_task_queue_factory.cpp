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

#include "compute_env/workgroup/scan_task_queue_factory.h"

#include "common/config_exec_flow_fwd.h"
#include "compute_env/workgroup/priority_scan_task_queue.h"

namespace starrocks::workgroup {

std::unique_ptr<ScanTaskQueue> create_scan_task_queue() {
    return std::make_unique<PriorityScanTaskQueue>(config::pipeline_scan_thread_pool_queue_size);
}

} // namespace starrocks::workgroup
