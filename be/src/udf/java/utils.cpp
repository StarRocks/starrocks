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

#include "udf/java/utils.h"

#include <bthread/bthread.h>

#include <memory>

#include "compute_env/workgroup/scan_executor.h"
#include "compute_env/workgroup/scan_task.h"
#include "compute_env/workgroup/work_group.h"
#include "compute_env/workgroup/work_group_manager.h"
#include "exec/exec_env.h"

namespace starrocks {
PromiseStatusPtr call_hdfs_scan_function_in_pthread(const std::function<Status()>& func) {
    PromiseStatusPtr ms = std::make_unique<PromiseStatus>();
    if (bthread_self()) {
        ExecEnv::GetInstance()->connector_scan_executor()->submit(workgroup::ScanTask(
                ExecEnv::GetInstance()->workgroup_manager()->get_default_workgroup(),
                [promise = ms.get(), func](workgroup::YieldContext&) { promise->set_value(func()); }));
    } else {
        ms->set_value(func());
    }
    return ms;
}

} // namespace starrocks
