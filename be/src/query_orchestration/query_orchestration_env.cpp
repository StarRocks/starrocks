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

#include "query_orchestration/query_orchestration_env.h"

#include <memory>

#include "common/logging.h"
#include "query_orchestration/routine_load_task_executor.h"

namespace starrocks::query_orchestration {

QueryOrchestrationEnv::QueryOrchestrationEnv() = default;

QueryOrchestrationEnv::~QueryOrchestrationEnv() {
    destroy();
}

Status QueryOrchestrationEnv::init(ExecEnv* exec_env, MetricRegistry* metrics) {
    DCHECK(exec_env != nullptr);

    _routine_load_task_executor = std::make_unique<RoutineLoadTaskExecutor>(exec_env);
    RETURN_IF_ERROR(_routine_load_task_executor->init(metrics));
    _routine_load_task_executor_started = true;

    return Status::OK();
}

void QueryOrchestrationEnv::stop() {
    if (_routine_load_task_executor != nullptr && _routine_load_task_executor_started) {
        _routine_load_task_executor->stop();
        _routine_load_task_executor_started = false;
    }
}

void QueryOrchestrationEnv::destroy() {
    stop();
    _routine_load_task_executor.reset();
}

} // namespace starrocks::query_orchestration
