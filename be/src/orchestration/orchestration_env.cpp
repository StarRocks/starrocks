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

#include "orchestration/orchestration_env.h"

#include <memory>

#include "common/logging.h"
#include "orchestration/external_scan_context_mgr.h"
#include "orchestration/external_scan_orchestrator.h"
#include "orchestration/orchestration_metrics.h"
#include "orchestration/routine_load_task_executor.h"
#include "orchestration/stream_load_orchestrator.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_filter_worker.h"

namespace starrocks::orchestration {

OrchestrationEnv::OrchestrationEnv() = default;

OrchestrationEnv::~OrchestrationEnv() {
    destroy();
}

Status OrchestrationEnv::init(ExecEnv* exec_env, MetricRegistry* metrics) {
    DCHECK(exec_env != nullptr);

    _metrics = std::make_unique<OrchestrationMetrics>();
    _metrics->install(metrics, [exec_env] {
        auto* runtime_filter_worker = exec_env->runtime_filter_worker();
        return runtime_filter_worker == nullptr ? nullptr : runtime_filter_worker->metrics();
    });

    _external_scan_context_mgr = std::make_unique<ExternalScanContextMgr>(exec_env, metrics);
    _external_scan_orchestrator =
            std::make_unique<ExternalScanOrchestrator>(exec_env, _external_scan_context_mgr.get());

    _stream_load_orchestrator = std::make_unique<StreamLoadOrchestrator>(exec_env);

    _routine_load_task_executor = std::make_unique<RoutineLoadTaskExecutor>(exec_env, _stream_load_orchestrator.get());
    RETURN_IF_ERROR(_routine_load_task_executor->init(metrics));
    _routine_load_task_executor_started = true;

    return Status::OK();
}

void OrchestrationEnv::stop() {
    if (_routine_load_task_executor != nullptr && _routine_load_task_executor_started) {
        _routine_load_task_executor->stop();
        _routine_load_task_executor_started = false;
    }
}

void OrchestrationEnv::destroy() {
    stop();
    _metrics.reset();
    _routine_load_task_executor.reset();
    _stream_load_orchestrator.reset();
    _external_scan_orchestrator.reset();
    _external_scan_context_mgr.reset();
}

} // namespace starrocks::orchestration
