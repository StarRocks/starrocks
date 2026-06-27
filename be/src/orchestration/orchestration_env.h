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

#include "common/status.h"

namespace starrocks {

class ExecEnv;
class MetricRegistry;

namespace orchestration {

class ExternalScanContextMgr;
class ExternalScanOrchestrator;
class OrchestrationMetrics;
class RoutineLoadTaskExecutor;
class RuntimeFilterWorker;
class StreamLoadOrchestrator;

class OrchestrationEnv {
public:
    OrchestrationEnv();
    ~OrchestrationEnv();

    Status init(ExecEnv* exec_env, MetricRegistry* metrics);
    void stop();
    void destroy();

    RoutineLoadTaskExecutor* routine_load_task_executor() { return _routine_load_task_executor.get(); }
    const RoutineLoadTaskExecutor* routine_load_task_executor() const { return _routine_load_task_executor.get(); }
    StreamLoadOrchestrator* stream_load_orchestrator() { return _stream_load_orchestrator.get(); }
    const StreamLoadOrchestrator* stream_load_orchestrator() const { return _stream_load_orchestrator.get(); }
    ExternalScanOrchestrator* external_scan_orchestrator() { return _external_scan_orchestrator.get(); }
    const ExternalScanOrchestrator* external_scan_orchestrator() const { return _external_scan_orchestrator.get(); }
    RuntimeFilterWorker* runtime_filter_worker() { return _runtime_filter_worker.get(); }
    const RuntimeFilterWorker* runtime_filter_worker() const { return _runtime_filter_worker.get(); }

private:
    ExecEnv* _exec_env = nullptr;
    std::unique_ptr<OrchestrationMetrics> _metrics;
    std::unique_ptr<RuntimeFilterWorker> _runtime_filter_worker;
    std::unique_ptr<ExternalScanContextMgr> _external_scan_context_mgr;
    std::unique_ptr<ExternalScanOrchestrator> _external_scan_orchestrator;
    std::unique_ptr<StreamLoadOrchestrator> _stream_load_orchestrator;
    std::unique_ptr<RoutineLoadTaskExecutor> _routine_load_task_executor;
    bool _runtime_filter_worker_started = false;
    bool _routine_load_task_executor_started = false;
};

} // namespace orchestration
} // namespace starrocks
