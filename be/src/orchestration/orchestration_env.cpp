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

#include <unistd.h>

#include <memory>
#include <vector>

#include "common/config_exec_env_fwd.h"
#include "common/logging.h"
#include "common/process_exit.h"
#include "common/system/master_info.h"
#include "compute_env/compute_env.h"
#include "compute_env/profile_report_worker.h"
#include "exec/exec_env.h"
#include "exec/pipeline/pipeline_fragment_reporter.h"
#include "exec/runtime/query_context_manager.h"
#include "orchestration/external_scan_context_mgr.h"
#include "orchestration/external_scan_orchestrator.h"
#include "orchestration/fragment_mgr.h"
#include "orchestration/orchestration_metrics.h"
#include "orchestration/routine_load_task_executor.h"
#include "orchestration/runtime_filter_worker.h"
#include "orchestration/stream_load_orchestrator.h"

namespace starrocks::orchestration {

OrchestrationEnv::OrchestrationEnv() = default;

OrchestrationEnv::~OrchestrationEnv() {
    destroy();
}

Status OrchestrationEnv::init(ExecEnv* exec_env, MetricRegistry* metrics) {
    DCHECK(exec_env != nullptr);
    _exec_env = exec_env;

    _fragment_mgr = std::make_unique<FragmentMgr>(exec_env, metrics);

    ProfileReportWorkerOptions profile_report_worker_options;
    profile_report_worker_options.report_non_pipeline_fragments =
            [this](const std::vector<TUniqueId>& non_pipeline_need_report_fragment_ids) {
                DCHECK(_fragment_mgr != nullptr);
                return _fragment_mgr->report_fragments(non_pipeline_need_report_fragment_ids);
            };
    profile_report_worker_options.report_pipeline_fragments =
            [exec_env](const std::vector<PipeLineReportTaskKey>& pipeline_need_report_query_fragment_ids) {
                DCHECK(exec_env->query_context_mgr() != nullptr);
                return report_pipeline_fragments(exec_env->query_context_mgr(),
                                                 pipeline_need_report_query_fragment_ids);
            };
    RETURN_IF_ERROR(exec_env->compute_env()->init_profile_report_worker(std::move(profile_report_worker_options)));

    _runtime_filter_worker =
            std::make_unique<RuntimeFilterWorker>(&exec_env->runtime_services(), &exec_env->rpc_services(),
                                                  exec_env->query_pool_mem_tracker(), _fragment_mgr.get());
    _runtime_filter_worker_started = true;
    exec_env->set_runtime_filter_services(_runtime_filter_worker.get(), _runtime_filter_worker.get());

    _metrics = std::make_unique<OrchestrationMetrics>();
    _metrics->install(
            metrics, [this] { return _runtime_filter_worker == nullptr ? nullptr : _runtime_filter_worker->metrics(); },
            [this] { return _runtime_filter_worker == nullptr ? 0 : _runtime_filter_worker->queue_size(); });

    _external_scan_context_mgr = std::make_unique<ExternalScanContextMgr>(exec_env, metrics, _fragment_mgr.get());
    _external_scan_orchestrator =
            std::make_unique<ExternalScanOrchestrator>(exec_env, _external_scan_context_mgr.get());

    _stream_load_orchestrator = std::make_unique<StreamLoadOrchestrator>(exec_env, _fragment_mgr.get());

    _routine_load_task_executor = std::make_unique<RoutineLoadTaskExecutor>(exec_env, _stream_load_orchestrator.get());
    RETURN_IF_ERROR(_routine_load_task_executor->init(metrics));
    _routine_load_task_executor_started = true;

    return Status::OK();
}

void OrchestrationEnv::wait_for_finish() {
    if (config::loop_count_wait_fragments_finish < 0) {
        LOG(WARNING) << "'config::loop_count_wait_fragments_finish' is set to a negative integer, ignore it.";
        return;
    }

    size_t max_loop_secs = config::loop_count_wait_fragments_finish * 10;
    if (max_loop_secs == 0) {
        return;
    }

    size_t running_fragments = _get_running_fragments_count();
    size_t loop_secs = 0;

    // TODO: decouple the heartbeat with the graceful exit
    // only wait for frontend's heartbeat when the node is ever received heartbeats from the frontend
    bool need_wait_frontend_hb = config::graceful_exit_wait_for_frontend_heartbeat && get_backend_id().has_value();

    while ((running_fragments > 0 || (need_wait_frontend_hb && !is_frontend_aware_of_exit())) &&
           loop_secs < max_loop_secs) {
        LOG(INFO) << "Frontend is aware of exit: " << is_frontend_aware_of_exit() << ", " << running_fragments
                  << " fragment(s) are still running...";
        sleep(1);
        running_fragments = _get_running_fragments_count();
        loop_secs++;
    }
}

void OrchestrationEnv::stop() {
    if (_exec_env != nullptr && _exec_env->compute_env() != nullptr && _exec_env->profile_report_worker() != nullptr) {
        _exec_env->compute_env()->stop_profile_report_worker();
    }
    if (_runtime_filter_worker != nullptr && _runtime_filter_worker_started) {
        _runtime_filter_worker->close();
        _runtime_filter_worker_started = false;
    }
    if (_routine_load_task_executor != nullptr && _routine_load_task_executor_started) {
        _routine_load_task_executor->stop();
        _routine_load_task_executor_started = false;
    }
    if (_fragment_mgr != nullptr) {
        _fragment_mgr->close();
    }
}

void OrchestrationEnv::destroy() {
    stop();
    _metrics.reset();
    _runtime_filter_worker.reset();
    _routine_load_task_executor.reset();
    _stream_load_orchestrator.reset();
    _external_scan_orchestrator.reset();
    _external_scan_context_mgr.reset();
    if (_exec_env != nullptr && _exec_env->compute_env() != nullptr) {
        _exec_env->compute_env()->destroy_profile_report_worker();
        _exec_env->set_runtime_filter_services(nullptr, nullptr);
        _exec_env = nullptr;
    }
    _fragment_mgr.reset();
}

size_t OrchestrationEnv::_get_running_fragments_count() const {
    const auto non_pipeline_fragments = _fragment_mgr == nullptr ? 0 : _fragment_mgr->running_fragment_count();
    const auto pipeline_fragments = (_exec_env == nullptr || _exec_env->query_context_mgr() == nullptr)
                                            ? 0
                                            : _exec_env->query_context_mgr()->size();
    return non_pipeline_fragments + pipeline_fragments;
}

} // namespace starrocks::orchestration
