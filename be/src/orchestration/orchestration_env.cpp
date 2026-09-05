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

#include "base/testutil/sync_point.h"
#include "common/config_exec_env_fwd.h"
#include "common/logging.h"
#include "common/process_exit.h"
#include "common/status.h"
#include "common/system/master_info.h"
#include "compute_env/compute_env.h"
#include "compute_env/load/stream_load_metrics.h"
#include "compute_env/profile_report_worker.h"
#include "data_workflows/load/tablet_writer/load_channel_mgr.h"
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

Status OrchestrationEnv::init(ExecEnv* exec_env, MetricRegistry* metrics, StreamLoadExecutor* stream_load_executor,
                              LoadChannelMgr* load_channel_mgr) {
    DCHECK(exec_env != nullptr);
    DCHECK(stream_load_executor != nullptr);
    _exec_env = exec_env;
    _load_channel_mgr = load_channel_mgr;
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

    _external_scan_context_mgr = std::make_unique<ExternalScanContextMgr>(exec_env, metrics);
    _external_scan_orchestrator =
            std::make_unique<ExternalScanOrchestrator>(exec_env, _external_scan_context_mgr.get());

    _stream_load_orchestrator = std::make_unique<StreamLoadOrchestrator>(exec_env, _fragment_mgr.get());

    _routine_load_task_executor =
            std::make_unique<RoutineLoadTaskExecutor>(exec_env, _stream_load_orchestrator.get(), stream_load_executor);
    RETURN_IF_ERROR(_routine_load_task_executor->init(metrics));
    _routine_load_task_executor_started = true;

    return Status::OK();
}

void OrchestrationEnv::wait_for_finish() {
    // New-request admission is handled by should_accept_new_request().
    if (config::loop_count_wait_fragments_finish <= 0) {
        if (config::loop_count_wait_fragments_finish < 0) {
            LOG(WARNING) << "'config::loop_count_wait_fragments_finish' is set to a negative integer, ignore it.";
        }
        force_reject_exec_plan_fragment();
        return;
    }

    size_t max_loop_secs = config::loop_count_wait_fragments_finish * 10;
    const int64_t drain_budget_ms = static_cast<int64_t>(max_loop_secs) * 1000;
    if (config::graceful_exit_reject_delay_ms >= drain_budget_ms ||
        config::graceful_exit_reject_fallback_ms >= drain_budget_ms) {
        LOG(WARNING) << "Graceful exit admission cutoff is not before the drain budget: delay_ms="
                     << config::graceful_exit_reject_delay_ms
                     << ", fallback_ms=" << config::graceful_exit_reject_fallback_ms
                     << ", drain_budget_ms=" << drain_budget_ms;
    }

    size_t loop_secs = 0;
    size_t running_fragments = 0;
    // Separate reads may miss an RPC admitted while count is zero.
    // Force-reject, then re-sample; seq_cst orders guards with the re-sample.
    //
    // Linearization proof (RPC admission in internal_service.cpp): the RPC's
    // inc_rpc_prep_inflight() (seq_cst RMW), its should_accept_new_request() force read
    // (seq_cst), force_reject_exec_plan_fragment()'s store(true) (seq_cst), and this
    // re-sample load (seq_cst) all share one seq_cst total order. Suppose an RPC read
    // force==false and was admitted, its paired dec has NOT yet run, and this loop already
    // broke. Its force read happened-before the force store, its inc happened-before that
    // read (inc precedes the check in the caller), and the breaking re-sample
    // happened-after the force store. Chaining: inc < force-read < force-store < re-sample,
    // so the re-sample must observe the still-outstanding inc (count >= 1), contradicting
    // count==0. Hence a zero-count break is only reached when every admitted request has
    // already run its paired dec (its work completed). An admitted-but-unfinished request
    // (dec not yet run) keeps this loop draining. The one path that may exit with admitted
    // work still in flight is the hard budget expiry below, not this zero-count break.
    while (loop_secs < max_loop_secs) {
        running_fragments = _get_running_fragments_count();
        if (running_fragments == 0 && (!process_exit_in_progress() || !should_accept_new_request())) {
            if (process_exit_in_progress()) {
                force_reject_exec_plan_fragment();
                running_fragments = _get_running_fragments_count();
                if (running_fragments != 0) {
                    LOG(INFO) << "Fragment admitted while closing admissions; " << running_fragments
                              << " fragment(s) still running, keep draining...";
                    sleep(1);
                    loop_secs++;
                    continue;
                }
            }
            break;
        }
        LOG(INFO) << "Frontend is aware of exit: " << is_frontend_aware_of_exit()
                  << ", reject new fragment: " << !should_accept_new_request() << ", " << running_fragments
                  << " fragment(s) are still running...";
        sleep(1);
        loop_secs++;
    }

    // Force rejection at budget expiry; report remaining admitted work.
    if (process_exit_in_progress()) {
        force_reject_exec_plan_fragment();
        running_fragments = _get_running_fragments_count();
        if (running_fragments != 0) {
            LOG(WARNING) << "Drain wait budget exhausted; " << running_fragments
                         << " admitted fragment(s) still running, proceed with shutdown.";
        }
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
    // Sample in predecessor -> successor order using independent statements (the C++ `+`
    // operand evaluation order is unspecified, so it cannot express the sampling order).
    // Entry counters (admission / RPC prepare) are read first; their release happens only
    // after the successor state (query/fragment registry, load-execution context) has been
    // published, so a zero entry count followed by the successor read observes either the
    // published state or its real completion. Read order per plan §8.1 (2026-09-05).
    const auto request_admissions = request_admissions_inflight();
    const auto rpc_prep = _rpc_prep_inflight.load(std::memory_order_seq_cst);
    const auto short_circuit = _short_circuit_inflight.load(std::memory_order_seq_cst);
    const auto stream_load_orchestrator_inflight =
            (_stream_load_orchestrator == nullptr ? 0 : _stream_load_orchestrator->load_inflight());
    const auto stream_loads = StreamLoadMetrics::instance()->streaming_load_current_processing.value();
    const auto transaction_stream_loads =
            StreamLoadMetrics::instance()->transaction_streaming_load_current_processing.value();
    const auto load_channel_work = (_load_channel_mgr == nullptr ? 0 : _load_channel_mgr->pending_work_count());
    const auto non_pipeline_fragments = _fragment_mgr == nullptr ? 0 : _fragment_mgr->running_fragment_count();
    // Test hook: lets a regression test publish a successor state (register an active query)
    // after the predecessor counts are read but before the successor registry is read, to
    // verify the handoff is not collapsed to a false zero (plan §8.1, 2026-09-05).
    TEST_SYNC_POINT("OrchestrationEnv::_get_running_fragments_count:before_query_read");
    const auto pipeline_fragments = (_exec_env == nullptr || _exec_env->query_context_mgr() == nullptr)
                                            ? 0
                                            : _exec_env->query_context_mgr()->size();
    return request_admissions + rpc_prep + short_circuit + stream_load_orchestrator_inflight + stream_loads +
           transaction_stream_loads + load_channel_work + non_pipeline_fragments + pipeline_fragments;
}

} // namespace starrocks::orchestration
