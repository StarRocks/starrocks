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

#include "exec/pipeline/exec_state_reporter.h"

#include <thrift/Thrift.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <memory>

#include "base/network/network_util.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_network_fwd.h"
#include "common/config_rpc_client_fwd.h"
#include "common/system/backend_options.h"
#include "common/system/master_info.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_metrics.h"
#include "exec/pipeline/query_context.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/client_cache.h"
#include "runtime/runtime_state_helper.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks::pipeline {
std::string to_load_error_http_path(const std::string& file_name) {
    if (file_name.empty()) {
        return "";
    }

    std::string resolved_ip = BackendOptions::get_resolved_ip();

    std::stringstream url;
    url << "http://" << get_host_port(resolved_ip, config::be_http_port) << "/api/_load_error_log?"
        << "file=" << file_name;
    return url.str();
}

std::string to_http_path(const std::string& token, const std::string& file_name) {
    std::stringstream url;
    url << "http://" << get_host_port(BackendOptions::get_localhost(), config::be_http_port) << "/api/_download_load?"
        << "token=" << token << "&file=" << file_name;
    return url.str();
}

std::unique_ptr<TReportExecStatusParams> ExecStateReporter::create_report_exec_status_params(
        QueryContext* query_ctx, FragmentContext* fragment_ctx, RuntimeProfile* profile,
        RuntimeProfile* load_channel_profile, const Status& status, bool done) {
    auto res = std::make_unique<TReportExecStatusParams>();
    TReportExecStatusParams& params = *res;
    auto* runtime_state = fragment_ctx->runtime_state();
    DCHECK(runtime_state != nullptr);
    DCHECK(profile != nullptr);
    params.protocol_version = FrontendServiceVersion::V1;
    params.__set_query_id(fragment_ctx->query_id());
    params.__set_backend_num(runtime_state->be_number());
    params.__set_fragment_instance_id(fragment_ctx->fragment_instance_id());
    status.set_t_status(&params);
    params.__set_done(done);

    if (runtime_state->query_options().query_type == TQueryType::LOAD && !done && status.ok()) {
        // this is a load plan, and load is not finished, just make a brief report
        RuntimeStateHelper::update_report_load_status(runtime_state, &params);
        params.__set_load_type(runtime_state->query_options().load_job_type);

        if (query_ctx->enable_profile()) {
            profile->to_thrift(&params.profile);
            params.__isset.profile = true;

            load_channel_profile->to_thrift(&params.load_channel_profile);
            params.__isset.load_channel_profile = true;
        }
    } else {
        if (runtime_state->query_options().query_type == TQueryType::LOAD) {
            RuntimeStateHelper::update_report_load_status(runtime_state, &params);
            params.__set_load_type(runtime_state->query_options().load_job_type);
        }
        if (query_ctx->enable_profile()) {
            profile->to_thrift(&params.profile);
            params.__isset.profile = true;

            if (runtime_state->query_options().query_type == TQueryType::LOAD) {
                load_channel_profile->to_thrift(&params.load_channel_profile);
                params.__isset.load_channel_profile = true;
            }
        }

        if (!runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            const auto master_token = get_master_token();
            for (auto& it : runtime_state->output_files()) {
                params.delta_urls.push_back(to_http_path(master_token, it));
            }
        }
        if (runtime_state->num_rows_load_sink() > 0 || runtime_state->num_rows_load_filtered() > 0 ||
            runtime_state->num_rows_load_unselected() > 0) {
            params.__isset.load_counters = true;
            static std::string s_dpp_normal_all = "dpp.norm.ALL";
            static std::string s_dpp_abnormal_all = "dpp.abnorm.ALL";
            static std::string s_unselected_rows = "unselected.rows";
            static std::string s_loaded_bytes = "loaded.bytes";

            params.load_counters.emplace(s_dpp_normal_all, std::to_string(runtime_state->num_rows_load_sink()));
            params.load_counters.emplace(s_dpp_abnormal_all, std::to_string(runtime_state->num_rows_load_filtered()));
            params.load_counters.emplace(s_unselected_rows, std::to_string(runtime_state->num_rows_load_unselected()));
            params.load_counters.emplace(s_loaded_bytes, std::to_string(runtime_state->num_bytes_load_sink()));
        }
        if (!runtime_state->get_error_log_file_path().empty()) {
            params.__set_tracking_url(to_load_error_http_path(runtime_state->get_error_log_file_path()));
        }
        if (!runtime_state->get_rejected_record_file_path().empty()) {
            params.__set_rejected_record_path(runtime_state->get_rejected_record_file_path());
        }
        if (!runtime_state->export_output_files().empty()) {
            params.__isset.export_files = true;
            params.export_files = runtime_state->export_output_files();
        }
        if (!runtime_state->tablet_commit_infos().empty()) {
            params.__isset.commitInfos = true;
            params.commitInfos.reserve(runtime_state->tablet_commit_infos().size());
            for (auto& info : runtime_state->tablet_commit_infos()) {
                params.commitInfos.push_back(info);
            }
        }
        if (!runtime_state->tablet_fail_infos().empty()) {
            params.__isset.failInfos = true;
            params.failInfos.reserve(runtime_state->tablet_fail_infos().size());
            for (auto& info : runtime_state->tablet_fail_infos()) {
                params.failInfos.push_back(info);
            }
        }
        if (!runtime_state->sink_commit_infos().empty()) {
            params.__isset.sink_commit_infos = true;
            params.sink_commit_infos.reserve(runtime_state->sink_commit_infos().size());
            for (auto& info : runtime_state->sink_commit_infos()) {
                params.sink_commit_infos.push_back(info);
            }
        }
    }

    auto backend_id = get_backend_id();
    if (backend_id.has_value()) {
        params.__set_backend_id(backend_id.value());
    }
    return res;
}

// including the final status when execution finishes.
Status ExecStateReporter::report_exec_status(const TReportExecStatusParams& params, const TNetworkAddress& fe_addr) {
    TReportExecStatusResult res;
    Status rpc_status;

    // since the caller(report_exec_state) has already retried {@code config::report_exec_rpc_request_retry_num} times,
    // no need to retry again.
    rpc_status = ThriftRpcHelper::rpc<FrontendServiceClient>(
            fe_addr, [&res, &params](FrontendServiceConnection& client) { client->reportExecStatus(res, params); },
            config::thrift_rpc_timeout_ms, 1);

    if (rpc_status.ok()) {
        rpc_status = Status(res.status);
    }

    return rpc_status;
}

ExecStateReporter::ExecStateReporter(const CpuUtil::CpuIds& cpuids, ExecStateReporterMetrics* metrics) {
    int exec_state_report_threads = std::max(1, config::exec_state_report_max_threads);
    auto status = ThreadPoolBuilder("exec_state_report") // exec state reporter
                          .set_min_threads(1)
                          .set_max_threads(exec_state_report_threads)
                          .set_max_queue_size(1000)
                          .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                          .set_cpuids(cpuids)
                          .build(&_thread_pool);
    if (!status.ok()) {
        LOG(FATAL) << "Cannot create thread pool for ExecStateReport: error=" << status.to_string();
    }

    int priority_exec_state_report_threads = std::max(1, config::priority_exec_state_report_max_threads);
    status = ThreadPoolBuilder("priority_exec_state_report") // priority exec state reporter with infinite queue
                     .set_min_threads(1)
                     .set_max_threads(priority_exec_state_report_threads)
                     .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                     .set_cpuids(cpuids)
                     .build(&_priority_thread_pool);
    if (!status.ok()) {
        LOG(FATAL) << "Cannot create thread pool for priority ExecStateReport: error=" << status.to_string();
    }

    _metrics = metrics;
    _metrics->monitor_reporter(_thread_pool.get());
    _metrics->monitor_priority_reporter(_priority_thread_pool.get());
}

ExecStateReporter::~ExecStateReporter() {
    // remove thread pool metrics from parent metrics
    _metrics->unmonitor_reporter(_thread_pool.get());
    _metrics->unmonitor_priority_reporter(_priority_thread_pool.get());
}

void ExecStateReporter::submit(std::function<void()>&& report_task, bool priority) {
    if (priority) {
        (void)_priority_thread_pool->submit_func(std::move(report_task));
    } else {
        (void)_thread_pool->submit_func(std::move(report_task));
    }
}

void ExecStateReporter::bind_cpus(const CpuUtil::CpuIds& cpuids) const {
    _thread_pool->bind_cpus(cpuids, {});
    _priority_thread_pool->bind_cpus(cpuids, {});
}

Status ExecStateReporter::update_max_threads(int max_threads) {
    return _thread_pool->update_max_threads(std::max(1, max_threads));
}

Status ExecStateReporter::update_priority_max_threads(int max_threads) {
    return _priority_thread_pool->update_max_threads(std::max(1, max_threads));
}

} // namespace starrocks::pipeline
