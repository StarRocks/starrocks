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

#include "agent/master_info.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"

namespace starrocks::pipeline {
std::string to_load_error_http_path(const std::string& file_name) {
    if (file_name.empty()) {
        return "";
    }
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::be_http_port << "/api/_load_error_log?"
        << "file=" << file_name;
    return url.str();
}

std::string to_http_path(const std::string& token, const std::string& file_name) {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::be_http_port << "/api/_download_load?"
        << "token=" << token << "&file=" << file_name;
    return url.str();
}

TReportExecStatusParams ExecStateReporter::create_report_exec_status_params(QueryContext* query_ctx,
                                                                            FragmentContext* fragment_ctx,
                                                                            RuntimeProfile* profile,
                                                                            const Status& status, bool done) {
    TReportExecStatusParams params;
    auto* runtime_state = fragment_ctx->runtime_state();
    DCHECK(runtime_state != nullptr);
    DCHECK(profile != nullptr);
    auto* exec_env = fragment_ctx->runtime_state()->exec_env();
    DCHECK(exec_env != nullptr);
    params.protocol_version = FrontendServiceVersion::V1;
    params.__set_query_id(fragment_ctx->query_id());
    params.__set_backend_num(runtime_state->be_number());
    params.__set_fragment_instance_id(fragment_ctx->fragment_instance_id());
    status.set_t_status(&params);
    params.__set_done(done);

    if (runtime_state->query_options().query_type == TQueryType::LOAD && !done && status.ok()) {
        // this is a load plan, and load is not finished, just make a brief report
        runtime_state->update_report_load_status(&params);
        params.__set_load_type(runtime_state->query_options().load_job_type);

        if (query_ctx->enable_profile()) {
            profile->to_thrift(&params.profile);
            params.__isset.profile = true;
        }
    } else {
        if (runtime_state->query_options().query_type == TQueryType::LOAD) {
            runtime_state->update_report_load_status(&params);
            params.__set_load_type(runtime_state->query_options().load_job_type);
        }
        if (query_ctx->enable_profile()) {
            profile->to_thrift(&params.profile);
            params.__isset.profile = true;
        }

        if (!runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : runtime_state->output_files()) {
                params.delta_urls.push_back(to_http_path(exec_env->token(), it));
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
        if (!runtime_state->sink_commit_infos().empty()) {
            params.__isset.sink_commit_infos = true;
            params.sink_commit_infos.reserve(runtime_state->sink_commit_infos().size());
            for (auto& info : runtime_state->sink_commit_infos()) {
                params.sink_commit_infos.push_back(info);
            }
        }

        // Send new errors to coordinator
        runtime_state->get_unreported_errors(&(params.error_log));
        params.__isset.error_log = (params.error_log.size() > 0);
    }

    auto backend_id = get_backend_id();
    if (backend_id.has_value()) {
        params.__set_backend_id(backend_id.value());
    }
    return params;
}

using apache::thrift::TException;
using apache::thrift::TProcessor;
using apache::thrift::transport::TTransportException;

// including the final status when execution finishes.
Status ExecStateReporter::report_exec_status(const TReportExecStatusParams& params, ExecEnv* exec_env,
                                             const TNetworkAddress& fe_addr) {
    Status fe_status;
    FrontendServiceConnection coord(exec_env->frontend_client_cache(), fe_addr, &fe_status);
    if (!fe_status.ok()) {
        LOG(WARNING) << "Couldn't get a client for " << fe_addr;
        return fe_status;
    }

    TReportExecStatusResult res;
    Status rpc_status;

    try {
        try {
            coord->reportExecStatus(res, params);
        } catch (TTransportException& e) {
            TTransportException::TTransportExceptionType type = e.getType();
            if (type != TTransportException::TTransportExceptionType::TIMED_OUT) {
                // if not TIMED_OUT, retry
                rpc_status = coord.reopen();

                if (!rpc_status.ok()) {
                    return rpc_status;
                }
                coord->reportExecStatus(res, params);
            } else {
                std::stringstream msg;
                msg << "ReportExecStatus() to " << fe_addr << " failed:\n" << e.what();
                LOG(WARNING) << msg.str();
                rpc_status = Status::InternalError(msg.str());
                return rpc_status;
            }
        }

        rpc_status = Status(res.status);
    } catch (TException& e) {
        std::stringstream msg;
        msg << "ReportExecStatus() to " << fe_addr << " failed:\n" << e.what();
        LOG(WARNING) << msg.str();
        rpc_status = Status::InternalError(msg.str());
        return rpc_status;
    }
    return rpc_status;
}

TMVMaintenanceTasks ExecStateReporter::create_report_epoch_params(const QueryContext* query_ctx,
                                                                  const std::vector<FragmentContext*>& fragment_ctxs) {
    TMVMaintenanceTasks params;

    DCHECK(query_ctx);
    auto* stream_epoch_manager = query_ctx->stream_epoch_manager();
    DCHECK(stream_epoch_manager);

    params.__set_task_type(MVTaskType::REPORT_EPOCH);
    // maintenance_task_info
    auto task_info = stream_epoch_manager->maintenance_task_info();
    params.__set_signature(task_info.signature);
    params.__set_db_name(task_info.db_name);
    params.__set_mv_name(task_info.mv_name);
    params.__set_db_id(task_info.db_id);
    params.__set_mv_id(task_info.mv_id);
    params.__set_job_id(task_info.job_id);
    params.__set_mv_id(task_info.mv_id);
    params.__set_task_id(task_info.task_id);
    params.__set_query_id(query_ctx->query_id());

    // report_epoch
    TMVReportEpochTask report_epoch;
    auto& epoch_info = stream_epoch_manager->epoch_info();
    TMVEpoch t_epoch;
    t_epoch.__set_txn_id(epoch_info.txn_id);
    t_epoch.__set_epoch_id(epoch_info.epoch_id);
    report_epoch.__set_epoch(t_epoch);

    // commit_infos/fail_infos
    std::vector<TTabletCommitInfo> total_commit_infos;
    std::vector<TTabletFailInfo> total_failed_infos;
    for (auto& fragment_ctx : fragment_ctxs) {
        auto* runtime_state = fragment_ctx->runtime_state();
        DCHECK(runtime_state != nullptr);
        auto commit_infos = runtime_state->tablet_commit_infos();
        auto failed_infos = runtime_state->tablet_fail_infos();
        total_commit_infos.insert(total_commit_infos.end(), commit_infos.begin(), commit_infos.end());
        total_failed_infos.insert(total_failed_infos.end(), failed_infos.begin(), failed_infos.end());
    }
    report_epoch.__set_txn_commit_info(total_commit_infos);
    report_epoch.__set_txn_fail_info(total_failed_infos);

    // binlog_consume_state
    std::map<TUniqueId, std::map<TPlanNodeId, std::vector<TScanRange>>> binlog_consume_states;
    auto& fragment_id_to_node_id_scan_ranges = stream_epoch_manager->fragment_id_to_node_id_scan_ranges();
    DCHECK_EQ(fragment_id_to_node_id_scan_ranges.size(), fragment_ctxs.size());
    for (auto& [fragment_instance_id, node_id_to_scan_ranges] : fragment_id_to_node_id_scan_ranges) {
        std::map<TPlanNodeId, std::vector<TScanRange>> node_id_scan_ranges;
        for (auto& [scan_node_id, scan_ranges] : node_id_to_scan_ranges) {
            for (auto& [tablet_id, scan_range] : scan_ranges) {
                TScanRange t_scan_range;
                t_scan_range.binlog_scan_range.offset.tablet_id = scan_range.tablet_id;
                t_scan_range.binlog_scan_range.offset.version = scan_range.tablet_version;
                t_scan_range.binlog_scan_range.offset.lsn = scan_range.lsn;
                node_id_scan_ranges[scan_node_id].push_back(t_scan_range);
            }
        }
        binlog_consume_states[fragment_instance_id] = node_id_scan_ranges;
    }
    report_epoch.__set_binlog_consume_state(binlog_consume_states);
    params.__set_report_epoch(report_epoch);

    return params;
}

// including the final status when execution finishes.
Status ExecStateReporter::report_epoch(const TMVMaintenanceTasks& params, ExecEnv* exec_env,
                                       const TNetworkAddress& fe_addr) {
    Status fe_status;
    FrontendServiceConnection coord(exec_env->frontend_client_cache(), fe_addr, &fe_status);
    if (!fe_status.ok()) {
        LOG(WARNING) << "Couldn't get a client for " << fe_addr;
        return fe_status;
    }

    TMVReportEpochResponse res;
    Status rpc_status;

    try {
        try {
            coord->mvReport(res, params);
        } catch (TTransportException& e) {
            TTransportException::TTransportExceptionType type = e.getType();
            if (type != TTransportException::TTransportExceptionType::TIMED_OUT) {
                // if not TIMED_OUT, retry
                rpc_status = coord.reopen();

                if (!rpc_status.ok()) {
                    return rpc_status;
                }
                coord->mvReport(res, params);
            } else {
                std::stringstream msg;
                msg << "mvReport() to " << fe_addr << " failed:\n" << e.what();
                LOG(WARNING) << msg.str();
                rpc_status = Status::InternalError(msg.str());
            }
        }

        rpc_status = Status::OK();
    } catch (TException& e) {
        std::stringstream msg;
        msg << "mvReport() to " << fe_addr << " failed:\n" << e.what();
        LOG(WARNING) << msg.str();
        rpc_status = Status::InternalError(msg.str());
    }
    return rpc_status;
}

ExecStateReporter::ExecStateReporter() {
    auto status = ThreadPoolBuilder("ex_state_report") // exec state reporter
                          .set_min_threads(1)
                          .set_max_threads(2)
                          .set_max_queue_size(1000)
                          .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                          .build(&_thread_pool);
    if (!status.ok()) {
        LOG(FATAL) << "Cannot create thread pool for ExecStateReport: error=" << status.to_string();
    }
}

void ExecStateReporter::submit(std::function<void()>&& report_task) {
    (void)_thread_pool->submit_func(std::move(report_task));
}

} // namespace starrocks::pipeline
