// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "exec/pipeline/exec_state_reporter.h"

#include <thrift/Thrift.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"

namespace starrocks::pipeline {
std::string to_load_error_http_path(const std::string& file_name) {
    if (file_name.empty()) {
        return "";
    }
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port << "/api/_load_error_log?"
        << "file=" << file_name;
    return url.str();
}

std::string to_http_path(const std::string& token, const std::string& file_name) {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port << "/api/_download_load?"
        << "token=" << token << "&file=" << file_name;
    return url.str();
}

TReportExecStatusParams ExecStateReporter::create_report_exec_status_params(QueryContext* query_ctx,
                                                                            FragmentContext* fragment_ctx,
                                                                            const Status& status, bool done) {
    TReportExecStatusParams params;
    auto* runtime_state = fragment_ctx->runtime_state();
    DCHECK(runtime_state != nullptr);
    auto* profile = runtime_state->runtime_profile();
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
        params.__set_loaded_rows(runtime_state->num_rows_load_total());
    } else {
        if (runtime_state->query_options().query_type == TQueryType::LOAD) {
            params.__set_loaded_rows(runtime_state->num_rows_load_total());
        }
        if (query_ctx->is_report_profile()) {
            profile->to_thrift(&params.profile);
            params.__isset.profile = true;
        }

        if (!runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : runtime_state->output_files()) {
                params.delta_urls.push_back(to_http_path(exec_env->token(), it));
            }
        }
        if (runtime_state->num_rows_load_total() > 0 || runtime_state->num_rows_load_filtered() > 0) {
            params.__isset.load_counters = true;
            // TODO(zc)
            static std::string s_dpp_normal_all = "dpp.norm.ALL";
            static std::string s_dpp_abnormal_all = "dpp.abnorm.ALL";
            static std::string s_unselected_rows = "unselected.rows";
            static std::string s_loaded_bytes = "loaded.bytes";

            params.load_counters.emplace(s_dpp_normal_all, std::to_string(runtime_state->num_rows_load_success()));
            params.load_counters.emplace(s_dpp_abnormal_all, std::to_string(runtime_state->num_rows_load_filtered()));
            params.load_counters.emplace(s_unselected_rows, std::to_string(runtime_state->num_rows_load_unselected()));
            params.load_counters.emplace(s_loaded_bytes, std::to_string(runtime_state->num_bytes_load_total()));
        }
        if (!runtime_state->get_error_log_file_path().empty()) {
            params.__set_tracking_url(to_load_error_http_path(runtime_state->get_error_log_file_path()));
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

        // Send new errors to coordinator
        runtime_state->get_unreported_errors(&(params.error_log));
        params.__isset.error_log = (params.error_log.size() > 0);
    }

    if (exec_env->master_info()->__isset.backend_id) {
        params.__set_backend_id(exec_env->master_info()->backend_id);
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

    VLOG_ROW << "debug: reportExecStatus params is " << apache::thrift::ThriftDebugString(params).c_str();
    try {
        try {
            coord->reportExecStatus(res, params);
        } catch (TTransportException& e) {
            LOG(WARNING) << "Retrying ReportExecStatus: " << e.what();
            rpc_status = coord.reopen();

            if (!rpc_status.ok()) {
                return rpc_status;
            }
            coord->reportExecStatus(res, params);
        }

        rpc_status = Status(res.status);
    } catch (TException& e) {
        std::stringstream msg;
        msg << "ReportExecStatus() to " << fe_addr << " failed:\n" << e.what();
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
    _thread_pool->submit_func(std::move(report_task));
}

} // namespace starrocks::pipeline
