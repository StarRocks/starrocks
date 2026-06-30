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
#include <sstream>
#include <utility>
#include <vector>

#include "base/failpoint/fail_point.h"
#include "base/network/network_util.h"
#include "base/utility/defer_op.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_network_fwd.h"
#include "common/config_rpc_client_fwd.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "common/system/backend_options.h"
#include "common/system/master_info.h"
#include "common/util/thrift_client_cache.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/primitives/pipeline_metrics.h"
#include "exec/pipeline/query_context.h"
#include "exec/runtime_compat/runtime_state_helper.h"
#include "gen_cpp/FrontendService.h"
#include "platform/thrift_rpc_helper.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_env.h"

namespace starrocks::pipeline {

DEFINE_FAIL_POINT(report_exec_state_failed_status);

namespace {

RuntimeProfile* build_merged_instance_profile(QueryRuntimeState* query_runtime_state, FragmentContext* fragment_ctx,
                                              ObjectPool* obj_pool) {
    auto* instance_profile = fragment_ctx->runtime_state()->runtime_profile();

    if (query_runtime_state->profile_level() >= TPipelineProfileLevel::type::DETAIL) {
        return instance_profile;
    }

    RuntimeProfile* new_instance_profile = nullptr;
    int64_t process_raw_timer = 0;
    DeferOp defer([&new_instance_profile, &process_raw_timer]() {
        if (new_instance_profile != nullptr) {
            auto* process_timer = ADD_TIMER(new_instance_profile, "BackendProfileMergeTime");
            COUNTER_SET(process_timer, process_raw_timer);
        }
    });

    SCOPED_RAW_TIMER(&process_raw_timer);
    std::vector<RuntimeProfile*> pipeline_profiles;
    instance_profile->get_children(&pipeline_profiles);

    std::vector<RuntimeProfile*> merged_driver_profiles;
    for (auto* pipeline_profile : pipeline_profiles) {
        std::vector<RuntimeProfile*> driver_profiles;
        pipeline_profile->get_children(&driver_profiles);

        if (driver_profiles.empty()) {
            continue;
        }

        auto* merged_driver_profile = RuntimeProfile::merge_isomorphic_profiles(obj_pool, driver_profiles);

        // use the name of pipeline' profile as pipeline driver's
        merged_driver_profile->set_name(pipeline_profile->name());

        // add all the info string and counters of the pipeline's profile
        // to the pipeline driver's profile
        merged_driver_profile->copy_all_info_strings_from(pipeline_profile);
        merged_driver_profile->copy_all_counters_from(pipeline_profile);

        merged_driver_profiles.push_back(merged_driver_profile);
    }

    new_instance_profile = obj_pool->add(new RuntimeProfile(instance_profile->name()));
    new_instance_profile->copy_all_info_strings_from(instance_profile);
    new_instance_profile->copy_all_counters_from(instance_profile);
    for (auto* merged_driver_profile : merged_driver_profiles) {
        merged_driver_profile->reset_parent();
        new_instance_profile->add_child(merged_driver_profile, true, nullptr);
    }

    return new_instance_profile;
}

} // namespace

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
    auto* query_runtime_state = runtime_state->query_runtime_state();
    DCHECK(query_runtime_state != nullptr);
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

        if (query_runtime_state->enable_profile()) {
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
        if (query_runtime_state->enable_profile()) {
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
        // Legacy rejected-record file removed; see
        // LoadPathStateHelper::append_rejected_record_to_file for the
        // writer-based replacement. The `rejected_record_path` Thrift
        // field is kept for wire-compat but is no longer populated.
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

void ExecStateReporter::report_exec_state(QueryContext* query_ctx, FragmentContext* fragment_ctx, const Status& status,
                                          bool done) {
    auto* profile = fragment_ctx->runtime_state()->runtime_profile();
    auto* query_runtime_state = fragment_ctx->runtime_state()->query_runtime_state();
    DCHECK(query_runtime_state != nullptr);
    ObjectPool obj_pool;
    if (query_runtime_state->enable_profile()) {
        profile = build_merged_instance_profile(query_runtime_state, fragment_ctx, &obj_pool);

        // Add counters for query level memory and cpu usage, these two metrics will be specially handled at the frontend
        auto* query_peak_memory = profile->add_counter(
                "QueryPeakMemoryUsage", TUnit::BYTES,
                RuntimeProfile::Counter::create_strategy(TUnit::BYTES, TCounterMergeType::SKIP_FIRST_MERGE));
        COUNTER_SET(query_peak_memory, query_ctx->mem_cost_bytes());
        auto* query_cumulative_cpu = profile->add_counter(
                "QueryCumulativeCpuTime", TUnit::TIME_NS,
                RuntimeProfile::Counter::create_strategy(TUnit::TIME_NS, TCounterMergeType::SKIP_FIRST_MERGE));
        COUNTER_SET(query_cumulative_cpu, query_runtime_state->cpu_cost());

        auto* query_spill_bytes = profile->add_counter(
                "QuerySpillBytes", TUnit::BYTES,
                RuntimeProfile::Counter::create_strategy(TUnit::BYTES, TCounterMergeType::SKIP_FIRST_MERGE));
        COUNTER_SET(query_spill_bytes, query_ctx->get_spill_bytes());

        // Add execution wall time
        auto* query_exec_wall_time = profile->add_counter(
                "QueryExecutionWallTime", TUnit::TIME_NS,
                RuntimeProfile::Counter::create_strategy(TUnit::TIME_NS, TCounterMergeType::SKIP_FIRST_MERGE));
        COUNTER_SET(query_exec_wall_time, query_ctx->lifetime());
    }

    const auto& fe_addr = fragment_ctx->fe_addr();
    if (fe_addr.hostname.empty()) {
        // query executed by external connectors, like spark and flink connector,
        // does not need to report exec state to FE, so return if fe addr is empty.
        return;
    }

    // Load channel profile will be merged on FE
    auto* load_channel_profile = fragment_ctx->runtime_state()->load_channel_profile();
    std::shared_ptr<TReportExecStatusParams> params;
    {
        // move profile memory to process, similar with SinkBuffer. the params will be released in ExecStateReporter
        int64_t before_bytes = CurrentThread::current().get_consumed_bytes();
        params = ExecStateReporter::create_report_exec_status_params(query_ctx, fragment_ctx, profile,
                                                                     load_channel_profile, status, done);
        int64_t delta = CurrentThread::current().get_consumed_bytes() - before_bytes;

        CurrentThread::current().mem_release(delta);
        RuntimeEnv::GetInstance()->process_mem_tracker()->consume(delta);
    }

    auto instance_id = fragment_ctx->fragment_instance_id();
    auto query_id = fragment_ctx->query_id();

    auto report_task = [params, fe_addr, instance_id, query_id]() {
        int retry_times = 0;
        int max_retry_times = config::report_exec_rpc_request_retry_num;
        while (retry_times++ < max_retry_times) {
            auto status = ExecStateReporter::report_exec_status(*params, fe_addr);

            FAIL_POINT_TRIGGER_EXECUTE(report_exec_state_failed_status, {
                if (status.ok()) {
                    status = Status::InternalError("injected failed status");
                }
            });

            if (!status.ok()) {
                if (status.is_not_found()) {
                    VLOG(1) << "[Driver] Fail to report exec state due to query not found. fragment_instance_id="
                            << print_id(instance_id) << ", query_id=" << print_id(query_id);
                } else {
                    LOG(WARNING) << "[Driver] Fail to report exec state. fragment_instance_id=" << print_id(instance_id)
                                 << ", query_id=" << print_id(query_id) << ", status: " << status.to_string()
                                 << ", retry_times=" << retry_times << ", max_retry_times=" << max_retry_times;
                    // if it is done exec state report, we should retry
                    if (params->__isset.done && params->done) {
                        continue;
                    }
                }
            } else {
                VLOG(1) << "[Driver] Succeed to report exec state. fragment_instance_id=" << print_id(instance_id)
                        << ", query_id=" << print_id(query_id) << ", is_done=" << params->done;
            }
            break;
        }
    };

    // if it is done exec state report, We need to ensure that this report is executed with priority
    // and is retried as much as possible to ensure success.
    // Otherwise, it may result in the ingestion status getting stuck.
    bool priority = done && fragment_ctx->runtime_state()->query_options().query_type == TQueryType::LOAD;
    submit(std::move(report_task), priority);
    VLOG(2) << "[Driver] Submit exec state report task. fragment_instance_id=" << print_id(instance_id)
            << ", query_id=" << print_id(query_id) << ", is_done=" << done;
}

ExecStateReporter::ExecStateReporter(const CpuUtil::CpuIds& cpuids, ExecStateReporterMetrics* metrics) {
    int exec_state_report_threads = std::max(1, config::exec_state_report_max_threads);
    auto status = ThreadPoolBuilder("exec_st_report") // exec state reporter
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
    status = ThreadPoolBuilder("prio_st_report") // priority exec state reporter with infinite queue
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
