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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/fragment_mgr.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/fragment_mgr.h"

#include <gperftools/profiler.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <functional>
#include <memory>
#include <sstream>

#include "agent/master_info.h"
#include "common/object_pool.h"
#include "exec/pipeline/fragment_executor.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/QueryPlanExtra_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/client_cache.h"
#include "runtime/current_thread.h"
#include "runtime/datetime_value.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/profile_report_worker.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_filter_worker.h"
#include "service/backend_options.h"
#include "util/misc.h"
#include "util/starrocks_metrics.h"
#include "util/stopwatch.hpp"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"
#include "util/url_coding.h"

namespace starrocks {

std::string to_load_error_http_path(const std::string& file_name) {
    if (file_name.empty()) {
        return "";
    }
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::be_http_port << "/api/_load_error_log?"
        << "file=" << file_name;
    return url.str();
}

using apache::thrift::TException;
using apache::thrift::TProcessor;
using apache::thrift::transport::TTransportException;

class RuntimeProfile;

class FragmentExecState {
public:
    FragmentExecState(const TUniqueId& query_id, const TUniqueId& instance_id, int backend_num, ExecEnv* exec_env,
                      const TNetworkAddress& coord_hostport);

    ~FragmentExecState();

    Status prepare(const TExecPlanFragmentParams& params);

    // just no use now
    void callback(const Status& status, RuntimeProfile* profile, bool done);

    std::string to_http_path(const std::string& file_name);

    Status execute();

    Status cancel(const PPlanFragmentCancelReason& reason);

    TUniqueId fragment_instance_id() const { return _fragment_instance_id; }

    PlanFragmentExecutor* executor() { return &_executor; }

    const DateTimeValue& start_time() const { return _start_time; }

    const TNetworkAddress& coord_addr() const { return _coord_addr; }

    ExecEnv* exec_env() const { return _exec_env; }

    const TUniqueId& query_id() const { return _query_id; }

    int backend_num() const { return _backend_num; }

    // Update status of this fragment execute
    Status update_status(const Status& status) {
        std::lock_guard<std::mutex> l(_status_lock);
        if (!status.ok() && _exec_status.ok()) {
            _exec_status = status;
        }
        return _exec_status;
    }

    bool is_timeout(const DateTimeValue& now) const {
        if (_timeout_second <= 0) {
            return false;
        }
        if (now.second_diff(_start_time) > _timeout_second) {
            return true;
        }
        return false;
    }

    int get_timeout_second() const { return _timeout_second; }

    std::shared_ptr<RuntimeState> runtime_state() { return _runtime_state; }

private:
    void coordinator_callback(const Status& status, RuntimeProfile* profile, bool done);

    std::shared_ptr<RuntimeState> _runtime_state = nullptr;

    // Id of this query
    TUniqueId _query_id;
    // Id of this instance
    TUniqueId _fragment_instance_id;
    // Used to reoprt to coordinator which backend is over
    int _backend_num;
    ExecEnv* _exec_env;
    TNetworkAddress _coord_addr;

    PlanFragmentExecutor _executor;
    DateTimeValue _start_time;

    std::mutex _status_lock;
    Status _exec_status;

    int _timeout_second{-1};
};

FragmentExecState::FragmentExecState(const TUniqueId& query_id, const TUniqueId& fragment_instance_id, int backend_num,
                                     ExecEnv* exec_env, const TNetworkAddress& coord_addr)
        : _query_id(query_id),
          _fragment_instance_id(fragment_instance_id),
          _backend_num(backend_num),
          _exec_env(exec_env),
          _coord_addr(coord_addr),
          _executor(exec_env, std::bind<void>(std::mem_fn(&FragmentExecState::coordinator_callback), this,
                                              std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)) {
    _start_time = DateTimeValue::local_time();
}

FragmentExecState::~FragmentExecState() = default;

Status FragmentExecState::prepare(const TExecPlanFragmentParams& params) {
    _runtime_state = std::make_shared<RuntimeState>(params.params.query_id, params.params.fragment_instance_id,
                                                    params.query_options, params.query_globals, _exec_env);
    int func_version = params.__isset.func_version ? params.func_version : 2;
    _runtime_state->set_func_version(func_version);
    _runtime_state->init_mem_trackers(_query_id);
    _executor.set_runtime_state(_runtime_state.get());

    if (params.__isset.query_options) {
        _timeout_second = params.query_options.query_timeout;
    }

    return _executor.prepare(params);
}

Status FragmentExecState::execute() {
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        WARN_IF_ERROR(_executor.open(),
                      strings::Substitute("Fail to open fragment $0", print_id(_fragment_instance_id)));
        _executor.close();
    }
    StarRocksMetrics::instance()->fragment_requests_total.increment(1);
    StarRocksMetrics::instance()->fragment_request_duration_us.increment(duration_ns / 1000);
    return Status::OK();
}

Status FragmentExecState::cancel(const PPlanFragmentCancelReason& reason) {
    std::lock_guard<std::mutex> l(_status_lock);
    RETURN_IF_ERROR(_exec_status);
    if (reason == PPlanFragmentCancelReason::LIMIT_REACH) {
        _executor.set_is_report_on_cancel(false);
    }
    _executor.cancel();
    return Status::OK();
}

void FragmentExecState::callback(const Status& status, RuntimeProfile* profile, bool done) {}

std::string FragmentExecState::to_http_path(const std::string& file_name) {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::be_http_port << "/api/_download_load?"
        << "token=" << _exec_env->token() << "&file=" << file_name;
    return url.str();
}

// There can only be one of these callbacks in-flight at any moment, because
// it is only invoked from the executor's reporting thread.
// Also, the reported status will always reflect the most recent execution status,
// including the final status when execution finishes.
void FragmentExecState::coordinator_callback(const Status& status, RuntimeProfile* profile, bool done) {
    DCHECK(status.ok() || done); // if !status.ok() => done
    Status exec_status = update_status(status);

    Status coord_status;
    FrontendServiceConnection coord(_exec_env->frontend_client_cache(), _coord_addr, &coord_status);
    if (!coord_status.ok()) {
        std::stringstream ss;
        ss << "couldn't get a client for " << _coord_addr;
        update_status(Status::InternalError(ss.str()));
        return;
    }

    TReportExecStatusParams params;
    params.protocol_version = FrontendServiceVersion::V1;
    params.__set_query_id(_query_id);
    params.__set_backend_num(_backend_num);
    params.__set_fragment_instance_id(_fragment_instance_id);
    exec_status.set_t_status(&params);
    params.__set_done(done);

    RuntimeState* runtime_state = _executor.runtime_state();
    DCHECK(runtime_state != nullptr);
    if (runtime_state->query_options().query_type == TQueryType::LOAD && !done && status.ok()) {
        // this is a load plan, and load is not finished, just make a brief report
        runtime_state->update_report_load_status(&params);
        params.__set_load_type(runtime_state->query_options().load_job_type);
    } else {
        if (runtime_state->query_options().query_type == TQueryType::LOAD) {
            runtime_state->update_report_load_status(&params);
            params.__set_load_type(runtime_state->query_options().load_job_type);
        }
        profile->to_thrift(&params.profile);
        params.__isset.profile = true;

        if (!runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : runtime_state->output_files()) {
                params.delta_urls.push_back(to_http_path(it));
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
            params.__set_failInfos(runtime_state->tablet_fail_infos());
        }

        // Send new errors to coordinator
        runtime_state->get_unreported_errors(&(params.error_log));
        params.__isset.error_log = (params.error_log.size() > 0);
    }

    auto backend_id = get_backend_id();
    if (backend_id.has_value()) {
        params.__set_backend_id(backend_id.value());
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
                // we need to cancel the execution of this fragment
                update_status(rpc_status);
                _executor.cancel();
                return;
            }
            coord->reportExecStatus(res, params);
        }

        rpc_status = Status(res.status);
    } catch (TException& e) {
        std::stringstream msg;
        msg << "ReportExecStatus() to " << _coord_addr << " failed:\n" << e.what();
        LOG(WARNING) << msg.str();
        rpc_status = Status::InternalError(msg.str());
    }

    if (!rpc_status.ok()) {
        // we need to cancel the execution of this fragment
        update_status(rpc_status);
        _executor.cancel();
    }
}

FragmentMgr::FragmentMgr(ExecEnv* exec_env)
        : _exec_env(exec_env), _stop(false), _cancel_thread([this] { cancel_worker(); }) {
    Thread::set_thread_name(_cancel_thread, "frag_mgr_cancel");
    REGISTER_GAUGE_STARROCKS_METRIC(plan_fragment_count, [this]() {
        std::lock_guard<std::mutex> lock(_lock);
        return _fragment_map.size();
    });
    // TODO(zc): we need a better thread-pool
    // now one user can use all the thread pool, others have no resource.
    ThreadPoolBuilder("fragment_mgr")
            .set_min_threads(config::fragment_pool_thread_num_min)
            .set_max_threads(config::fragment_pool_thread_num_max)
            .set_max_queue_size(config::fragment_pool_queue_size)
            .build(&_thread_pool);
}

FragmentMgr::~FragmentMgr() {
    // stop thread
    _stop = true;
    _cancel_thread.join();
    // Stop all the worker, should wait for a while?
    // _thread_pool->wait_for();
    _thread_pool->shutdown();

    // Only me can delete
    {
        std::lock_guard<std::mutex> lock(_lock);
        _fragment_map.clear();
    }
}

static void empty_function(PlanFragmentExecutor* exec) {}

void FragmentMgr::exec_actual(const std::shared_ptr<FragmentExecState>& exec_state, const FinishCallback& cb) {
    // This writing is to ensure that MemTracker will not be destructed before the thread ends.
    // This writing method is a bit tricky, and when there is a better way, replace it
    auto profile = exec_state->runtime_state()->runtime_profile_ptr();
    auto q_tracker = exec_state->runtime_state()->query_mem_tracker_ptr();
    auto s_tracker = exec_state->runtime_state()->instance_mem_tracker_ptr();

    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(s_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    exec_state->execute();

    // Callback after remove from this id
    cb(exec_state->executor());

    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(exec_state->fragment_instance_id());
        if (iter != _fragment_map.end()) {
            _fragment_map.erase(iter);
        } else {
            // Impossible
            LOG(WARNING) << "missing entry in fragment exec state map: instance_id="
                         << exec_state->fragment_instance_id();
        }
    }
    // NOTE: 'exec_state' is deconstructed here without lock
}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params) {
    return exec_plan_fragment(params, [](auto&& PH1) { return empty_function(std::forward<decltype(PH1)>(PH1)); });
}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params, const FinishCallback& cb) {
    return exec_plan_fragment(
            params, [](auto&& PH1) { return empty_function(std::forward<decltype(PH1)>(PH1)); }, cb);
}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params, const StartSuccCallback& start_cb,
                                       const FinishCallback& cb) {
    RETURN_IF_ERROR(_exec_env->query_pool_mem_tracker()->check_mem_limit("Start execute plan fragment."));

    const TUniqueId& fragment_instance_id = params.params.fragment_instance_id;
    std::shared_ptr<FragmentExecState> exec_state;
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(fragment_instance_id);
        if (iter != _fragment_map.end()) {
            // Duplicated
            return Status::OK();
        }
    }
    exec_state.reset(new FragmentExecState(params.params.query_id, fragment_instance_id, params.backend_num, _exec_env,
                                           params.coord));
    RETURN_IF_ERROR_WITH_WARN(exec_state->prepare(params), "Fail to prepare Fragment");

    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(fragment_instance_id);
        if (iter != _fragment_map.end()) {
            // Duplicated
            return Status::InternalError("Double execute");
        }
        // register exec_state before starting exec thread
        _fragment_map.insert(std::make_pair(fragment_instance_id, exec_state));
    }

    auto st = _thread_pool->submit_func([this, exec_state, cb] { exec_actual(exec_state, cb); });
    if (!st.ok()) {
        exec_state->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR);
        std::string error_msg = strings::Substitute("Put planfragment $0 to thread pool failed. err = $1",
                                                    print_id(fragment_instance_id), st.get_error_msg());
        LOG(WARNING) << error_msg;
        {
            // Remove the exec state added
            std::lock_guard<std::mutex> lock(_lock);
            _fragment_map.erase(fragment_instance_id);
        }
        return Status::InternalError(error_msg);
    } else {
        // It is necessary to ensure that ExecState is not destructed,
        // so do not reset ExecState before calling start_cb, otherwise "heap use after free" may appear
        start_cb(exec_state->executor());
    }

    return Status::OK();
}

Status FragmentMgr::cancel(const TUniqueId& id, const PPlanFragmentCancelReason& reason) {
    std::shared_ptr<FragmentExecState> exec_state;
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(id);
        if (iter == _fragment_map.end()) {
            // No match
            return Status::OK();
        }
        exec_state = iter->second;
    }
    auto profile = exec_state->runtime_state()->runtime_profile_ptr();
    auto q_tracker = exec_state->runtime_state()->query_mem_tracker_ptr();
    auto s_tracker = exec_state->runtime_state()->instance_mem_tracker_ptr();
    exec_state->cancel(reason);
    exec_state.reset();

    return Status::OK();
}

void FragmentMgr::receive_runtime_filter(const PTransmitRuntimeFilterParams& params,
                                         const std::shared_ptr<const JoinRuntimeFilter>& shared_rf) {
    std::shared_ptr<FragmentExecState> exec_state;
    const PUniqueId& query_id = params.query_id();
    _exec_env->add_rf_event({query_id, params.filter_id(), BackendOptions::get_localhost(), "RECV_TOTAL_RF_RPC"});
    size_t size = params.probe_finst_ids_size();
    for (size_t i = 0; i < size; i++) {
        TUniqueId frag_inst_id;
        frag_inst_id.hi = params.probe_finst_ids(i).hi();
        frag_inst_id.lo = params.probe_finst_ids(i).lo();
        bool found = true;
        {
            std::lock_guard<std::mutex> lock(_lock);
            auto iter = _fragment_map.find(frag_inst_id);
            if (iter == _fragment_map.end()) {
                // No match
                found = false;
            } else {
                exec_state = iter->second;
            }
        }
        if (!found) {
            VLOG_FILE << "FragmentMgr::receive_runtime_filter: finst not found. finst_id = " << frag_inst_id
                      << ", filter_id = " << params.filter_id();
            TUniqueId tquery_id;
            tquery_id.lo = query_id.lo();
            tquery_id.hi = query_id.hi();
            _exec_env->runtime_filter_cache()->put_if_absent(tquery_id, params.filter_id(), shared_rf);
        } else {
            auto profile = exec_state->runtime_state()->runtime_profile_ptr();
            auto q_tracker = exec_state->runtime_state()->query_mem_tracker_ptr();
            auto s_tracker = exec_state->runtime_state()->instance_mem_tracker_ptr();
            exec_state->executor()->runtime_state()->runtime_filter_port()->receive_shared_runtime_filter(
                    params.filter_id(), shared_rf);
            exec_state.reset();
        }
    }
}

void FragmentMgr::cancel_worker() {
    LOG(INFO) << "FragmentMgr cancel worker start working.";
    while (!_stop) {
        std::vector<TUniqueId> to_delete;
        DateTimeValue now = DateTimeValue::local_time();
        {
            std::lock_guard<std::mutex> lock(_lock);
            for (auto& it : _fragment_map) {
                if (it.second->is_timeout(now)) {
                    to_delete.push_back(it.second->fragment_instance_id());
                }
            }
        }
        for (auto& id : to_delete) {
            cancel(id, PPlanFragmentCancelReason::TIMEOUT);
            LOG(INFO) << "FragmentMgr cancel worker going to cancel timeout fragment " << print_id(id);
        }
        nap_sleep(1, [this] { return _stop; });
    }
    LOG(INFO) << "FragmentMgr cancel worker is going to exit.";
}

Status FragmentMgr::trigger_profile_report(const PTriggerProfileReportRequest* request) {
    std::vector<std::shared_ptr<FragmentExecState>> need_report_exec_states;
    if (request->instance_ids_size() > 0) {
        for (int i = 0; i < request->instance_ids_size(); i++) {
            const PUniqueId& p_fragment_id = request->instance_ids(i);
            TUniqueId id;
            id.__set_hi(p_fragment_id.hi());
            id.__set_lo(p_fragment_id.lo());
            {
                std::lock_guard<std::mutex> lock(_lock);
                auto iter = _fragment_map.find(id);
                if (iter != _fragment_map.end()) {
                    need_report_exec_states.emplace_back(iter->second);
                }
            }
        }
    } else {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.begin();
        for (; iter != _fragment_map.end(); iter++) {
            need_report_exec_states.emplace_back(iter->second);
        }
    }

    for (auto& exec_state : need_report_exec_states) {
        exec_state->executor()->report_profile_once();
    }

    return Status::OK();
}

void FragmentMgr::report_fragments_with_same_host(
        const std::vector<std::shared_ptr<FragmentExecState>>& need_report_exec_states, std::vector<bool>& reported,
        const TNetworkAddress& last_coord_addr, std::vector<TReportExecStatusParams>& report_exec_status_params_vector,
        std::vector<int32_t>& cur_batch_report_indexes) {
    for (int i = 0; i < need_report_exec_states.size(); i++) {
        if (reported[i] == false) {
            FragmentExecState* fragment_exec_state = need_report_exec_states[i].get();
            PlanFragmentExecutor* executor = fragment_exec_state->executor();

            if (executor->is_done()) {
                reported[i] = true;
                continue;
            }

            Status executor_status = executor->status();
            if (!executor_status.ok()) {
                reported[i] = true;
                starrocks::ExecEnv::GetInstance()->profile_report_worker()->unregister_non_pipeline_load(
                        fragment_exec_state->fragment_instance_id());
                continue;
            }

            const TNetworkAddress& coord_addr = fragment_exec_state->coord_addr();
            if (coord_addr == last_coord_addr) {
                TReportExecStatusParams params;
                params.protocol_version = FrontendServiceVersion::V1;
                params.__set_query_id(fragment_exec_state->query_id());
                params.__set_backend_num(fragment_exec_state->backend_num());
                params.__set_fragment_instance_id(fragment_exec_state->fragment_instance_id());
                executor_status.set_t_status(&params);
                params.__set_done(executor->is_done());

                RuntimeState* runtime_state = executor->runtime_state();
                DCHECK(runtime_state != nullptr);
                if (runtime_state->query_options().query_type == TQueryType::LOAD) {
                    runtime_state->update_report_load_status(&params);
                    params.__set_load_type(runtime_state->query_options().load_job_type);
                }

                auto backend_id = get_backend_id();
                if (backend_id.has_value()) {
                    params.__set_backend_id(backend_id.value());
                }

                report_exec_status_params_vector.push_back(params);
                cur_batch_report_indexes.push_back(i);
                reported[i] = true;
            }
        }
    }
}

void FragmentMgr::report_fragments(const std::vector<TUniqueId>& non_pipeline_need_report_fragment_ids) {
    std::vector<std::shared_ptr<FragmentExecState>> need_report_exec_states;

    std::vector<TUniqueId> fragments_non_exist;

    std::unique_lock<std::mutex> lock(_lock);
    for (const auto& id : non_pipeline_need_report_fragment_ids) {
        auto iter = _fragment_map.find(id);
        if (iter != _fragment_map.end()) {
            need_report_exec_states.emplace_back(iter->second);
        } else {
            fragments_non_exist.emplace_back(id);
        }
    }
    lock.unlock();

    std::vector<bool> reported(need_report_exec_states.size(), false);
    for (int i = 0; i < need_report_exec_states.size(); i++) {
        if (reported[i] == false) {
            reported[i] = true;

            FragmentExecState* fragment_exec_state = need_report_exec_states[i].get();
            PlanFragmentExecutor* executor = fragment_exec_state->executor();

            if (executor->is_done()) {
                continue;
            }

            Status executor_status = executor->status();
            if (!executor_status.ok()) {
                starrocks::ExecEnv::GetInstance()->profile_report_worker()->unregister_non_pipeline_load(
                        fragment_exec_state->fragment_instance_id());
                continue;
            }

            Status fe_connection_status;

            FrontendServiceConnection fe_connection(fragment_exec_state->exec_env()->frontend_client_cache(),
                                                    fragment_exec_state->coord_addr(), &fe_connection_status);
            if (!fe_connection_status.ok()) {
                std::stringstream ss;
                ss << "couldn't get a client for " << fragment_exec_state->coord_addr();
                LOG(WARNING) << ss.str();
                starrocks::ExecEnv::GetInstance()->profile_report_worker()->unregister_non_pipeline_load(
                        fragment_exec_state->fragment_instance_id());
                fragment_exec_state->exec_env()->frontend_client_cache()->close_connections(
                        fragment_exec_state->coord_addr());
                continue;
            }

            std::vector<TReportExecStatusParams> report_exec_status_params_vector;

            TReportExecStatusParams params;
            params.protocol_version = FrontendServiceVersion::V1;
            params.__set_query_id(fragment_exec_state->query_id());
            params.__set_backend_num(fragment_exec_state->backend_num());
            params.__set_fragment_instance_id(fragment_exec_state->fragment_instance_id());
            executor_status.set_t_status(&params);
            params.__set_done(executor->is_done());

            RuntimeState* runtime_state = executor->runtime_state();
            DCHECK(runtime_state != nullptr);
            if (runtime_state->query_options().query_type == TQueryType::LOAD) {
                runtime_state->update_report_load_status(&params);
                params.__set_load_type(runtime_state->query_options().load_job_type);
            }

            auto backend_id = get_backend_id();
            if (backend_id.has_value()) {
                params.__set_backend_id(backend_id.value());
            }

            report_exec_status_params_vector.push_back(params);

            std::vector<int32_t> cur_batch_report_indexes;
            cur_batch_report_indexes.push_back(i);

            report_fragments_with_same_host(need_report_exec_states, reported, fragment_exec_state->coord_addr(),
                                            report_exec_status_params_vector, cur_batch_report_indexes);

            TBatchReportExecStatusParams report_batch;
            report_batch.__set_params_list(report_exec_status_params_vector);

            TBatchReportExecStatusResult res;
            Status rpc_status;

            VLOG_ROW << "debug: reportExecStatus params is " << apache::thrift::ThriftDebugString(params).c_str();
            try {
                try {
                    fe_connection->batchReportExecStatus(res, report_batch);
                } catch (TTransportException& e) {
                    LOG(WARNING) << "Retrying ReportExecStatus: " << e.what();
                    rpc_status = fe_connection.reopen();
                    if (!rpc_status.ok()) {
                        continue;
                    }
                    fe_connection->batchReportExecStatus(res, report_batch);
                }

            } catch (TException& e) {
                std::stringstream msg;
                msg << "ReportExecStatus() to " << fragment_exec_state->coord_addr() << " failed:\n" << e.what();
                LOG(WARNING) << msg.str();
            }

            const std::vector<TStatus>& status_list = res.status_list;
            for (int j = 0; j < status_list.size(); j++) {
                Status rpc_status = Status(status_list[j]);
                if (!rpc_status.ok()) {
                    int32_t index = cur_batch_report_indexes[j];
                    FragmentExecState* fragment_exec_state = need_report_exec_states[index].get();
                    PlanFragmentExecutor* executor = fragment_exec_state->executor();
                    fragment_exec_state->update_status(rpc_status);
                    executor->cancel();
                }
            }
        }
    }

    for (const auto& fragment_instance_id : fragments_non_exist) {
        starrocks::ExecEnv::GetInstance()->profile_report_worker()->unregister_non_pipeline_load(fragment_instance_id);
    }
}

void FragmentMgr::debug(std::stringstream& ss) {
    // Keep things simple
    std::lock_guard<std::mutex> lock(_lock);

    ss << "FragmentMgr have " << _fragment_map.size() << " jobs.\n";
    ss << "job_id\t\tstart_time\t\texecute_time(s)\n";
    DateTimeValue now = DateTimeValue::local_time();
    for (auto& it : _fragment_map) {
        ss << it.first << "\t" << it.second->start_time().debug_string() << "\t"
           << now.second_diff(it.second->start_time()) << "\n";
    }
}

/*
 * 1. resolve opaqued_query_plan to thrift structure
 * 2. build TExecPlanFragmentParams
 */
Status FragmentMgr::exec_external_plan_fragment(const TScanOpenParams& params, const TUniqueId& fragment_instance_id,
                                                std::vector<TScanColumnDesc>* selected_columns, TUniqueId* query_id) {
    const std::string& opaqued_query_plan = params.opaqued_query_plan;
    std::string query_plan_info;
    // base64 decode query plan
    if (!base64_decode(opaqued_query_plan, &query_plan_info)) {
        LOG(WARNING) << "open context error: base64_decode decode opaqued_query_plan failure";
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info
            << " validate error, should not be modified after returned StarRocks FE processed";
        return Status::InvalidArgument(msg.str());
    }
    TQueryPlanInfo t_query_plan_info;
    const auto* buf = (const uint8_t*)query_plan_info.data();
    uint32_t len = query_plan_info.size();
    // deserialize TQueryPlanInfo
    auto st = deserialize_thrift_msg(buf, &len, TProtocolType::BINARY, &t_query_plan_info);
    if (!st.ok()) {
        LOG(WARNING) << "open context error: deserialize TQueryPlanInfo failure";
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info
            << " deserialize error, should not be modified after returned StarRocks FE processed";
        return Status::InvalidArgument(msg.str());
    }

    *query_id = t_query_plan_info.query_id;

    // set up desc tbl
    DescriptorTbl* desc_tbl = nullptr;
    ObjectPool obj_pool;
    st = DescriptorTbl::create(nullptr, &obj_pool, t_query_plan_info.desc_tbl, &desc_tbl, params.batch_size);
    if (!st.ok()) {
        LOG(WARNING) << "open context error: extract DescriptorTbl failure";
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info
            << " create DescriptorTbl error, should not be modified after returned StarRocks FE "
               "processed";
        return Status::InvalidArgument(msg.str());
    }

    const auto& output_names = t_query_plan_info.output_names;
    int i = 0;
    for (const auto& expr : t_query_plan_info.plan_fragment.output_exprs) {
        const auto& nodes = expr.nodes;
        if (nodes.empty() || nodes[0].node_type != TExprNodeType::SLOT_REF) {
            LOG(WARNING) << "output expr is not slot ref";
            return Status::InvalidArgument("output expr is not slot ref");
        }
        const auto& slot_ref = nodes[0].slot_ref;
        auto* tuple_desc = desc_tbl->get_tuple_descriptor(slot_ref.tuple_id);
        if (tuple_desc == nullptr) {
            LOG(WARNING) << "tuple descriptor is null. id: " << slot_ref.tuple_id;
            return Status::InvalidArgument("tuple descriptor is null");
        }
        auto* slot_desc = desc_tbl->get_slot_descriptor(slot_ref.slot_id);
        if (slot_desc == nullptr) {
            LOG(WARNING) << "slot descriptor is null. id: " << slot_ref.slot_id;
            return Status::InvalidArgument("slot descriptor is null");
        }

        TScanColumnDesc col;
        if (!output_names.empty()) {
            col.__set_name(output_names[i]);
        } else {
            col.__set_name(slot_desc->col_name());
        }
        col.__set_type(to_thrift(slot_desc->type().type));
        selected_columns->emplace_back(std::move(col));
        i++;
    }

    LOG(INFO) << "BackendService execute open()  TQueryPlanInfo: "
              << apache::thrift::ThriftDebugString(t_query_plan_info);
    // assign the param used to execute PlanFragment
    TExecPlanFragmentParams exec_fragment_params;
    exec_fragment_params.protocol_version = (InternalServiceVersion::type)0;
    exec_fragment_params.__set_fragment(t_query_plan_info.plan_fragment);
    exec_fragment_params.__set_desc_tbl(t_query_plan_info.desc_tbl);
    exec_fragment_params.__set_backend_num(1);
    exec_fragment_params.__set_pipeline_dop(1);

    // assign the param used for executing of PlanFragment-self
    TPlanFragmentExecParams fragment_exec_params;
    fragment_exec_params.query_id = t_query_plan_info.query_id;
    fragment_exec_params.fragment_instance_id = fragment_instance_id;
    std::map<::starrocks::TPlanNodeId, std::vector<TScanRangeParams>> per_node_scan_ranges;
    std::vector<TScanRangeParams> scan_ranges;
    std::vector<int64_t> tablet_ids = params.tablet_ids;
    TNetworkAddress address;
    address.hostname = BackendOptions::get_localhost();
    address.port = starrocks::config::be_port;
    std::map<int64_t, TTabletVersionInfo> tablet_info = t_query_plan_info.tablet_info;
    for (auto tablet_id : params.tablet_ids) {
        TInternalScanRange scan_range;
        scan_range.db_name = params.database;
        scan_range.table_name = params.table;
        auto iter = tablet_info.find(tablet_id);
        if (iter != tablet_info.end()) {
            TTabletVersionInfo info = iter->second;
            scan_range.tablet_id = tablet_id;
            scan_range.version = std::to_string(info.version);
            scan_range.schema_hash = std::to_string(info.schema_hash);
            scan_range.hosts.push_back(address);
        } else {
            std::stringstream msg;
            msg << "tablet_id: " << tablet_id << " not found";
            LOG(WARNING) << "tablet_id [ " << tablet_id << " ] not found";
            return Status::NotFound(msg.str());
        }
        TScanRange starrocks_scan_range;
        starrocks_scan_range.__set_internal_scan_range(scan_range);
        TScanRangeParams scan_range_params;
        scan_range_params.scan_range = starrocks_scan_range;
        scan_ranges.push_back(scan_range_params);
    }
    per_node_scan_ranges.insert(std::make_pair((::starrocks::TPlanNodeId)0, scan_ranges));
    fragment_exec_params.per_node_scan_ranges = per_node_scan_ranges;
    // set a mock sender id
    fragment_exec_params.__set_sender_id(0);
    fragment_exec_params.__set_instances_number(1);
    exec_fragment_params.__set_params(fragment_exec_params);
    // batch_size for one RowBatch
    TQueryOptions query_options;
    query_options.batch_size = params.batch_size;
    query_options.query_timeout = params.query_timeout;
    query_options.mem_limit = params.mem_limit;
    query_options.query_type = TQueryType::EXTERNAL;
    // For spark sql / flink sql, we dont use page cache.
    query_options.use_page_cache = false;
    query_options.use_column_pool = false;
    query_options.enable_profile = config::enable_profile_for_external_plan;
    exec_fragment_params.__set_query_options(query_options);
    VLOG_ROW << "external exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(exec_fragment_params).c_str();
    pipeline::FragmentExecutor fragment_executor;
    auto status = fragment_executor.prepare(ExecEnv::GetInstance(), exec_fragment_params, exec_fragment_params);
    if (status.ok()) {
        return fragment_executor.execute(ExecEnv::GetInstance());
    }
    return status.is_duplicate_rpc_invocation() ? Status::OK() : status;
}

} // namespace starrocks
