// This file is made available under Elastic License 2.0.
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
#include <thread>

#include "common/object_pool.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/QueryPlanExtra_types.h"
#include "gen_cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/client_cache.h"
#include "runtime/current_thread.h"
#include "runtime/datetime_value.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/runtime_filter_worker.h"
#include "service/backend_options.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
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
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port << "/api/_load_error_log?"
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

    // Update status of this fragment execute
    Status update_status(const Status& status) {
        std::lock_guard<std::mutex> l(_status_lock);
        if (!status.ok() && _exec_status.ok()) {
            _exec_status = status;
        }
        return _exec_status;
    }

    void set_group(const TResourceInfo& info) {
        _set_rsc_info = true;
        _user = info.user;
        _group = info.group;
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
    Status report_exec_status(const TReportExecStatusParams& params);

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

    bool _set_rsc_info;
    std::string _user;
    std::string _group;

    int _timeout_second;
};

FragmentExecState::FragmentExecState(const TUniqueId& query_id, const TUniqueId& fragment_instance_id, int backend_num,
                                     ExecEnv* exec_env, const TNetworkAddress& coord_addr)
        : _query_id(query_id),
          _fragment_instance_id(fragment_instance_id),
          _backend_num(backend_num),
          _exec_env(exec_env),
          _coord_addr(coord_addr),
          _executor(exec_env, std::bind<void>(std::mem_fn(&FragmentExecState::coordinator_callback), this,
                                              std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)),
          _set_rsc_info(false),
          _timeout_second(-1) {
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

    if (params.__isset.resource_info) {
        set_group(params.resource_info);
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
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port << "/api/_download_load?"
        << "token=" << _exec_env->token() << "&file=" << file_name;
    return url.str();
}

Status FragmentExecState::report_exec_status(const TReportExecStatusParams& params) {
    TReportExecStatusResult res;
    Status st;

    FrontendServiceConnection coord(_exec_env->frontend_client_cache(), _coord_addr, &st);
    if (!st.ok()) {
        std::stringstream ss;
        ss << "couldn't get a client for " << _coord_addr << ", error: " << st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    VLOG_ROW << "debug: reportExecStatus params is " << apache::thrift::ThriftDebugString(params).c_str();
    try {
        try {
            coord->reportExecStatus(res, params);
        } catch (TTransportException& e) {
            LOG(WARNING) << "Retrying ReportExecStatus due to: " << e.what();
            RETURN_IF_ERROR_WITH_WARN(coord.reopen(), "reopen client:");
            coord->reportExecStatus(res, params);
        }

        st = Status(res.status);
    } catch (TException& e) {
        std::stringstream msg;
        msg << "ReportExecStatus() to " << _coord_addr << " failed:\n" << e.what();
        LOG(WARNING) << msg.str();
        return Status::InternalError(msg.str());
    }
    WARN_IF_ERROR(st, "reportExecStatus() error: ");
    return st;
}

// There can only be one of these callbacks in-flight at any moment, because
// it is only invoked from the executor's reporting thread.
// Also, the reported status will always reflect the most recent execution status,
// including the final status when execution finishes.
void FragmentExecState::coordinator_callback(const Status& status, RuntimeProfile* profile, bool done) {
    DCHECK(status.ok() || done); // if !status.ok() => done
    Status exec_status = update_status(status);

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
        params.__set_loaded_rows(runtime_state->num_rows_load_total());
    } else {
        if (runtime_state->query_options().query_type == TQueryType::LOAD) {
            params.__set_loaded_rows(runtime_state->num_rows_load_total());
        }
        profile->to_thrift(&params.profile);
        params.__isset.profile = true;

        if (!runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : runtime_state->output_files()) {
                params.delta_urls.push_back(to_http_path(it));
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

    if (_exec_env->master_info()->__isset.backend_id) {
        params.__set_backend_id(_exec_env->master_info()->backend_id);
    }

    auto st = report_exec_status(params);
    if (st.ok()) {
        return;
    }

    LOG(WARNING) << "Failed to report exec status to coordinator, instance_id: " << print_id(_fragment_instance_id);

    // Since the load coordinator is awaiting the result of fragment instance,
    // we try our best to report the load status.
    if (runtime_state->query_options().query_type == TQueryType::LOAD) {
        for (size_t i = 0; i < config::max_load_status_report_retry_times; i++) {
            // sleep with backoff
            std::this_thread::sleep_for(std::chrono::seconds((4 << i)));

            st = report_exec_status(params);
            if (st.ok()) {
                return;
            }
            LOG(WARNING) << "Failed to report exec status to coordinator, instance_id: "
                         << print_id(_fragment_instance_id) << ", retry: " << i + 1 << "/"
                         << config::max_load_status_report_retry_times;
        }
        LOG(WARNING) << "Abort to report exec status to coordinator, instance_id: " << print_id(_fragment_instance_id)
                     << ", retry times:  " << config::max_load_status_report_retry_times;
    }

    if (!st.ok()) {
        // we need to cancel the execution of this fragment
        update_status(st);
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

void FragmentMgr::exec_actual(std::shared_ptr<FragmentExecState> exec_state, const FinishCallback& cb) {
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
    // NOTE: 'exec_state' is desconstructed here without lock
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
                                         const std::shared_ptr<const vectorized::JoinRuntimeFilter>& shared_rf) {
    std::shared_ptr<FragmentExecState> exec_state;
    _exec_env->add_rf_event(
            {params.query_id(), params.filter_id(), BackendOptions::get_localhost(), "RECV_TOTAL_RF_RPC"});
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
            VLOG_FILE << "FragmentMgr::receive_runtime_filter: finst not found. finst_id = " << frag_inst_id;
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

        // check every 1 seconds
        sleep(1);
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
                                                std::vector<TScanColumnDesc>* selected_columns) {
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
    const uint8_t* buf = (const uint8_t*)query_plan_info.data();
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

    // set up desc tbl
    DescriptorTbl* desc_tbl = nullptr;
    ObjectPool obj_pool;
    st = DescriptorTbl::create(&obj_pool, t_query_plan_info.desc_tbl, &desc_tbl, params.batch_size);
    if (!st.ok()) {
        LOG(WARNING) << "open context error: extract DescriptorTbl failure";
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info
            << " create DescriptorTbl error, should not be modified after returned StarRocks FE "
               "processed";
        return Status::InvalidArgument(msg.str());
    }

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
        col.__set_name(slot_desc->col_name());
        col.__set_type(to_thrift(slot_desc->type().type));
        selected_columns->emplace_back(std::move(col));
    }

    LOG(INFO) << "BackendService execute open()  TQueryPlanInfo: "
              << apache::thrift::ThriftDebugString(t_query_plan_info);
    // assign the param used to execute PlanFragment
    TExecPlanFragmentParams exec_fragment_params;
    exec_fragment_params.protocol_version = (InternalServiceVersion::type)0;
    exec_fragment_params.__set_fragment(t_query_plan_info.plan_fragment);
    exec_fragment_params.__set_desc_tbl(t_query_plan_info.desc_tbl);

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
    exec_fragment_params.__set_params(fragment_exec_params);
    // batch_size for one RowBatch
    TQueryOptions query_options;
    query_options.batch_size = params.batch_size;
    query_options.query_timeout = params.query_timeout;
    query_options.mem_limit = params.mem_limit;
    query_options.query_type = TQueryType::EXTERNAL;
    exec_fragment_params.__set_query_options(query_options);
    VLOG_ROW << "external exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(exec_fragment_params).c_str();
    return exec_plan_fragment(exec_fragment_params);
}

} // namespace starrocks
