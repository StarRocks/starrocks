// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/runtime_state.h

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

#pragma once

#include <atomic>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "cctz/time_zone.h"
#include "common/constexpr.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/InternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"           // for TUniqueId
#include "runtime/global_dict/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/logging.h"
#include "util/runtime_profile.h"

namespace starrocks {

class DescriptorTbl;
class ObjectPool;
class Status;
class ExecEnv;
class Expr;
class DateTimeValue;
class MemTracker;
class DataStreamRecvr;
class ResultBufferMgr;
class LoadErrorHub;
class RowDescriptor;
class RuntimeFilterPort;
class QueryStatistics;
class QueryStatisticsRecvr;

namespace pipeline {
class QueryContext;
}

constexpr int64_t kRpcHttpMinSize = ((1L << 31) - (1L << 10));

// A collection of items that are part of the global state of a
// query and shared across all execution nodes of that query.
class RuntimeState {
public:
    // for ut only
    RuntimeState() {}
    // for ut only
    RuntimeState(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env);

    RuntimeState(const TUniqueId& query_id, const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env);

    // RuntimeState for executing expr in fe-support.
    explicit RuntimeState(const TQueryGlobals& query_globals);

    // Empty d'tor to avoid issues with std::unique_ptr.
    ~RuntimeState();

    // Set up four-level hierarchy of mem trackers: process, query, fragment instance.
    // The instance tracker is tied to our profile.
    // Specific parts of the fragment (i.e. exec nodes, sinks, data stream senders, etc)
    // will add a fourth level when they are initialized.
    // This function also initializes a user function mem tracker (in the fourth level).
    void init_mem_trackers(const TUniqueId& query_id, MemTracker* parent = nullptr);

    void init_mem_trackers(const std::shared_ptr<MemTracker>& query_mem_tracker);

    // for ut only
    Status init_instance_mem_tracker();

    const TQueryOptions& query_options() const { return _query_options; }
    ObjectPool* obj_pool() const { return _obj_pool.get(); }
    ObjectPool* global_obj_pool() const;
    void set_query_ctx(pipeline::QueryContext* ctx) { _query_ctx = ctx; }
    pipeline::QueryContext* query_ctx() { return _query_ctx; }
    const DescriptorTbl& desc_tbl() const { return *_desc_tbl; }
    void set_desc_tbl(DescriptorTbl* desc_tbl) { _desc_tbl = desc_tbl; }
    int chunk_size() const { return _query_options.batch_size; }
    void set_chunk_size(int chunk_size) { _query_options.batch_size = chunk_size; }
    bool abort_on_default_limit_exceeded() const { return _query_options.abort_on_default_limit_exceeded; }
    int64_t timestamp_ms() const { return _timestamp_ms; }
    const std::string& timezone() const { return _timezone; }
    const cctz::time_zone& timezone_obj() const { return _timezone_obj; }
    const std::string& user() const { return _user; }
    const std::vector<std::string>& error_log() const { return _error_log; }
    const std::string& last_query_id() const { return _last_query_id; }
    const TUniqueId& query_id() const { return _query_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    ExecEnv* exec_env() { return _exec_env; }
    MemTracker* instance_mem_tracker() { return _instance_mem_tracker.get(); }
    MemPool* instance_mem_pool() { return _instance_mem_pool.get(); }
    std::shared_ptr<MemTracker> query_mem_tracker_ptr() { return _query_mem_tracker; }
    const std::shared_ptr<MemTracker>& query_mem_tracker_ptr() const { return _query_mem_tracker; }
    std::shared_ptr<MemTracker> instance_mem_tracker_ptr() { return _instance_mem_tracker; }
    RuntimeFilterPort* runtime_filter_port() { return _runtime_filter_port; }
    const std::atomic<bool>& cancelled_ref() const { return _is_cancelled; }

    void set_fragment_root_id(PlanNodeId id) {
        DCHECK(_root_node_id == -1) << "Should not set this twice.";
        _root_node_id = id;
    }

    // Returns runtime state profile
    RuntimeProfile* runtime_profile() { return _profile.get(); }
    std::shared_ptr<RuntimeProfile> runtime_profile_ptr() { return _profile; }

    Status query_status() {
        std::lock_guard<std::mutex> l(_process_status_lock);
        return _process_status;
    };

    // Appends error to the _error_log if there is space
    bool log_error(const std::string& error);

    // If !status.ok(), appends the error to the _error_log
    void log_error(const Status& status);

    // Returns true if the error log has not reached _max_errors.
    bool log_has_space() {
        std::lock_guard<std::mutex> l(_error_log_lock);
        return _error_log.size() < _query_options.max_errors;
    }

    // Returns the error log lines as a string joined with '\n'.
    std::string error_log();

    // Append all _error_log[_unreported_error_idx+] to new_errors and set
    // _unreported_error_idx to _errors_log.size()
    void get_unreported_errors(std::vector<std::string>* new_errors);

    bool is_cancelled() const { return _is_cancelled.load(std::memory_order_acquire); }
    void set_is_cancelled(bool v) { _is_cancelled.store(v, std::memory_order_release); }

    void set_be_number(int be_number) { _be_number = be_number; }
    int be_number() const { return _be_number; }

    // Sets _process_status with err_msg if no error has been set yet.
    void set_process_status(const std::string& err_msg) {
        std::lock_guard<std::mutex> l(_process_status_lock);
        if (!_process_status.ok()) {
            return;
        }
        _process_status = Status::InternalError(err_msg);
    }

    void set_process_status(const Status& status) {
        if (status.ok()) {
            return;
        }
        std::lock_guard<std::mutex> l(_process_status_lock);
        if (!_process_status.ok()) {
            return;
        }
        _process_status = status;
    }

    // Sets query_status_ to MEM_LIMIT_EXCEEDED and logs all the registered trackers.
    // Subsequent calls to this will be no-ops. Returns query_status_.
    // If 'failed_allocation_size' is not 0, then it is the size of the allocation (in
    // bytes) that would have exceeded the limit allocated for 'tracker'.
    // This value and tracker are only used for error reporting.
    // If 'msg' is non-NULL, it will be appended to query_status_ in addition to the
    // generic "Memory limit exceeded" error.
    Status set_mem_limit_exceeded(MemTracker* tracker = nullptr, int64_t failed_allocation_size = 0,
                                  const std::string* msg = nullptr);

    Status set_mem_limit_exceeded(const std::string& msg) { return set_mem_limit_exceeded(nullptr, 0, &msg); }

    // Returns a non-OK status if query execution should stop (e.g., the query was cancelled
    // or a mem limit was exceeded). Exec nodes should check this periodically so execution
    // doesn't continue if the query terminates abnormally.
    Status check_query_state(const std::string& msg);

    Status check_mem_limit(const std::string& msg);

    std::vector<std::string>& output_files() { return _output_files; }

    const std::vector<std::string>& export_output_files() const { return _export_output_files; }

    void add_export_output_file(const std::string& file) { _export_output_files.push_back(file); }

    void set_load_job_id(int64_t job_id) { _load_job_id = job_id; }

    int64_t load_job_id() const { return _load_job_id; }

    const std::string& get_error_log_file_path() const { return _error_log_file_path; }

    // is_summary is true, means we are going to write the summary line
    void append_error_msg_to_file(const std::string& line, const std::string& error_msg, bool is_summary = false);

    bool has_reached_max_error_msg_num(bool is_summary = false);

    int64_t num_bytes_load_from_source() const noexcept { return _num_bytes_load_from_source.load(); }

    int64_t num_rows_load_from_source() const noexcept { return _num_rows_load_total_from_source.load(); }

    int64_t num_bytes_load_from_sink() const noexcept { return _num_bytes_load_from_sink.load(); }

    int64_t num_rows_load_from_sink() const noexcept { return _num_rows_load_from_sink.load(); }

    int64_t num_rows_load_filtered() const noexcept { return _num_rows_load_filtered.load(); }

    int64_t num_rows_load_unselected() const noexcept { return _num_rows_load_unselected.load(); }

    int64_t num_rows_load_sink_success() const noexcept {
        return num_rows_load_from_sink() - num_rows_load_filtered() - num_rows_load_unselected();
    }

    void update_num_bytes_load_from_source(int64_t bytes_load) { _num_bytes_load_from_source.fetch_add(bytes_load); }

    void set_update_num_bytes_load_from_source(int64_t bytes_load) { _num_bytes_load_from_source.store(bytes_load); }

    void update_num_rows_load_from_source(int64_t num_rows) { _num_rows_load_total_from_source.fetch_add(num_rows); }

    void set_num_rows_load_from_source(int64_t num_rows) { _num_rows_load_total_from_source.store(num_rows); }

    void update_num_bytes_load_from_sink(int64_t bytes_load) { _num_bytes_load_from_sink.fetch_add(bytes_load); }

    void set_update_num_bytes_load_from_sink(int64_t bytes_load) { _num_bytes_load_from_sink.store(bytes_load); }

    void update_num_rows_load_from_sink(int64_t num_rows) { _num_rows_load_from_sink.fetch_add(num_rows); }

    void set_num_rows_load_from_sink(int64_t num_rows) { _num_rows_load_from_sink.store(num_rows); }

    void update_num_rows_load_filtered(int64_t num_rows) { _num_rows_load_filtered.fetch_add(num_rows); }

    void update_num_rows_load_unselected(int64_t num_rows) { _num_rows_load_unselected.fetch_add(num_rows); }

    void update_report_load_status(TReportExecStatusParams* load_params) {
        load_params->__set_loaded_rows(num_rows_load_from_sink());
        load_params->__set_sink_load_bytes(num_bytes_load_from_sink());
        load_params->__set_source_load_rows(num_rows_load_from_source());
        load_params->__set_source_load_bytes(num_bytes_load_from_source());
    }

    void set_per_fragment_instance_idx(int idx) { _per_fragment_instance_idx = idx; }

    int per_fragment_instance_idx() const { return _per_fragment_instance_idx; }

    void set_num_per_fragment_instances(int num_instances) { _num_per_fragment_instances = num_instances; }

    int num_per_fragment_instances() const { return _num_per_fragment_instances; }

    int64_t min_reservation() const { return _query_options.min_reservation; }

    int64_t max_reservation() const { return _query_options.max_reservation; }

    bool disable_stream_preaggregations() const { return _query_options.disable_stream_preaggregations; }

    bool enable_spill() const { return _query_options.enable_spilling; }

    const std::vector<TTabletCommitInfo>& tablet_commit_infos() const { return _tablet_commit_infos; }

    std::vector<TTabletCommitInfo>& tablet_commit_infos() { return _tablet_commit_infos; }

    void append_tablet_commit_infos(std::vector<TTabletCommitInfo>& commit_info) {
        std::lock_guard<std::mutex> l(_tablet_infos_lock);
        _tablet_commit_infos.insert(_tablet_commit_infos.end(), std::make_move_iterator(commit_info.begin()),
                                    std::make_move_iterator(commit_info.end()));
    }

    const std::vector<TTabletFailInfo>& tablet_fail_infos() const { return _tablet_fail_infos; }

    std::vector<TTabletFailInfo>& tablet_fail_infos() { return _tablet_fail_infos; }

    void append_tablet_fail_infos(TTabletFailInfo fail_info) {
        std::lock_guard<std::mutex> l(_tablet_infos_lock);
        _tablet_fail_infos.emplace_back(std::move(fail_info));
    }

    // get mem limit for load channel
    // if load mem limit is not set, or is zero, using query mem limit instead.
    int64_t get_load_mem_limit() const;

    const vectorized::GlobalDictMaps& get_query_global_dict_map() const;
    // for query global dict
    vectorized::GlobalDictMaps* mutable_query_global_dict_map();

    const vectorized::GlobalDictMaps& get_load_global_dict_map() const;

    using GlobalDictLists = std::vector<TGlobalDict>;
    Status init_query_global_dict(const GlobalDictLists& global_dict_list);
    Status init_load_global_dict(const GlobalDictLists& global_dict_list);

    void set_func_version(int func_version) { this->_func_version = func_version; }
    int func_version() const { return this->_func_version; }

    void set_enable_pipeline_engine(bool enable_pipeline_engine) { _enable_pipeline_engine = enable_pipeline_engine; }
    bool enable_pipeline_engine() const { return _enable_pipeline_engine; }

    bool enable_query_statistic() const;
    std::shared_ptr<QueryStatistics> intermediate_query_statistic();
    std::shared_ptr<QueryStatisticsRecvr> query_recv();

    int64_t get_rpc_http_min_size() {
        return _query_options.__isset.rpc_http_min_size ? _query_options.rpc_http_min_size : kRpcHttpMinSize;
    }

private:
    // Set per-query state.
    void _init(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
               const TQueryGlobals& query_globals, ExecEnv* exec_env);

    Status create_error_log_file();

    Status _build_global_dict(const GlobalDictLists& global_dict_list, vectorized::GlobalDictMaps* result);

    // put runtime state before _obj_pool, so that it will be deconstructed after
    // _obj_pool. Because some object in _obj_pool will use profile when deconstructing.
    std::shared_ptr<RuntimeProfile> _profile;

    // An aggregation function may have multiple versions of implementation, func_version determines the chosen version.
    int _func_version = 0;

    DescriptorTbl* _desc_tbl = nullptr;

    // Lock protecting _error_log and _unreported_error_idx
    std::mutex _error_log_lock;

    // Logs error messages.
    std::vector<std::string> _error_log;

    // _error_log[_unreported_error_idx+] has been not reported to the coordinator.
    int _unreported_error_idx;

    // Username of user that is executing the query to which this RuntimeState belongs.
    std::string _user;

    //Query-global timestamp_ms
    int64_t _timestamp_ms = 0;
    std::string _timezone;
    cctz::time_zone _timezone_obj;

    std::string _last_query_id;
    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    TQueryOptions _query_options;
    ExecEnv* _exec_env = nullptr;

    // MemTracker that is shared by all fragment instances running on this host.
    // The query mem tracker must be released after the _instance_mem_tracker.
    std::shared_ptr<MemTracker> _query_mem_tracker;

    // Memory usage of this fragment instance
    std::shared_ptr<MemTracker> _instance_mem_tracker;

    std::shared_ptr<ObjectPool> _obj_pool;

    // if true, execution should stop with a CANCELLED status
    std::atomic<bool> _is_cancelled{false};

    int _per_fragment_instance_idx;
    int _num_per_fragment_instances = 0;

    // used as send id
    int _be_number = 0;

    // Non-OK if an error has occurred and query execution should abort. Used only for
    // asynchronously reporting such errors (e.g., when a UDF reports an error), so this
    // will not necessarily be set in all error cases.
    std::mutex _process_status_lock;
    Status _process_status;
    std::unique_ptr<MemPool> _instance_mem_pool;

    // This is the node id of the root node for this plan fragment. This is used as the
    // hash seed and has two useful properties:
    // 1) It is the same for all exec nodes in a fragment, so the resulting hash values
    // can be shared (i.e. for _slot_bitmap_filters).
    // 2) It is different between different fragments, so we do not run into hash
    // collisions after data partitioning (across fragments). See IMPALA-219 for more
    // details.
    PlanNodeId _root_node_id = -1;

    // put here to collect files??
    std::vector<std::string> _output_files;

    // here we separate number counter of rows/bytes load from source node and sink node,
    // num_rows/bytes_load_from_source is mainly used for reporting the progress of broker load and insert into
    // num_rows/bytes_load_from_sink is mainly used for recording how many rows are loaded into tablet sink
    std::atomic<int64_t> _num_rows_load_total_from_source{
            0};                                          // total rows load from source node (file scan node, olap scan
                                                         // node)
    std::atomic<int64_t> _num_bytes_load_from_source{0}; // total bytes load from source node (file scan node, olap scan
                                                         // node)

    std::atomic<int64_t> _num_rows_load_from_sink{0};  // total rows load from sink node (tablet sink node)
    std::atomic<int64_t> _num_bytes_load_from_sink{0}; // total bytes load from sink node (tablet sink node)

    std::atomic<int64_t> _num_rows_load_filtered{0};   // unqualified rows
    std::atomic<int64_t> _num_rows_load_unselected{0}; // rows filtered by predicates

    std::atomic<int64_t> _num_print_error_rows{0};

    std::vector<std::string> _export_output_files;

    int64_t _load_job_id = 0;

    std::string _error_log_file_path;
    std::ofstream* _error_log_file = nullptr; // error file path, absolute path
    std::mutex _tablet_infos_lock;
    std::vector<TTabletCommitInfo> _tablet_commit_infos;
    std::vector<TTabletFailInfo> _tablet_fail_infos;

    // prohibit copies
    RuntimeState(const RuntimeState&) = delete;

    RuntimeFilterPort* _runtime_filter_port = nullptr;

    vectorized::GlobalDictMaps _query_global_dicts;
    vectorized::GlobalDictMaps _load_global_dicts;

    pipeline::QueryContext* _query_ctx = nullptr;

    bool _enable_pipeline_engine = false;
};

#define LIMIT_EXCEEDED(tracker, state, msg)                                                                         \
    do {                                                                                                            \
        stringstream str;                                                                                           \
        str << "Memory of " << tracker->label() << " exceed limit. " << msg << " ";                                 \
        str << "Backend: " << BackendOptions::get_localhost() << ", ";                                              \
        if (state != nullptr) {                                                                                     \
            str << "fragment: " << print_id(state->fragment_instance_id()) << " ";                                  \
        }                                                                                                           \
        str << "Used: " << tracker->consumption() << ", Limit: " << tracker->limit() << ". ";                       \
        switch (tracker->type()) {                                                                                  \
        case MemTracker::NO_SET:                                                                                    \
            break;                                                                                                  \
        case MemTracker::QUERY:                                                                                     \
            str << "Mem usage has exceed the limit of single query, You can change the limit by "                   \
                   "set session variable exec_mem_limit or query_mem_limit.";                                       \
            break;                                                                                                  \
        case MemTracker::PROCESS:                                                                                   \
            str << "Mem usage has exceed the limit of BE";                                                          \
            break;                                                                                                  \
        case MemTracker::QUERY_POOL:                                                                                \
            str << "Mem usage has exceed the limit of query pool";                                                  \
            break;                                                                                                  \
        case MemTracker::LOAD:                                                                                      \
            str << "Mem usage has exceed the limit of load";                                                        \
            break;                                                                                                  \
        case MemTracker::SCHEMA_CHANGE_TASK:                                                                        \
            str << "You can change the limit by modify BE config [memory_limitation_per_thread_for_schema_change]"; \
            break;                                                                                                  \
        case MemTracker::RESOURCE_GROUP:                                                                            \
            /* TODO: make default_wg configuable. */                                                                \
            if (tracker->label() == "default_wg") {                                                                 \
                str << "Mem usage has exceed the limit of query pool";                                              \
            } else {                                                                                                \
                str << "Mem usage has exceed the limit of the resource group [" << tracker->label() << "]. "        \
                    << "You can change the limit by modifying [mem_limit] of this group";                           \
            }                                                                                                       \
            break;                                                                                                  \
        case MemTracker::RESOURCE_GROUP_BIG_QUERY:                                                                  \
            str << "Mem usage has exceed the big query limit of the resource group [" << tracker->label() << "]. "  \
                << "You can change the limit by modifying [big_query_mem_limit] of this group";                     \
            break;                                                                                                  \
        default:                                                                                                    \
            break;                                                                                                  \
        }                                                                                                           \
        return Status::MemoryLimitExceeded(str.str());                                                              \
    } while (false)

#define RETURN_IF_LIMIT_EXCEEDED(state, msg)                                                \
    do {                                                                                    \
        MemTracker* tracker = state->instance_mem_tracker()->find_limit_exceeded_tracker(); \
        if (tracker != nullptr) {                                                           \
            LIMIT_EXCEEDED(tracker, state, msg);                                            \
        }                                                                                   \
    } while (false)

#define RETURN_IF_CANCELLED(state)                                                       \
    do {                                                                                 \
        if (UNLIKELY((state)->is_cancelled()))                                           \
            return Status::Cancelled("Cancelled because of runtime state is cancelled"); \
    } while (false)

} // namespace starrocks
