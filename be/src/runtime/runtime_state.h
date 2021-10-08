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

#ifndef STARROCKS_BE_SRC_QUERY_RUNTIME_RUNTIME_STATE_H
#define STARROCKS_BE_SRC_QUERY_RUNTIME_RUNTIME_STATE_H

#include <atomic>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "cctz/time_zone.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "gen_cpp/InternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"           // for TUniqueId
#include "runtime/mem_pool.h"
#include "runtime/thread_resource_mgr.h"
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
class TmpFileMgr;
class BufferedBlockMgr2;
class LoadErrorHub;
class ReservationTracker;
class InitialReservations;
class RowDescriptor;
class RuntimeFilterPort;

// A collection of items that are part of the global state of a
// query and shared across all execution nodes of that query.
class RuntimeState {
public:
    // for ut only
    RuntimeState(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env);

    RuntimeState(const TExecPlanFragmentParams& fragment_params, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env);

    // RuntimeState for executing expr in fe-support.
    explicit RuntimeState(const TQueryGlobals& query_globals);

    // Empty d'tor to avoid issues with std::unique_ptr.
    ~RuntimeState();

    // Set per-query state.
    Status init(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                const TQueryGlobals& query_globals, ExecEnv* exec_env);

    // Set up four-level hierarchy of mem trackers: process, query, fragment instance.
    // The instance tracker is tied to our profile.
    // Specific parts of the fragment (i.e. exec nodes, sinks, data stream senders, etc)
    // will add a fourth level when they are initialized.
    // This function also initializes a user function mem tracker (in the fourth level).
    Status init_mem_trackers(const TUniqueId& query_id);

    // for ut only
    Status init_instance_mem_tracker();

    /// Called from Init() to set up buffer reservations and the file group.
    Status init_buffer_poolstate();

    // Gets/Creates the query wide block mgr.
    Status create_block_mgr();

    const TQueryOptions& query_options() const { return _query_options; }
    ObjectPool* obj_pool() const { return _obj_pool.get(); }

    const DescriptorTbl& desc_tbl() const { return *_desc_tbl; }
    void set_desc_tbl(DescriptorTbl* desc_tbl) { _desc_tbl = desc_tbl; }
    int batch_size() const { return _query_options.batch_size; }
    void set_batch_size(int batch_size) { _query_options.batch_size = batch_size; }
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
    std::vector<MemTracker*>* mem_trackers() { return &_mem_trackers; }
    MemTracker* fragment_mem_tracker() { return _fragment_mem_tracker; }
    MemTracker* instance_mem_tracker() { return _instance_mem_tracker.get(); }
    ThreadResourceMgr::ResourcePool* resource_pool() { return _resource_pool; }
    RuntimeFilterPort* runtime_filter_port() { return _runtime_filter_port; }

    void set_fragment_root_id(PlanNodeId id) {
        DCHECK(_root_node_id == -1) << "Should not set this twice.";
        _root_node_id = id;
    }

    // The seed value to use when hashing tuples.
    // See comment on _root_node_id. We add one to prevent having a hash seed of 0.
    uint32_t fragment_hash_seed() const { return _root_node_id + 1; }

    // Returns runtime state profile
    RuntimeProfile* runtime_profile() { return &_profile; }

    BufferedBlockMgr2* block_mgr2() {
        DCHECK(_block_mgr2.get() != nullptr);
        return _block_mgr2.get();
    }

    Status query_status() {
        std::lock_guard<std::mutex> l(_process_status_lock);
        return _process_status;
    };

    // Sets the fragment memory limit and adds it to _mem_trackers
    void set_fragment_mem_tracker(MemTracker* limit) {
        DCHECK(_fragment_mem_tracker == nullptr);
        _fragment_mem_tracker = limit;
        _mem_trackers.push_back(limit);
    }

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

    bool is_cancelled() const { return _is_cancelled; }
    void set_is_cancelled(bool v) { _is_cancelled = v; }

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

    std::vector<std::string>& output_files() { return _output_files; }

    void set_import_label(const std::string& import_label) { _import_label = import_label; }

    const std::string& import_label() { return _import_label; }

    const std::vector<std::string>& export_output_files() const { return _export_output_files; }

    void add_export_output_file(const std::string& file) { _export_output_files.push_back(file); }

    void set_db_name(const std::string& db_name) { _db_name = db_name; }

    const std::string& db_name() { return _db_name; }

    void set_load_job_id(int64_t job_id) { _load_job_id = job_id; }

    int64_t load_job_id() const { return _load_job_id; }

    // we only initialize object for load jobs
    void set_load_error_hub_info(const TLoadErrorHubInfo& hub_info) {
        TLoadErrorHubInfo* info = new TLoadErrorHubInfo(hub_info);
        _load_error_hub_info.reset(info);
    }

    const std::string& get_error_log_file_path() const { return _error_log_file_path; }

    // is_summary is true, means we are going to write the summary line
    void append_error_msg_to_file(const std::string& line, const std::string& error_msg, bool is_summary = false);

    int64_t num_bytes_load_total() const noexcept { return _num_bytes_load_total.load(); }

    int64_t num_rows_load_total() const noexcept { return _num_rows_load_total.load(); }

    int64_t num_rows_load_filtered() const noexcept { return _num_rows_load_filtered.load(); }

    int64_t num_rows_load_unselected() const noexcept { return _num_rows_load_unselected.load(); }

    int64_t num_rows_load_success() const noexcept {
        return num_rows_load_total() - num_rows_load_filtered() - num_rows_load_unselected();
    }

    void update_num_rows_load_total(int64_t num_rows) { _num_rows_load_total.fetch_add(num_rows); }

    void set_num_rows_load_total(int64_t num_rows) { _num_rows_load_total.store(num_rows); }

    void update_num_bytes_load_total(int64_t bytes_load) { _num_bytes_load_total.fetch_add(bytes_load); }

    void set_update_num_bytes_load_total(int64_t bytes_load) { _num_bytes_load_total.store(bytes_load); }

    void update_num_rows_load_filtered(int64_t num_rows) { _num_rows_load_filtered.fetch_add(num_rows); }

    void update_num_rows_load_unselected(int64_t num_rows) { _num_rows_load_unselected.fetch_add(num_rows); }

    void export_load_error(const std::string& error_msg);

    void set_per_fragment_instance_idx(int idx) { _per_fragment_instance_idx = idx; }

    int per_fragment_instance_idx() const { return _per_fragment_instance_idx; }

    void set_num_per_fragment_instances(int num_instances) { _num_per_fragment_instances = num_instances; }

    int num_per_fragment_instances() const { return _num_per_fragment_instances; }

    ReservationTracker* instance_buffer_reservation() { return _instance_buffer_reservation.get(); }

    int64_t min_reservation() const { return _query_options.min_reservation; }

    int64_t max_reservation() const { return _query_options.max_reservation; }

    bool disable_stream_preaggregations() const { return _query_options.disable_stream_preaggregations; }

    bool enable_spill() const { return _query_options.enable_spilling; }

    // the following getters are only valid after Prepare()
    InitialReservations* initial_reservations() const { return _initial_reservations; }

    ReservationTracker* buffer_reservation() const { return _buffer_reservation; }

    const std::vector<TTabletCommitInfo>& tablet_commit_infos() const { return _tablet_commit_infos; }

    std::vector<TTabletCommitInfo>& tablet_commit_infos() { return _tablet_commit_infos; }

    // get mem limit for load channel
    // if load mem limit is not set, or is zero, using query mem limit instead.
    int64_t get_load_mem_limit() const;

private:
    // Allow TestEnv to set block_mgr manually for testing.
    friend class TestEnv;

    // Use a custom block manager for the query for testing purposes.
    void set_block_mgr2(const std::shared_ptr<BufferedBlockMgr2>& block_mgr) { _block_mgr2 = block_mgr; }

    Status create_error_log_file();

    static const int DEFAULT_BATCH_SIZE = 2048;

    // put runtime state before _obj_pool, so that it will be deconstructed after
    // _obj_pool. Because some of object in _obj_pool will use profile when deconstructing.
    RuntimeProfile _profile;

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

    // Thread resource management object for this fragment's execution.  The runtime
    // state is responsible for returning this pool to the thread mgr.
    ThreadResourceMgr::ResourcePool* _resource_pool = nullptr;

    // all mem limits that apply to this query
    std::vector<MemTracker*> _mem_trackers;

    // Fragment memory limit.  Also contained in _mem_trackers
    MemTracker* _fragment_mem_tracker = nullptr;

    // MemTracker that is shared by all fragment instances running on this host.
    // The query mem tracker must be released after the _instance_mem_tracker.
    std::unique_ptr<MemTracker> _query_mem_tracker;

    // Memory usage of this fragment instance
    std::unique_ptr<MemTracker> _instance_mem_tracker;

    std::shared_ptr<ObjectPool> _obj_pool;

    // if true, execution should stop with a CANCELLED status
    bool _is_cancelled;

    int _per_fragment_instance_idx;
    int _num_per_fragment_instances = 0;

    // used as send id
    int _be_number = 0;

    // Non-OK if an error has occurred and query execution should abort. Used only for
    // asynchronously reporting such errors (e.g., when a UDF reports an error), so this
    // will not necessarily be set in all error cases.
    std::mutex _process_status_lock;
    Status _process_status;
    //std::unique_ptr<MemPool> _udf_pool;

    // BufferedBlockMgr object used to allocate and manage blocks of input data in memory
    // with a fixed memory budget.
    // The block mgr is shared by all fragments for this query.
    std::shared_ptr<BufferedBlockMgr2> _block_mgr2;

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
    std::atomic<int64_t> _num_rows_load_total{0};      // total rows read from source
    std::atomic<int64_t> _num_rows_load_filtered{0};   // unqualified rows
    std::atomic<int64_t> _num_rows_load_unselected{0}; // rows filtered by predicates
    std::atomic<int64_t> _num_print_error_rows{0};

    std::atomic<int64_t> _num_bytes_load_total{0}; // total bytes read from source

    std::vector<std::string> _export_output_files;

    std::string _import_label;
    std::string _db_name;
    int64_t _load_job_id = 0;
    std::unique_ptr<TLoadErrorHubInfo> _load_error_hub_info;

    std::string _error_log_file_path;
    std::ofstream* _error_log_file = nullptr; // error file path, absolute path
    std::unique_ptr<LoadErrorHub> _error_hub;
    std::vector<TTabletCommitInfo> _tablet_commit_infos;

    //TODO chenhao , remove this to QueryState
    /// Pool of buffer reservations used to distribute initial reservations to operators
    /// in the query. Contains a ReservationTracker that is a child of
    /// 'buffer_reservation_'. Owned by 'obj_pool_'. Set in Prepare().
    ReservationTracker* _buffer_reservation = nullptr;

    /// Buffer reservation for this fragment instance - a child of the query buffer
    /// reservation. Non-NULL if 'query_state_' is not NULL.
    std::unique_ptr<ReservationTracker> _instance_buffer_reservation;

    /// Pool of buffer reservations used to distribute initial reservations to operators
    /// in the query. Contains a ReservationTracker that is a child of
    /// 'buffer_reservation_'. Owned by 'obj_pool_'. Set in Prepare().
    InitialReservations* _initial_reservations = nullptr;

    /// Number of fragment instances executing, which may need to claim
    /// from 'initial_reservations_'.
    /// TODO: not needed if we call ReleaseResources() in a timely manner (IMPALA-1575).
    std::atomic<int32_t> _initial_reservation_refcnt{0};

    // prohibit copies
    RuntimeState(const RuntimeState&) = delete;

    RuntimeFilterPort* _runtime_filter_port;
};

#define RETURN_IF_CANCELLED(state)                                                       \
    do {                                                                                 \
        if (UNLIKELY((state)->is_cancelled()))                                           \
            return Status::Cancelled("Cancelled because of runtime state is cancelled"); \
    } while (false)

} // namespace starrocks

#endif // end of STARROCKS_BE_SRC_QUERY_RUNTIME_RUNTIME_STATE_H
