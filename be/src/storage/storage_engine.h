// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/storage_engine.h

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

#include <pthread.h>
#include <rapidjson/document.h>

#include <condition_variable>
#include <ctime>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "agent/status.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "runtime/heartbeat_flags.h"
#include "storage/cluster_id_mgr.h"
#include "storage/compaction_manager.h"
#include "storage/kv_store.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/task/engine_task.h"
#include "storage/txn_manager.h"

namespace starrocks {

class AsyncDeltaWriterExecutor;
class DataDir;
class EngineTask;
class MemTableFlushExecutor;
class Tablet;
class UpdateManager;
class CompactionManager;

// StorageEngine singleton to manage all Table pointers.
// Providing add/drop/get operations.
// StorageEngine instance doesn't own the Table resources, just hold the pointer,
// allocation/deallocation must be done outside.
class StorageEngine {
public:
    StorageEngine(const EngineOptions& options);
    virtual ~StorageEngine();

    static Status open(const EngineOptions& options, StorageEngine** engine_ptr);

    static StorageEngine* instance() { return _s_instance; }

    Status create_tablet(const TCreateTabletReq& request);

    void clear_transaction_task(const TTransactionId transaction_id);
    void clear_transaction_task(const TTransactionId transaction_id, const std::vector<TPartitionId>& partition_ids);

    void load_data_dirs(const std::vector<DataDir*>& stores);

    template <bool include_unused = false>
    std::vector<DataDir*> get_stores();

    size_t get_store_num() { return _store_map.size(); }

    Status get_all_data_dir_info(std::vector<DataDirInfo>* data_dir_infos, bool need_update);

    std::vector<string> get_store_paths();
    // get root path for creating tablet. The returned vector of root path should be random,
    // for avoiding that all the tablet would be deployed one disk.
    std::vector<DataDir*> get_stores_for_create_tablet(TStorageMedium::type storage_medium);
    DataDir* get_store(const std::string& path);
    DataDir* get_store(int64_t path_hash);

    uint32_t available_storage_medium_type_count() { return _available_storage_medium_type_count; }

    virtual Status set_cluster_id(int32_t cluster_id);
    int32_t effective_cluster_id() const { return _effective_cluster_id; }

    double delete_unused_rowset();
    void add_unused_rowset(const RowsetSharedPtr& rowset);

    // Obtain shard path for new tablet.
    //
    // @param [in] storage_medium specify medium needed
    // @param [in] path_hash: If path_hash is not -1, get store by path hash.
    //                        Else get store randomly by the specified medium.
    // @param [out] shard_path choose an available shard_path to clone new tablet
    // @param [out] store choose an available root_path to clone new tablet
    // @return error code
    Status obtain_shard_path(TStorageMedium::type storage_medium, int64_t path_hash, std::string* shared_path,
                             DataDir** store);

    // Load new tablet to make it effective.
    //
    // @param [in] root_path specify root path of new tablet
    // @param [in] request specify new tablet info
    // @param [in] restore whether we're restoring a tablet from trash
    // @return Status::OK() if load tablet success
    Status load_header(const std::string& shard_path, const TCloneReq& request, bool restore = false,
                       bool is_primary_key = false);

    // To trigger a disk-stat and tablet report
    void trigger_report() {
        std::lock_guard<std::mutex> l(_report_mtx);
        _need_report_tablet = true;
        _need_report_disk_stat = true;
        _report_cv.notify_all();
    }

    // call this to wait for a report notification until timeout
    void wait_for_report_notify(int64_t timeout_sec, bool from_report_tablet_thread) {
        auto wait_timeout_sec = std::chrono::seconds(timeout_sec);
        std::unique_lock<std::mutex> l(_report_mtx);
        // When wait_for() returns, regardless of the return-result(possibly a timeout
        // error), the report_tablet_thread and report_disk_stat_thread(see TaskWorkerPool)
        // immediately begin the next round of reporting, so there is no need to check
        // the return-value of wait_for().
        if (from_report_tablet_thread) {
            _report_cv.wait_for(l, wait_timeout_sec, [this] { return _need_report_tablet; });
            _need_report_tablet = false;
        } else {
            _report_cv.wait_for(l, wait_timeout_sec, [this] { return _need_report_disk_stat; });
            _need_report_disk_stat = false;
        }
    }

    Status execute_task(EngineTask* task);

    TabletManager* tablet_manager() { return _tablet_manager.get(); }

    TxnManager* txn_manager() { return _txn_manager.get(); }

    CompactionManager* compaction_manager() { return _compaction_manager.get(); }

    AsyncDeltaWriterExecutor* async_delta_writer_executor() { return _async_delta_writer_executor.get(); }

    MemTableFlushExecutor* memtable_flush_executor() { return _memtable_flush_executor.get(); }

    UpdateManager* update_manager() { return _update_manager.get(); }

    bool check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id);

    RowsetId next_rowset_id() { return _rowset_id_generator->next_id(); };

    bool rowset_id_in_use(const RowsetId& rowset_id) { return _rowset_id_generator->id_in_use(rowset_id); }

    void release_rowset_id(const RowsetId& rowset_id) { return _rowset_id_generator->release_id(rowset_id); }

    void set_heartbeat_flags(HeartbeatFlags* heartbeat_flags) { _heartbeat_flags = heartbeat_flags; }

    // start all backgroud threads. This should be call after env is ready.
    virtual Status start_bg_threads();

    void stop();

    bool bg_worker_stopped() { return _bg_worker_stopped.load(std::memory_order_consume); }

    void compaction_check();

protected:
    static StorageEngine* _s_instance;
    int32_t _effective_cluster_id;

private:
    // Instance should be inited from `static open()`
    // MUST NOT be called in other circumstances.
    Status _open();

    Status _init_store_map();

    void _update_storage_medium_type_count();

    // Some check methods
    Status _check_file_descriptor_number();
    Status _check_all_root_path_cluster_id();
    Status _judge_and_update_effective_cluster_id(int32_t cluster_id);

    bool _delete_tablets_on_unused_root_path();

    void _clean_unused_txns();

    void _clean_unused_rowset_metas();

    void _do_manual_compact();

    Status _do_sweep(const std::string& scan_root, const time_t& local_tm_now, const int32_t expire);

    // All these xxx_callback() functions are for Background threads
    // update cache expire thread
    void* _update_cache_expire_thread_callback(void* arg);

    // unused rowset monitor thread
    void* _unused_rowset_monitor_thread_callback(void* arg);

    // base compaction thread process function
    void* _base_compaction_thread_callback(void* arg, DataDir* data_dir);
    // cumulative process function
    void* _cumulative_compaction_thread_callback(void* arg, DataDir* data_dir);
    // update compaction function
    void* _update_compaction_thread_callback(void* arg, DataDir* data_dir);

    // garbage sweep thread process function. clear snapshot and trash folder
    void* _garbage_sweeper_thread_callback(void* arg);

    // delete tablet with io error process function
    void* _disk_stat_monitor_thread_callback(void* arg);

    // clean file descriptors cache
    void* _fd_cache_clean_callback(void* arg);

    // path gc process function
    void* _path_gc_thread_callback(void* arg);

    void* _path_scan_thread_callback(void* arg);

    void* _tablet_checkpoint_callback(void* arg);

    void _start_clean_fd_cache();
    Status _perform_cumulative_compaction(DataDir* data_dir);
    Status _perform_base_compaction(DataDir* data_dir);
    Status _perform_update_compaction(DataDir* data_dir);
    Status _start_trash_sweep(double* usage);
    void _start_disk_stat_monitor();

    size_t _compaction_check_one_round();

private:
    struct CompactionCandidate {
        CompactionCandidate(uint32_t nicumulative_compaction_, int64_t tablet_id_, uint32_t index_)
                : nice(nicumulative_compaction_), tablet_id(tablet_id_), disk_index(index_) {}
        uint32_t nice;
        int64_t tablet_id;
        uint32_t disk_index = -1;
    };

    // In descending order
    struct CompactionCandidateComparator {
        bool operator()(const CompactionCandidate& a, const CompactionCandidate& b) { return a.nice > b.nice; }
    };

    struct CompactionDiskStat {
        CompactionDiskStat(std::string path, uint32_t index, bool used)
                : storage_path(std::move(path)), disk_index(index), task_running(0), task_remaining(0), is_used(used) {}
        const std::string storage_path;
        const uint32_t disk_index;
        uint32_t task_running;
        uint32_t task_remaining;
        bool is_used;
    };

    EngineOptions _options;
    std::mutex _store_lock;
    std::map<std::string, DataDir*> _store_map;
    uint32_t _available_storage_medium_type_count;

    bool _is_all_cluster_id_exist;

    std::mutex _gc_mutex;
    // map<rowset_id(str), RowsetSharedPtr>, if we use RowsetId as the key, we need custom hash func
    std::unordered_map<std::string, RowsetSharedPtr> _unused_rowsets;

    std::atomic<bool> _bg_worker_stopped{false};
    // thread to expire update cache;
    std::thread _update_cache_expire_thread;
    std::thread _unused_rowset_monitor_thread;
    // thread to monitor snapshot expiry
    std::thread _garbage_sweeper_thread;
    // thread to monitor disk stat
    std::thread _disk_stat_monitor_thread;
    // threads to run base compaction
    std::vector<std::thread> _base_compaction_threads;
    // threads to check cumulative
    std::vector<std::thread> _cumulative_compaction_threads;
    // threads to run update compaction
    std::vector<std::thread> _update_compaction_threads;
    // threads to clean all file descriptor not actively in use
    std::thread _fd_cache_clean_thread;
    std::vector<std::thread> _path_gc_threads;
    // threads to scan disk paths
    std::vector<std::thread> _path_scan_threads;
    // threads to run tablet checkpoint
    std::vector<std::thread> _tablet_checkpoint_threads;

    std::thread _compaction_scheduler;

    std::thread _compaction_checker_thread;
    std::mutex _checker_mutex;
    std::condition_variable _checker_cv;

    std::mutex _trash_sweeper_mutex;
    std::condition_variable _trash_sweeper_cv;

    // For tablet and disk-stat report
    std::mutex _report_mtx;
    std::condition_variable _report_cv;
    bool _need_report_tablet = false;
    bool _need_report_disk_stat = false;

    std::mutex _engine_task_mutex;

    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<TxnManager> _txn_manager;

    std::unique_ptr<RowsetIdGenerator> _rowset_id_generator;

    std::unique_ptr<AsyncDeltaWriterExecutor> _async_delta_writer_executor;

    std::unique_ptr<MemTableFlushExecutor> _memtable_flush_executor;

    std::unique_ptr<UpdateManager> _update_manager;

    std::unique_ptr<CompactionManager> _compaction_manager;

    HeartbeatFlags* _heartbeat_flags = nullptr;

    StorageEngine(const StorageEngine&) = delete;
    const StorageEngine& operator=(const StorageEngine&) = delete;
};

// DummyStorageEngine is used for ComputeNode, it only stores cluster id.
class DummyStorageEngine : public StorageEngine {
    std::string _conf_path;
    std::unique_ptr<ClusterIdMgr> cluster_id_mgr;

public:
    DummyStorageEngine(const EngineOptions& options);
    static Status open(const EngineOptions& options, StorageEngine** engine_ptr);
    Status set_cluster_id(int32_t cluster_id) override;
    Status start_bg_threads() override { return Status::OK(); };
};

} // namespace starrocks
