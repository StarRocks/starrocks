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

#include <atomic>
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
#include "column/chunk.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "runtime/heartbeat_flags.h"
#include "storage/cluster_id_mgr.h"
#include "storage/kv_store.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/tablet.h"

namespace bthread {
class Executor;
}

namespace starrocks {

class DataDir;
class EngineTask;
class MemTableFlushExecutor;
class Tablet;
class ReplicationTxnManager;
class UpdateManager;
class CompactionManager;
class PublishVersionManager;
class DictionaryCacheManager;
class SegmentFlushExecutor;
class SegmentReplicateExecutor;

struct DeltaColumnGroupKey {
    int64_t tablet_id;
    RowsetId rowsetid;
    uint32_t segment_id;

    DeltaColumnGroupKey() = default;
    DeltaColumnGroupKey(int64_t tid, RowsetId rid, uint32_t sid) : tablet_id(tid), rowsetid(rid), segment_id(sid) {}
    ~DeltaColumnGroupKey() = default;

    bool operator==(const DeltaColumnGroupKey& rhs) const {
        return tablet_id == rhs.tablet_id && segment_id == rhs.segment_id && rowsetid == rhs.rowsetid;
    }

    bool operator<(const DeltaColumnGroupKey& rhs) const {
        if (tablet_id < rhs.tablet_id) {
            return true;
        } else if (tablet_id > rhs.tablet_id) {
            return false;
        } else if (rowsetid < rhs.rowsetid) {
            return true;
        } else if (rowsetid != rhs.rowsetid) {
            return false;
        } else {
            return segment_id < rhs.segment_id;
        }
    }
};

struct AutoIncrementMeta {
    int64_t min;
    int64_t max;
    std::mutex mutex;

    AutoIncrementMeta() {
        min = 0;
        max = 0;
    }
};

// StorageEngine singleton to manage all Table pointers.
// Providing add/drop/get operations.
// StorageEngine instance doesn't own the Table resources, just hold the pointer,
// allocation/deallocation must be done outside.
class StorageEngine {
public:
    StorageEngine(const EngineOptions& options);
    virtual ~StorageEngine();

    StorageEngine(const StorageEngine&) = delete;
    const StorageEngine& operator=(const StorageEngine&) = delete;

    static Status open(const EngineOptions& options, StorageEngine** engine_ptr);

    static StorageEngine* instance() { return _s_instance; }

    Status create_tablet(const TCreateTabletReq& request);

    void clear_transaction_task(const TTransactionId transaction_id);
    void clear_transaction_task(const TTransactionId transaction_id, const std::vector<TPartitionId>& partition_ids);

    void load_data_dirs(const std::vector<DataDir*>& stores);

    template <bool include_unused = false>
    std::vector<DataDir*> get_stores();

    size_t get_store_num() { return _store_map.size(); }

    void get_all_data_dir_info(std::vector<DataDirInfo>* data_dir_infos, bool need_update);

    std::vector<string> get_store_paths();
    // Get root path vector for creating tablet. The returned vector is sorted by the disk usage in asc order,
    // then the front portion of the vector excluding paths which have high disk usage is shuffled to avoid
    // the newly created tablet is distributed on only on specific path.
    std::vector<DataDir*> get_stores_for_create_tablet(TStorageMedium::type storage_medium);
    DataDir* get_store(const std::string& path);
    DataDir* get_store(int64_t path_hash);

    DataDir* get_persistent_index_store(int64_t tablet_id);

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

    // call this to wait for a report notification or until timeout.
    // returns:
    // - true: wake up with notification recieved
    // - false: wait until timeout without notification
    bool wait_for_report_notify(int64_t timeout_sec, bool from_report_tablet_thread) {
        bool* watch_var = from_report_tablet_thread ? &_need_report_tablet : &_need_report_disk_stat;
        auto wait_timeout_sec = std::chrono::seconds(timeout_sec);
        std::unique_lock<std::mutex> l(_report_mtx);
        auto ret = _report_cv.wait_for(l, wait_timeout_sec, [&] { return *watch_var; });
        if (ret) {
            // if is waken up with return value `true`, the condition must be satisfied.
            DCHECK(*watch_var);
            *watch_var = false;
        }
        return ret;
    }

    Status execute_task(EngineTask* task);

    TabletManager* tablet_manager() { return _tablet_manager.get(); }

    TxnManager* txn_manager() { return _txn_manager.get(); }

    ReplicationTxnManager* replication_txn_manager() { return _replication_txn_manager.get(); }

    CompactionManager* compaction_manager() { return _compaction_manager.get(); }

    PublishVersionManager* publish_version_manager() { return _publish_version_manager.get(); }

    DictionaryCacheManager* dictionary_cache_manager() { return _dictionary_cache_manager.get(); }

    bthread::Executor* async_delta_writer_executor() { return _async_delta_writer_executor.get(); }

    MemTableFlushExecutor* memtable_flush_executor() { return _memtable_flush_executor.get(); }

    SegmentReplicateExecutor* segment_replicate_executor() { return _segment_replicate_executor.get(); }

    SegmentFlushExecutor* segment_flush_executor() { return _segment_flush_executor.get(); }

    UpdateManager* update_manager() { return _update_manager.get(); }

    bool check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id);

    RowsetId next_rowset_id() { return _rowset_id_generator->next_id(); };

    bool rowset_id_in_use(const RowsetId& rowset_id) { return _rowset_id_generator->id_in_use(rowset_id); }

    void release_rowset_id(const RowsetId& rowset_id) { return _rowset_id_generator->release_id(rowset_id); }

    // start all backgroud threads. This should be call after env is ready.
    virtual Status start_bg_threads();

    void stop();

    bool bg_worker_stopped() { return _bg_worker_stopped.load(std::memory_order_consume); }

    void compaction_check();

    // submit repair compaction tasks
    void submit_repair_compaction_tasks(const std::vector<std::pair<int64_t, std::vector<uint32_t>>>& tasks);
    std::vector<std::pair<int64_t, std::vector<std::pair<uint32_t, std::string>>>>
    get_executed_repair_compaction_tasks();

    void submit_manual_compaction_task(int64_t tablet_id, int64_t rowset_size_threshold);
    std::string get_manual_compaction_status();

    void do_manual_compact(bool force_compact);

    void increase_update_compaction_thread(const int num_threads_per_disk);

    Status get_next_increment_id_interval(int64_t tableid, size_t num_row, std::vector<int64_t>& ids);

    void remove_increment_map_by_table_id(int64_t table_id);

    bool get_need_write_cluster_id() { return _need_write_cluster_id; }

    size_t delta_column_group_list_memory_usage(const DeltaColumnGroupList& dcgs);

    void search_delta_column_groups_by_version(const DeltaColumnGroupList& all_dcgs, int64_t version,
                                               DeltaColumnGroupList* dcgs);

    Status get_delta_column_group(KVStore* meta, int64_t tablet_id, RowsetId rowsetid, uint32_t segment_id,
                                  int64_t version, DeltaColumnGroupList* dcgs);

    void clear_cached_delta_column_group(const std::vector<DeltaColumnGroupKey>& dcg_keys);

    void clear_rowset_delta_column_group_cache(const Rowset& rowset);

    void disable_disks(const std::vector<string>& disabled_disks);

    void decommission_disks(const std::vector<string>& decommissioned_disks);

    void wake_finish_publish_vesion_thread() {
        std::unique_lock<std::mutex> wl(_finish_publish_version_mutex);
        _finish_publish_version_cv.notify_one();
    }

    bool is_as_cn() { return !_options.need_write_cluster_id; }

protected:
    static StorageEngine* _s_instance;

    static StorageEngine* _p_instance;

    int32_t _effective_cluster_id;

private:
    // Instance should be inited from `static open()`
    // MUST NOT be called in other circumstances.
    Status _open(const EngineOptions& options);

    Status _init_store_map();

    void _update_storage_medium_type_count();

    // Some check methods
    Status _check_file_descriptor_number();
    Status _check_all_root_path_cluster_id();
    Status _judge_and_update_effective_cluster_id(int32_t cluster_id);

    bool _delete_tablets_on_unused_root_path();

    void _clean_unused_txns();

    void _clean_unused_rowset_metas();

    Status _do_sweep(const std::string& scan_root, const time_t& local_tm_now, const int32_t expire);

    Status _get_remote_next_increment_id_interval(const TAllocateAutoIncrementIdParam& request,
                                                  TAllocateAutoIncrementIdResult* result);

    // All these xxx_callback() functions are for Background threads
    // update cache expire thread
    void* _update_cache_expire_thread_callback(void* arg);
    // update cache evict thread
    void* _update_cache_evict_thread_callback(void* arg);

    // unused rowset monitor thread
    void* _unused_rowset_monitor_thread_callback(void* arg);

    void* _base_compaction_thread_callback(void* arg, DataDir* data_dir,
                                           std::pair<int32_t, int32_t> tablet_shards_range);
    void* _cumulative_compaction_thread_callback(void* arg, DataDir* data_dir,
                                                 const std::pair<int32_t, int32_t>& tablet_shards_range);
    // update compaction function
    void* _update_compaction_thread_callback(void* arg, DataDir* data_dir);
    // repair compaction function
    void* _repair_compaction_thread_callback(void* arg);
    // manual compaction function
    void* _manual_compaction_thread_callback(void* arg);
    // pk index major compaction function
    void* _pk_index_major_compaction_thread_callback(void* arg);

    void* _pk_dump_thread_callback(void* arg);

#ifdef USE_STAROS
    // local pk index of SHARED_DATA gc/evict function
    void* _local_pk_index_shared_data_gc_evict_thread_callback(void* arg);
#endif

    bool _check_and_run_manual_compaction_task();

    // garbage sweep thread process function. clear snapshot and trash folder
    void* _garbage_sweeper_thread_callback(void* arg);

    // delete tablet with io error process function
    void* _disk_stat_monitor_thread_callback(void* arg);

    // finish publish version process function
    void* _finish_publish_version_thread_callback(void* arg);

    // clean file descriptors cache
    void* _fd_cache_clean_callback(void* arg);

    // path gc process function
    void* _path_gc_thread_callback(void* arg);

    void* _path_scan_thread_callback(void* arg);

    void* _clear_expired_replication_snapshots_callback(void* arg);

    void* _tablet_checkpoint_callback(void* arg);

    void* _adjust_pagecache_callback(void* arg);

    void _start_clean_fd_cache();
    Status _perform_cumulative_compaction(DataDir* data_dir, std::pair<int32_t, int32_t> tablet_shards_range);
    Status _perform_base_compaction(DataDir* data_dir, std::pair<int32_t, int32_t> tablet_shards_range);
    Status _perform_update_compaction(DataDir* data_dir);
    Status _start_trash_sweep(double* usage);
    void _start_disk_stat_monitor();

    size_t _compaction_check_one_round();

private:
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
    std::thread _update_cache_evict_thread;
    std::thread _unused_rowset_monitor_thread;
    // thread to monitor snapshot expiry
    std::thread _garbage_sweeper_thread;
    // thread to monitor disk stat
    std::thread _disk_stat_monitor_thread;
    // thread to check finish publish version task
    std::thread _finish_publish_version_thread;
    // threads to run base compaction
    std::vector<std::thread> _base_compaction_threads;
    // threads to check cumulative
    std::vector<std::thread> _cumulative_compaction_threads;
    // threads to run update compaction
    std::vector<std::thread> _update_compaction_threads;
    // thread to run repair compactions
    std::thread _repair_compaction_thread;
    std::mutex _repair_compaction_tasks_lock;
    std::vector<std::pair<int64_t, std::vector<uint32_t>>> _repair_compaction_tasks;
    std::vector<std::pair<int64_t, std::vector<std::pair<uint32_t, std::string>>>> _executed_repair_compaction_tasks;
    std::vector<std::thread> _manual_compaction_threads;
    // thread to run pk index major compaction
    std::thread _pk_index_major_compaction_thread;
    // thread to generate pk dump
    std::thread _pk_dump_thread;
    // thread to gc/evict local pk index in sharded_data
    std::thread _local_pk_index_shared_data_gc_evict_thread;

    // threads to clean all file descriptor not actively in use
    std::thread _fd_cache_clean_thread;
    std::thread _adjust_cache_thread;
    std::vector<std::thread> _path_gc_threads;
    // threads to scan disk paths
    std::vector<std::thread> _path_scan_threads;
    // threads to run tablet checkpoint
    std::vector<std::thread> _tablet_checkpoint_threads;

    std::thread _clear_expired_replcation_snapshots_thread;

    std::thread _compaction_checker_thread;
    std::mutex _checker_mutex;
    std::condition_variable _checker_cv;

    std::mutex _trash_sweeper_mutex;
    std::condition_variable _trash_sweeper_cv;

    std::mutex _finish_publish_version_mutex;
    std::condition_variable _finish_publish_version_cv;

    // For tablet and disk-stat report
    std::mutex _report_mtx;
    std::condition_variable _report_cv;
    bool _need_report_tablet = false;
    bool _need_report_disk_stat = false;

    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<TxnManager> _txn_manager;

    std::unique_ptr<ReplicationTxnManager> _replication_txn_manager;

    std::unique_ptr<RowsetIdGenerator> _rowset_id_generator;

    std::unique_ptr<bthread::Executor> _async_delta_writer_executor;

    std::unique_ptr<MemTableFlushExecutor> _memtable_flush_executor;

    std::unique_ptr<SegmentReplicateExecutor> _segment_replicate_executor;

    std::unique_ptr<SegmentFlushExecutor> _segment_flush_executor;

    std::unique_ptr<UpdateManager> _update_manager;

    std::unique_ptr<CompactionManager> _compaction_manager;

    std::unique_ptr<PublishVersionManager> _publish_version_manager;

    std::unique_ptr<DictionaryCacheManager> _dictionary_cache_manager;

    std::unordered_map<int64_t, std::shared_ptr<AutoIncrementMeta>> _auto_increment_meta_map;

    std::mutex _auto_increment_mutex;

    bool _need_write_cluster_id = true;

    // Delta Column Group cache, dcg is short for `Delta Column Group`
    // This cache just used for non-Primary Key table
    std::mutex _delta_column_group_cache_lock;
    std::map<DeltaColumnGroupKey, DeltaColumnGroupList> _delta_column_group_cache;
    std::unique_ptr<MemTracker> _delta_column_group_cache_mem_tracker;
};

/// Load min_garbage_sweep_interval and max_garbage_sweep_interval from config,
/// and calculate a proper sweep interval according to the disk usage and min/max interval.
///
/// *maybe_interval_updated* can be used to check and update min/max interval from config.
/// All the methods are not thread-safe.
class GarbageSweepIntervalCalculator {
public:
    GarbageSweepIntervalCalculator();

    DISALLOW_COPY(GarbageSweepIntervalCalculator);
    DISALLOW_MOVE(GarbageSweepIntervalCalculator);

    double& mutable_disk_usage() { return _disk_usage; }

    // Return true and update min and max interval, if the value from config is changed.
    bool maybe_interval_updated();
    // Calculate the interval according to the disk usage and min/max interval.
    int32_t curr_interval() const;

private:
    void _normalize_min_max();

    // The value read from config.
    int32_t _original_min_interval;
    int32_t _original_max_interval;
    // The value after normalized.
    int32_t _min_interval;
    int32_t _max_interval;

    // Value is in [0, 1].
    double _disk_usage = 1.0;
};

} // namespace starrocks
