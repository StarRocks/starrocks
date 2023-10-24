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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/exec_env.h

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
#include <memory>
#include <unordered_map>

#include "common/status.h"
#include "exec/query_cache/cache_manager.h"
#include "exec/workgroup/work_group_fwd.h"
#include "runtime/base_load_path_mgr.h"
#include "storage/options.h"
#include "util/threadpool.h"
// NOTE: Be careful about adding includes here. This file is included by many files.
// Unnecssary includes will cause compilatio very slow.
// So please consider use forward declaraion as much as possible.

namespace starrocks {
class AgentServer;
class BfdParser;
class BrokerMgr;
class BrpcStubCache;
class DataStreamMgr;
class EvHttpServer;
class ExternalScanContextMgr;
class FragmentMgr;
class LoadPathMgr;
class LoadStreamMgr;
class StreamContextMgr;
class TransactionMgr;
class MemTracker;
class MetricRegistry;
class StorageEngine;
class ThreadPool;
class PriorityThreadPool;
class ResultBufferMgr;
class ResultQueueMgr;
class LoadChannelMgr;
class WebPageHandler;
class StreamLoadExecutor;
class RoutineLoadTaskExecutor;
class SmallFileMgr;
class RuntimeFilterWorker;
class RuntimeFilterCache;
class ProfileReportWorker;
class QuerySpillManager;
struct RfTracePoint;

class BackendServiceClient;
class FrontendServiceClient;
class TFileBrokerServiceClient;
template <class T>
class ClientCache;
class HeartbeatFlags;

namespace pipeline {
class DriverExecutor;
class QueryContextManager;
class DriverLimiter;
} // namespace pipeline

namespace lake {
class LocationProvider;
class TabletManager;
class UpdateManager;
class ReplicationTxnManager;
} // namespace lake
namespace spill {
class DirManager;
}

class GlobalEnv {
public:
    static GlobalEnv* GetInstance() {
        static GlobalEnv s_global_env;
        return &s_global_env;
    }

    GlobalEnv() = default;
    ~GlobalEnv() { _is_init = false; }

    Status init();
    void stop() {
        _is_init = false;
        _reset_tracker();
    }

    static bool is_init();

    MemTracker* process_mem_tracker() { return _process_mem_tracker.get(); }
    MemTracker* query_pool_mem_tracker() { return _query_pool_mem_tracker.get(); }
    MemTracker* connector_scan_pool_mem_tracker() { return _connector_scan_pool_mem_tracker.get(); }
    MemTracker* load_mem_tracker() { return _load_mem_tracker.get(); }
    MemTracker* metadata_mem_tracker() { return _metadata_mem_tracker.get(); }
    MemTracker* tablet_metadata_mem_tracker() { return _tablet_metadata_mem_tracker.get(); }
    MemTracker* rowset_metadata_mem_tracker() { return _rowset_metadata_mem_tracker.get(); }
    MemTracker* segment_metadata_mem_tracker() { return _segment_metadata_mem_tracker.get(); }
    MemTracker* column_metadata_mem_tracker() { return _column_metadata_mem_tracker.get(); }
    MemTracker* tablet_schema_mem_tracker() { return _tablet_schema_mem_tracker.get(); }
    MemTracker* column_zonemap_index_mem_tracker() { return _column_zonemap_index_mem_tracker.get(); }
    MemTracker* ordinal_index_mem_tracker() { return _ordinal_index_mem_tracker.get(); }
    MemTracker* bitmap_index_mem_tracker() { return _bitmap_index_mem_tracker.get(); }
    MemTracker* bloom_filter_index_mem_tracker() { return _bloom_filter_index_mem_tracker.get(); }
    MemTracker* segment_zonemap_mem_tracker() { return _segment_zonemap_mem_tracker.get(); }
    MemTracker* short_key_index_mem_tracker() { return _short_key_index_mem_tracker.get(); }
    MemTracker* compaction_mem_tracker() { return _compaction_mem_tracker.get(); }
    MemTracker* schema_change_mem_tracker() { return _schema_change_mem_tracker.get(); }
    MemTracker* column_pool_mem_tracker() { return _column_pool_mem_tracker.get(); }
    MemTracker* page_cache_mem_tracker() { return _page_cache_mem_tracker.get(); }
    MemTracker* update_mem_tracker() { return _update_mem_tracker.get(); }
    MemTracker* chunk_allocator_mem_tracker() { return _chunk_allocator_mem_tracker.get(); }
    MemTracker* clone_mem_tracker() { return _clone_mem_tracker.get(); }
    MemTracker* consistency_mem_tracker() { return _consistency_mem_tracker.get(); }
    MemTracker* replication_mem_tracker() { return _replication_mem_tracker.get(); }
    std::vector<std::shared_ptr<MemTracker>>& mem_trackers() { return _mem_trackers; }

    int64_t get_storage_page_cache_size();
    int64_t check_storage_page_cache_size(int64_t storage_cache_limit);
    static int64_t calc_max_query_memory(int64_t process_mem_limit, int64_t percent);

private:
    static bool _is_init;

    Status _init_mem_tracker();
    void _reset_tracker();

    void _init_storage_page_cache();

    template <class... Args>
    std::shared_ptr<MemTracker> regist_tracker(Args&&... args);

    // root process memory tracker
    std::shared_ptr<MemTracker> _process_mem_tracker;

    // Limit the memory used by the query. At present, it can use 90% of the be memory limit
    std::shared_ptr<MemTracker> _query_pool_mem_tracker;
    std::shared_ptr<MemTracker> _connector_scan_pool_mem_tracker;

    // Limit the memory used by load
    std::shared_ptr<MemTracker> _load_mem_tracker;

    // metadata l0
    std::shared_ptr<MemTracker> _metadata_mem_tracker;

    // metadata l1
    std::shared_ptr<MemTracker> _tablet_metadata_mem_tracker;
    std::shared_ptr<MemTracker> _rowset_metadata_mem_tracker;
    std::shared_ptr<MemTracker> _segment_metadata_mem_tracker;
    std::shared_ptr<MemTracker> _column_metadata_mem_tracker;

    // metadata l2
    std::shared_ptr<MemTracker> _tablet_schema_mem_tracker;
    std::shared_ptr<MemTracker> _segment_zonemap_mem_tracker;
    std::shared_ptr<MemTracker> _short_key_index_mem_tracker;
    std::shared_ptr<MemTracker> _column_zonemap_index_mem_tracker;
    std::shared_ptr<MemTracker> _ordinal_index_mem_tracker;
    std::shared_ptr<MemTracker> _bitmap_index_mem_tracker;
    std::shared_ptr<MemTracker> _bloom_filter_index_mem_tracker;

    // The memory used for compaction
    std::shared_ptr<MemTracker> _compaction_mem_tracker;

    // The memory used for schema change
    std::shared_ptr<MemTracker> _schema_change_mem_tracker;

    // The memory used for column pool
    std::shared_ptr<MemTracker> _column_pool_mem_tracker;

    // The memory used for page cache
    std::shared_ptr<MemTracker> _page_cache_mem_tracker;

    // The memory tracker for update manager
    std::shared_ptr<MemTracker> _update_mem_tracker;

    std::shared_ptr<MemTracker> _chunk_allocator_mem_tracker;

    std::shared_ptr<MemTracker> _clone_mem_tracker;

    std::shared_ptr<MemTracker> _consistency_mem_tracker;

    std::shared_ptr<MemTracker> _replication_mem_tracker;

    std::vector<std::shared_ptr<MemTracker>> _mem_trackers;
};

// Execution environment for queries/plan fragments.
// Contains all required global structures, and handles to
// singleton services. Clients must call StartServices exactly
// once to properly initialise service state.
class ExecEnv {
public:
    // Initial exec environment. must call this to init all
    Status init(const std::vector<StorePath>& store_paths, bool as_cn = false);
    void stop();
    void destroy();
    void wait_for_finish();

    /// Returns the first created exec env instance. In a normal starrocks, this is
    /// the only instance. In test setups with multiple ExecEnv's per process,
    /// we return the most recently created instance.
    static ExecEnv* GetInstance() {
        static ExecEnv s_exec_env;
        return &s_exec_env;
    }

    // only used for test
    ExecEnv() = default;

    // Empty destructor because the compiler-generated one requires full
    // declarations for classes in scoped_ptrs.
    ~ExecEnv() = default;

    std::string token() const;
    ExternalScanContextMgr* external_scan_context_mgr() { return _external_scan_context_mgr; }
    MetricRegistry* metrics() const { return _metrics; }
    DataStreamMgr* stream_mgr() { return _stream_mgr; }
    ResultBufferMgr* result_mgr() { return _result_mgr; }
    ResultQueueMgr* result_queue_mgr() { return _result_queue_mgr; }
    ClientCache<BackendServiceClient>* client_cache() { return _backend_client_cache; }
    ClientCache<FrontendServiceClient>* frontend_client_cache() { return _frontend_client_cache; }
    ClientCache<TFileBrokerServiceClient>* broker_client_cache() { return _broker_client_cache; }

    // using template to simplify client cache management
    template <typename T>
    ClientCache<T>* get_client_cache();

    PriorityThreadPool* thread_pool() { return _thread_pool; }
    ThreadPool* streaming_load_thread_pool() { return _streaming_load_thread_pool; }
    workgroup::ScanExecutor* scan_executor() { return _scan_executor; }
    workgroup::ScanExecutor* connector_scan_executor() { return _connector_scan_executor; }

    PriorityThreadPool* udf_call_pool() { return _udf_call_pool; }
    PriorityThreadPool* pipeline_prepare_pool() { return _pipeline_prepare_pool; }
    PriorityThreadPool* pipeline_sink_io_pool() { return _pipeline_sink_io_pool; }
    PriorityThreadPool* query_rpc_pool() { return _query_rpc_pool; }
    ThreadPool* load_rpc_pool() { return _load_rpc_pool.get(); }
    ThreadPool* dictionary_cache_pool() { return _dictionary_cache_pool.get(); }
    FragmentMgr* fragment_mgr() { return _fragment_mgr; }
    starrocks::pipeline::DriverExecutor* wg_driver_executor() { return _wg_driver_executor; }
    BaseLoadPathMgr* load_path_mgr() { return _load_path_mgr; }
    BfdParser* bfd_parser() const { return _bfd_parser; }
    BrokerMgr* broker_mgr() const { return _broker_mgr; }
    BrpcStubCache* brpc_stub_cache() const { return _brpc_stub_cache; }
    LoadChannelMgr* load_channel_mgr() { return _load_channel_mgr; }
    LoadStreamMgr* load_stream_mgr() { return _load_stream_mgr; }
    SmallFileMgr* small_file_mgr() { return _small_file_mgr; }
    StreamContextMgr* stream_context_mgr() { return _stream_context_mgr; }
    TransactionMgr* transaction_mgr() { return _transaction_mgr; }

    const std::vector<StorePath>& store_paths() const { return _store_paths; }

    StreamLoadExecutor* stream_load_executor() { return _stream_load_executor; }
    RoutineLoadTaskExecutor* routine_load_task_executor() { return _routine_load_task_executor; }
    HeartbeatFlags* heartbeat_flags() { return _heartbeat_flags; }

    ThreadPool* automatic_partition_pool() { return _automatic_partition_pool.get(); }

    RuntimeFilterWorker* runtime_filter_worker() { return _runtime_filter_worker; }

    RuntimeFilterCache* runtime_filter_cache() { return _runtime_filter_cache; }

    ProfileReportWorker* profile_report_worker() { return _profile_report_worker; }

    void add_rf_event(const RfTracePoint& pt);

    pipeline::QueryContextManager* query_context_mgr() { return _query_context_mgr; }

    pipeline::DriverLimiter* driver_limiter() { return _driver_limiter; }

    int64_t max_executor_threads() const { return _max_executor_threads; }

    uint32_t calc_pipeline_dop(int32_t pipeline_dop) const;

    uint32_t calc_pipeline_sink_dop(int32_t pipeline_sink_dop) const;

    lake::TabletManager* lake_tablet_manager() const { return _lake_tablet_manager; }

    std::shared_ptr<lake::LocationProvider> lake_location_provider() const { return _lake_location_provider; }

    lake::UpdateManager* lake_update_manager() const { return _lake_update_manager; }

    lake::ReplicationTxnManager* lake_replication_txn_manager() const { return _lake_replication_txn_manager; }

    AgentServer* agent_server() const { return _agent_server; }

    query_cache::CacheManagerRawPtr cache_mgr() const { return _cache_mgr; }

    spill::DirManager* spill_dir_mgr() const { return _spill_dir_mgr.get(); }

    ThreadPool* delete_file_thread_pool();

private:
    void _wait_for_fragments_finish();

    std::vector<StorePath> _store_paths;
    // Leave protected so that subclasses can override
    ExternalScanContextMgr* _external_scan_context_mgr = nullptr;
    MetricRegistry* _metrics = nullptr;
    DataStreamMgr* _stream_mgr = nullptr;
    ResultBufferMgr* _result_mgr = nullptr;
    ResultQueueMgr* _result_queue_mgr = nullptr;
    ClientCache<BackendServiceClient>* _backend_client_cache = nullptr;
    ClientCache<FrontendServiceClient>* _frontend_client_cache = nullptr;
    ClientCache<TFileBrokerServiceClient>* _broker_client_cache = nullptr;

    PriorityThreadPool* _thread_pool = nullptr;
    ThreadPool* _streaming_load_thread_pool = nullptr;

    workgroup::ScanExecutor* _scan_executor = nullptr;
    workgroup::ScanExecutor* _connector_scan_executor = nullptr;

    PriorityThreadPool* _udf_call_pool = nullptr;
    PriorityThreadPool* _pipeline_prepare_pool = nullptr;
    PriorityThreadPool* _pipeline_sink_io_pool = nullptr;
    PriorityThreadPool* _query_rpc_pool = nullptr;
    std::unique_ptr<ThreadPool> _load_rpc_pool;
    std::unique_ptr<ThreadPool> _dictionary_cache_pool;
    FragmentMgr* _fragment_mgr = nullptr;
    pipeline::QueryContextManager* _query_context_mgr = nullptr;
    pipeline::DriverExecutor* _wg_driver_executor = nullptr;
    pipeline::DriverLimiter* _driver_limiter = nullptr;
    int64_t _max_executor_threads = 0; // Max thread number of executor

    BaseLoadPathMgr* _load_path_mgr = nullptr;

    BfdParser* _bfd_parser = nullptr;
    BrokerMgr* _broker_mgr = nullptr;
    LoadChannelMgr* _load_channel_mgr = nullptr;
    LoadStreamMgr* _load_stream_mgr = nullptr;
    BrpcStubCache* _brpc_stub_cache = nullptr;
    StreamContextMgr* _stream_context_mgr = nullptr;
    TransactionMgr* _transaction_mgr = nullptr;

    [[maybe_unused]] StorageEngine* _storage_engine = nullptr;

    StreamLoadExecutor* _stream_load_executor = nullptr;
    RoutineLoadTaskExecutor* _routine_load_task_executor = nullptr;
    SmallFileMgr* _small_file_mgr = nullptr;
    HeartbeatFlags* _heartbeat_flags = nullptr;

    std::unique_ptr<ThreadPool> _automatic_partition_pool;

    RuntimeFilterWorker* _runtime_filter_worker = nullptr;
    RuntimeFilterCache* _runtime_filter_cache = nullptr;

    ProfileReportWorker* _profile_report_worker = nullptr;

    lake::TabletManager* _lake_tablet_manager = nullptr;
    std::shared_ptr<lake::LocationProvider> _lake_location_provider;
    lake::UpdateManager* _lake_update_manager = nullptr;
    lake::ReplicationTxnManager* _lake_replication_txn_manager = nullptr;

    AgentServer* _agent_server = nullptr;
    query_cache::CacheManagerRawPtr _cache_mgr;
    std::shared_ptr<spill::DirManager> _spill_dir_mgr;
};

template <>
inline ClientCache<BackendServiceClient>* ExecEnv::get_client_cache<BackendServiceClient>() {
    return _backend_client_cache;
}
template <>
inline ClientCache<FrontendServiceClient>* ExecEnv::get_client_cache<FrontendServiceClient>() {
    return _frontend_client_cache;
}
template <>
inline ClientCache<TFileBrokerServiceClient>* ExecEnv::get_client_cache<TFileBrokerServiceClient>() {
    return _broker_client_cache;
}

} // namespace starrocks
