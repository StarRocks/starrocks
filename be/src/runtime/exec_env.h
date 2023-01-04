// This file is made available under Elastic License 2.0.
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

#include "common/status.h"
#include "exec/workgroup/work_group_fwd.h"
#include "storage/options.h"
// NOTE: Be careful about adding includes here. This file is included by many files.
// Unnecssary includes will cause compilatio very slow.
// So please consider use forward declaraion as much as possible.

namespace starrocks {
class AgentServer;
class BfdParser;
class BrokerMgr;
class BrpcStubCache;
class DataStreamMgr;
class DiskIoMgr;
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
class PluginMgr;
class RuntimeFilterWorker;
class RuntimeFilterCache;
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
} // namespace lake

// Execution environment for queries/plan fragments.
// Contains all required global structures, and handles to
// singleton services. Clients must call StartServices exactly
// once to properly initialise service state.
class ExecEnv {
public:
    // Initial exec environment. must call this to init all
    static Status init(ExecEnv* env, const std::vector<StorePath>& store_paths);
    static void destroy(ExecEnv* exec_env);

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
    ClientCache<T>* get_client_cache() {
        return nullptr;
    }

    MemTracker* process_mem_tracker() { return _mem_tracker; }
    MemTracker* query_pool_mem_tracker() { return _query_pool_mem_tracker; }
    MemTracker* load_mem_tracker() { return _load_mem_tracker; }
    MemTracker* metadata_mem_tracker() { return _metadata_mem_tracker; }
    MemTracker* tablet_metadata_mem_tracker() { return _tablet_metadata_mem_tracker; }
    MemTracker* rowset_metadata_mem_tracker() { return _rowset_metadata_mem_tracker; }
    MemTracker* segment_metadata_mem_tracker() { return _segment_metadata_mem_tracker; }
    MemTracker* column_metadata_mem_tracker() { return _column_metadata_mem_tracker; }
    MemTracker* tablet_schema_mem_tracker() { return _tablet_schema_mem_tracker; }
    MemTracker* column_zonemap_index_mem_tracker() { return _column_zonemap_index_mem_tracker; }
    MemTracker* ordinal_index_mem_tracker() { return _ordinal_index_mem_tracker; }
    MemTracker* bitmap_index_mem_tracker() { return _bitmap_index_mem_tracker; }
    MemTracker* bloom_filter_index_mem_tracker() { return _bloom_filter_index_mem_tracker; }
    MemTracker* segment_zonemap_mem_tracker() { return _segment_zonemap_mem_tracker; }
    MemTracker* short_key_index_mem_tracker() { return _short_key_index_mem_tracker; }
    MemTracker* compaction_mem_tracker() { return _compaction_mem_tracker; }
    MemTracker* schema_change_mem_tracker() { return _schema_change_mem_tracker; }
    MemTracker* column_pool_mem_tracker() { return _column_pool_mem_tracker; }
    MemTracker* page_cache_mem_tracker() { return _page_cache_mem_tracker; }
    MemTracker* update_mem_tracker() { return _update_mem_tracker; }
    MemTracker* chunk_allocator_mem_tracker() { return _chunk_allocator_mem_tracker; }
    MemTracker* clone_mem_tracker() { return _clone_mem_tracker; }
    MemTracker* consistency_mem_tracker() { return _consistency_mem_tracker; }

    PriorityThreadPool* thread_pool() { return _thread_pool; }
    workgroup::ScanExecutor* scan_executor_without_workgroup() { return _scan_executor_without_workgroup; }
    workgroup::ScanExecutor* scan_executor_with_workgroup() { return _scan_executor_with_workgroup; }
    workgroup::ScanExecutor* connector_scan_executor_without_workgroup() {
        return _connector_scan_executor_without_workgroup;
    }
    workgroup::ScanExecutor* connector_scan_executor_with_workgroup() {
        return _connector_scan_executor_with_workgroup;
    }

    PriorityThreadPool* udf_call_pool() { return _udf_call_pool; }
    PriorityThreadPool* pipeline_prepare_pool() { return _pipeline_prepare_pool; }
    FragmentMgr* fragment_mgr() { return _fragment_mgr; }
    starrocks::pipeline::DriverExecutor* driver_executor() { return _driver_executor; }
    starrocks::pipeline::DriverExecutor* wg_driver_executor() { return _wg_driver_executor; }
    LoadPathMgr* load_path_mgr() { return _load_path_mgr; }
    BfdParser* bfd_parser() const { return _bfd_parser; }
    BrokerMgr* broker_mgr() const { return _broker_mgr; }
    BrpcStubCache* brpc_stub_cache() const { return _brpc_stub_cache; }
    LoadChannelMgr* load_channel_mgr() { return _load_channel_mgr; }
    LoadStreamMgr* load_stream_mgr() { return _load_stream_mgr; }
    SmallFileMgr* small_file_mgr() { return _small_file_mgr; }
    StreamContextMgr* stream_context_mgr() { return _stream_context_mgr; }
    TransactionMgr* transaction_mgr() { return _transaction_mgr; }

    const std::vector<StorePath>& store_paths() const { return _store_paths; }
    void set_store_paths(const std::vector<StorePath>& paths) { _store_paths = paths; }
    StorageEngine* storage_engine() { return _storage_engine; }
    void set_storage_engine(StorageEngine* storage_engine);

    StreamLoadExecutor* stream_load_executor() { return _stream_load_executor; }
    RoutineLoadTaskExecutor* routine_load_task_executor() { return _routine_load_task_executor; }
    HeartbeatFlags* heartbeat_flags() { return _heartbeat_flags; }

    RuntimeFilterWorker* runtime_filter_worker() { return _runtime_filter_worker; }
    Status init_mem_tracker();

    RuntimeFilterCache* runtime_filter_cache() { return _runtime_filter_cache; }
    void add_rf_event(const RfTracePoint& pt);

    pipeline::QueryContextManager* query_context_mgr() { return _query_context_mgr; }

    pipeline::DriverLimiter* driver_limiter() { return _driver_limiter; }

    int64_t max_executor_threads() const { return _max_executor_threads; }

    int32_t calc_pipeline_dop(int32_t pipeline_dop) const;

    lake::TabletManager* lake_tablet_manager() const { return _lake_tablet_manager; }

    lake::LocationProvider* lake_location_provider() const { return _lake_location_provider; }

    AgentServer* agent_server() const { return _agent_server; }

    int64_t get_storage_page_cache_size();
    int64_t check_storage_page_cache_size(int64_t storage_cache_limit);

private:
    Status _init(const std::vector<StorePath>& store_paths);
    void _destroy();

    Status _init_storage_page_cache();

private:
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
    MemTracker* _mem_tracker = nullptr;

    // Limit the memory used by the query. At present, it can use 90% of the be memory limit
    MemTracker* _query_pool_mem_tracker = nullptr;

    // Limit the memory used by load
    MemTracker* _load_mem_tracker = nullptr;

    // metadata l0
    MemTracker* _metadata_mem_tracker = nullptr;

    // metadata l1
    MemTracker* _tablet_metadata_mem_tracker = nullptr;
    MemTracker* _rowset_metadata_mem_tracker = nullptr;
    MemTracker* _segment_metadata_mem_tracker = nullptr;
    MemTracker* _column_metadata_mem_tracker = nullptr;

    // metadata l2
    MemTracker* _tablet_schema_mem_tracker = nullptr;
    MemTracker* _segment_zonemap_mem_tracker = nullptr;
    MemTracker* _short_key_index_mem_tracker = nullptr;
    MemTracker* _column_zonemap_index_mem_tracker = nullptr;
    MemTracker* _ordinal_index_mem_tracker = nullptr;
    MemTracker* _bitmap_index_mem_tracker = nullptr;
    MemTracker* _bloom_filter_index_mem_tracker = nullptr;

    // The memory used for compaction
    MemTracker* _compaction_mem_tracker = nullptr;

    // The memory used for schema change
    MemTracker* _schema_change_mem_tracker = nullptr;

    // The memory used for column pool
    MemTracker* _column_pool_mem_tracker = nullptr;

    // The memory used for page cache
    MemTracker* _page_cache_mem_tracker = nullptr;

    // The memory tracker for update manager
    MemTracker* _update_mem_tracker = nullptr;

    MemTracker* _chunk_allocator_mem_tracker = nullptr;

    MemTracker* _clone_mem_tracker = nullptr;

    MemTracker* _consistency_mem_tracker = nullptr;

    PriorityThreadPool* _thread_pool = nullptr;

    workgroup::ScanExecutor* _scan_executor_without_workgroup = nullptr;
    workgroup::ScanExecutor* _scan_executor_with_workgroup = nullptr;
    workgroup::ScanExecutor* _connector_scan_executor_without_workgroup = nullptr;
    workgroup::ScanExecutor* _connector_scan_executor_with_workgroup = nullptr;

    PriorityThreadPool* _udf_call_pool = nullptr;
    PriorityThreadPool* _pipeline_prepare_pool = nullptr;
    FragmentMgr* _fragment_mgr = nullptr;
    pipeline::QueryContextManager* _query_context_mgr = nullptr;
    pipeline::DriverExecutor* _driver_executor = nullptr;
    pipeline::DriverExecutor* _wg_driver_executor = nullptr;
    pipeline::DriverLimiter* _driver_limiter = nullptr;
    int64_t _max_executor_threads = 0; // Max thread number of executor

    LoadPathMgr* _load_path_mgr = nullptr;

    BfdParser* _bfd_parser = nullptr;
    BrokerMgr* _broker_mgr = nullptr;
    LoadChannelMgr* _load_channel_mgr = nullptr;
    LoadStreamMgr* _load_stream_mgr = nullptr;
    BrpcStubCache* _brpc_stub_cache = nullptr;
    StreamContextMgr* _stream_context_mgr = nullptr;
    TransactionMgr* _transaction_mgr = nullptr;

    StorageEngine* _storage_engine = nullptr;

    StreamLoadExecutor* _stream_load_executor = nullptr;
    RoutineLoadTaskExecutor* _routine_load_task_executor = nullptr;
    SmallFileMgr* _small_file_mgr = nullptr;
    HeartbeatFlags* _heartbeat_flags = nullptr;

    RuntimeFilterWorker* _runtime_filter_worker = nullptr;
    RuntimeFilterCache* _runtime_filter_cache = nullptr;

    lake::TabletManager* _lake_tablet_manager = nullptr;
    lake::LocationProvider* _lake_location_provider = nullptr;

    AgentServer* _agent_server = nullptr;
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
