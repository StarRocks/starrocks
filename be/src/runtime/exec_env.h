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
#include "runtime/runtime_filter_cache.h"
#include "storage/options.h"

namespace starrocks {

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
class MemTracker;
class MetricRegistry;
class StorageEngine;
class ThreadPool;
class PriorityThreadPool;
class ResultBufferMgr;
class ResultQueueMgr;
class TMasterInfo;
class LoadChannelMgr;
class ThreadResourceMgr;
class WebPageHandler;
class StreamLoadExecutor;
class RoutineLoadTaskExecutor;
class SmallFileMgr;
class PluginMgr;
class RuntimeFilterWorker;

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

// Execution environment for queries/plan fragments.
// Contains all required global structures, and handles to
// singleton services. Clients must call StartServices exactly
// once to properly initialise service state.
class ExecEnv {
public:
    // Initial exec enviorment. must call this to init all
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

    const std::string& token() const;
    ExternalScanContextMgr* external_scan_context_mgr() { return _external_scan_context_mgr; }
    MetricRegistry* metrics() const { return _metrics; }
    DataStreamMgr* stream_mgr() { return _stream_mgr; }
    ResultBufferMgr* result_mgr() { return _result_mgr; }
    ResultQueueMgr* result_queue_mgr() { return _result_queue_mgr; }
    ClientCache<BackendServiceClient>* client_cache() { return _backend_client_cache; }
    ClientCache<FrontendServiceClient>* frontend_client_cache() { return _frontend_client_cache; }
    ClientCache<TFileBrokerServiceClient>* broker_client_cache() { return _broker_client_cache; }

    static int64_t calc_max_query_memory(int64_t process_mem_limit, int64_t percent);

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

    ThreadResourceMgr* thread_mgr() { return _thread_mgr; }
    PriorityThreadPool* thread_pool() { return _thread_pool; }
    PriorityThreadPool* pipeline_scan_io_thread_pool() { return _pipeline_scan_io_thread_pool; }
    size_t increment_num_scan_operators(size_t n) { return _num_scan_operators.fetch_add(n); }
    size_t decrement_num_scan_operators(size_t n) { return _num_scan_operators.fetch_sub(n); }
    PriorityThreadPool* etl_thread_pool() { return _etl_thread_pool; }
    PriorityThreadPool* udf_call_pool() { return _udf_call_pool; }
    FragmentMgr* fragment_mgr() { return _fragment_mgr; }
    starrocks::pipeline::DriverExecutor* driver_executor() { return _driver_executor; }
    starrocks::pipeline::DriverExecutor* wg_driver_executor() { return _wg_driver_executor; }
    TMasterInfo* master_info() { return _master_info; }
    LoadPathMgr* load_path_mgr() { return _load_path_mgr; }
    BfdParser* bfd_parser() const { return _bfd_parser; }
    BrokerMgr* broker_mgr() const { return _broker_mgr; }
    BrpcStubCache* brpc_stub_cache() const { return _brpc_stub_cache; }
    LoadChannelMgr* load_channel_mgr() { return _load_channel_mgr; }
    LoadStreamMgr* load_stream_mgr() { return _load_stream_mgr; }
    SmallFileMgr* small_file_mgr() { return _small_file_mgr; }

    starrocks::workgroup::ScanExecutor* scan_executor() { return _scan_executor; }

    const std::vector<StorePath>& store_paths() const { return _store_paths; }
    void set_store_paths(const std::vector<StorePath>& paths) { _store_paths = paths; }
    StorageEngine* storage_engine() { return _storage_engine; }
    void set_storage_engine(StorageEngine* storage_engine);

    StreamLoadExecutor* stream_load_executor() { return _stream_load_executor; }
    RoutineLoadTaskExecutor* routine_load_task_executor() { return _routine_load_task_executor; }
    HeartbeatFlags* heartbeat_flags() { return _heartbeat_flags; }

    PluginMgr* plugin_mgr() { return _plugin_mgr; }
    RuntimeFilterWorker* runtime_filter_worker() { return _runtime_filter_worker; }
    Status init_mem_tracker();

    RuntimeFilterCache* runtime_filter_cache() { return _runtime_filter_cache; }
    void add_rf_event(const RfTracePoint& pt);

    pipeline::QueryContextManager* query_context_mgr() { return _query_context_mgr; }

    pipeline::DriverLimiter* driver_limiter() { return _driver_limiter; }

private:
    Status _init(const std::vector<StorePath>& store_paths);
    void _destroy();

    Status _init_storage_page_cache();

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

    ThreadResourceMgr* _thread_mgr = nullptr;
    PriorityThreadPool* _thread_pool = nullptr;
    PriorityThreadPool* _pipeline_scan_io_thread_pool = nullptr;
    std::atomic<size_t> _num_scan_operators;
    PriorityThreadPool* _etl_thread_pool = nullptr;
    PriorityThreadPool* _udf_call_pool = nullptr;
    FragmentMgr* _fragment_mgr = nullptr;
    starrocks::pipeline::QueryContextManager* _query_context_mgr = nullptr;
    starrocks::pipeline::DriverExecutor* _driver_executor = nullptr;
    pipeline::DriverExecutor* _wg_driver_executor = nullptr;
    pipeline::DriverLimiter* _driver_limiter;

    TMasterInfo* _master_info = nullptr;

    LoadPathMgr* _load_path_mgr = nullptr;

    starrocks::workgroup::ScanExecutor* _scan_executor = nullptr;

    BfdParser* _bfd_parser = nullptr;
    BrokerMgr* _broker_mgr = nullptr;
    LoadChannelMgr* _load_channel_mgr = nullptr;
    LoadStreamMgr* _load_stream_mgr = nullptr;
    BrpcStubCache* _brpc_stub_cache = nullptr;

    StorageEngine* _storage_engine = nullptr;

    StreamLoadExecutor* _stream_load_executor = nullptr;
    RoutineLoadTaskExecutor* _routine_load_task_executor = nullptr;
    SmallFileMgr* _small_file_mgr = nullptr;
    HeartbeatFlags* _heartbeat_flags = nullptr;

    PluginMgr* _plugin_mgr = nullptr;

    RuntimeFilterWorker* _runtime_filter_worker = nullptr;
    RuntimeFilterCache* _runtime_filter_cache = nullptr;
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
