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
#include <vector>

#include "common/status.h"
#include "common/thread/threadpool.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/query_cache/cache_manager_fwd.h"
#include "exec/workgroup/work_group_fwd.h"
#include "runtime/env/global_env.h"
#include "runtime/mem_tracker_fwd.h"
#include "runtime/service_contexts.h"
// NOTE: Be careful about adding includes here. This file is included by many files.
// Unnecessary includes will cause compilation very slow.
// So please consider use forward declaration as much as possible.

namespace starrocks {
struct StorePath;
class AgentServer;
class BrokerMgr;
class ComputeEnv;
class DataStreamMgr;
class EvHttpServer;
class ExternalScanContextMgr;
class FragmentMgr;
class BaseLoadPathMgr;
class LoadPathMgr;
class RejectedRecordSyncDaemon;
class LoadStreamMgr;
class LookUpDispatcherMgr;
class StreamContextMgr;
class TransactionMgr;
class BatchWriteMgr;
class ProcessMetricsRegistry;
class StorageEngine;
class TableMetricsManager;
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
class GlobalSpillManager;

class HeartbeatFlags;
class DiagnoseDaemon;

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
class LakePersistentIndexParallelCompactMgr;
} // namespace lake
namespace spill {
class DirManager;
class GlobalSpillManager;
} // namespace spill

namespace connector {
class ConnectorSinkSpillExecutor;
}

// Execution environment for queries/plan fragments.
// Contains all required global structures, and handles to
// singleton services. Clients must call StartServices exactly
// once to properly initialise service state.
class ExecEnv {
public:
    // Initial exec environment. must call this to init all
    Status init(const std::vector<StorePath>& store_paths, ProcessMetricsRegistry* process_metrics_registry,
                GlobalEnv* global_env, bool as_cn = false);
    void stop();
    void destroy();
    void wait_for_finish();

    /// Returns the first created exec env instance. In a normal starrocks, this is
    /// the only instance. In test setups with multiple ExecEnv's per process,
    /// we return the most recently created instance.
    static ExecEnv* GetInstance();

    // only used for test
    ExecEnv();

    // Empty destructor because the compiler-generated one requires full
    // declarations for classes in scoped_ptrs.
    ~ExecEnv();

    std::string token() const;
    ExternalScanContextMgr* external_scan_context_mgr() { return _external_scan_context_mgr; }
    ProcessMetricsRegistry* process_metrics_registry() const { return _process_metrics_registry; }
    TableMetricsManager* table_metrics_mgr() const { return _table_metrics_mgr; }
    DataStreamMgr* stream_mgr() { return _stream_mgr; }
    LookUpDispatcherMgr* lookup_dispatcher_mgr() { return _lookup_dispatcher_mgr; }
    ResultBufferMgr* result_mgr() { return _result_mgr; }
    ResultQueueMgr* result_queue_mgr() { return _result_queue_mgr; }

    pipeline::DriverExecutor* wg_driver_executor();
    workgroup::ScanExecutor* scan_executor();
    workgroup::ScanExecutor* connector_scan_executor();
    workgroup::WorkGroupManager* workgroup_manager() { return _workgroup_manager.get(); }

    FragmentMgr* fragment_mgr() { return _fragment_mgr; }
    BaseLoadPathMgr* load_path_mgr() { return _load_path_mgr; }
    RejectedRecordSyncDaemon* rejected_record_sync_daemon() { return _rejected_record_sync_daemon; }
    BrokerMgr* broker_mgr() const { return _broker_mgr; }
    LoadChannelMgr* load_channel_mgr() { return _load_channel_mgr; }
    LoadStreamMgr* load_stream_mgr() { return _load_stream_mgr; }
    SmallFileMgr* small_file_mgr() { return _small_file_mgr; }
    StreamContextMgr* stream_context_mgr() { return _stream_context_mgr; }
    TransactionMgr* transaction_mgr() { return _transaction_mgr; }
    BatchWriteMgr* batch_write_mgr() { return _batch_write_mgr; }

    const std::vector<StorePath>& store_paths() const { return _store_paths; }

    StreamLoadExecutor* stream_load_executor() { return _stream_load_executor; }
    RoutineLoadTaskExecutor* routine_load_task_executor() { return _routine_load_task_executor; }
    HeartbeatFlags* heartbeat_flags() { return _heartbeat_flags; }
    const ExecutionEnv& execution_services() const { return _execution_services; }
    const RpcServices& rpc_services() const { return _rpc_services; }
    const LakeServices& lake_services() const { return _lake_services; }
    const RuntimeServices& runtime_services() const { return _runtime_services; }
    const AgentServices& agent_services() const { return _agent_services; }
    const QueryExecutionServices& query_execution_services() const { return _query_execution_services; }
    const AdminServices& admin_services() const { return _admin_services; }

    connector::ConnectorSinkSpillExecutor* connector_sink_spill_executor() { return _connector_sink_spill_executor; }

    RuntimeFilterWorker* runtime_filter_worker() { return _runtime_filter_worker; }
    MemTracker* query_pool_mem_tracker() { return _global_env->query_pool_mem_tracker(); }

    RuntimeFilterCache* runtime_filter_cache() { return _runtime_filter_cache; }

    ProfileReportWorker* profile_report_worker() { return _profile_report_worker; }

    pipeline::QueryContextManager* query_context_mgr() { return _query_context_mgr; }

    ComputeEnv* compute_env() const { return _compute_env.get(); }

    int64_t max_executor_threads() const { return _global_env->max_executor_threads(); }

    uint32_t calc_pipeline_dop(int32_t pipeline_dop) const;

    uint32_t calc_pipeline_sink_dop(int32_t pipeline_sink_dop) const;

    lake::TabletManager* lake_tablet_manager() const { return _lake_tablet_manager; }

    std::shared_ptr<lake::LocationProvider> lake_location_provider() const { return _lake_location_provider; }

    lake::UpdateManager* lake_update_manager() const { return _lake_update_manager; }

    lake::ReplicationTxnManager* lake_replication_txn_manager() const { return _lake_replication_txn_manager; }

    AgentServer* agent_server() const { return _agent_server; }

    query_cache::CacheManagerRawPtr cache_mgr() const { return _cache_mgr; }

    spill::DirManager* spill_dir_mgr() const { return _spill_dir_mgr.get(); }

    spill::GlobalSpillManager* global_spill_manager() const { return _global_spill_manager.get(); }

    ThreadPool* delete_file_thread_pool();

    lake::LakePersistentIndexParallelCompactMgr* parallel_compact_mgr() { return _parallel_compact_mgr.get(); }

    void try_release_resource_before_core_dump();

    DiagnoseDaemon* diagnose_daemon() const { return _diagnose_daemon; }

private:
    void _refresh_service_contexts();
    void _wait_for_fragments_finish();
    size_t _get_running_fragments_count() const;

    GlobalEnv* _global_env = nullptr;
    std::vector<StorePath> _store_paths;
    // Leave protected so that subclasses can override
    ExternalScanContextMgr* _external_scan_context_mgr = nullptr;
    ProcessMetricsRegistry* _process_metrics_registry = nullptr;
    TableMetricsManager* _table_metrics_mgr = nullptr;
    DataStreamMgr* _stream_mgr = nullptr;
    ResultBufferMgr* _result_mgr = nullptr;
    ResultQueueMgr* _result_queue_mgr = nullptr;
    FragmentMgr* _fragment_mgr = nullptr;
    pipeline::QueryContextManager* _query_context_mgr = nullptr;
    std::unique_ptr<workgroup::WorkGroupManager> _workgroup_manager;
    std::unique_ptr<ComputeEnv> _compute_env;

    BaseLoadPathMgr* _load_path_mgr = nullptr;
    RejectedRecordSyncDaemon* _rejected_record_sync_daemon = nullptr;

    BrokerMgr* _broker_mgr = nullptr;
    LoadChannelMgr* _load_channel_mgr = nullptr;
    LoadStreamMgr* _load_stream_mgr = nullptr;
    StreamContextMgr* _stream_context_mgr = nullptr;
    TransactionMgr* _transaction_mgr = nullptr;
    BatchWriteMgr* _batch_write_mgr = nullptr;

    [[maybe_unused]] StorageEngine* _storage_engine = nullptr;

    StreamLoadExecutor* _stream_load_executor = nullptr;
    RoutineLoadTaskExecutor* _routine_load_task_executor = nullptr;
    SmallFileMgr* _small_file_mgr = nullptr;
    HeartbeatFlags* _heartbeat_flags = nullptr;

    connector::ConnectorSinkSpillExecutor* _connector_sink_spill_executor = nullptr;

    RuntimeFilterWorker* _runtime_filter_worker = nullptr;
    RuntimeFilterCache* _runtime_filter_cache = nullptr;

    ProfileReportWorker* _profile_report_worker = nullptr;

    lake::TabletManager* _lake_tablet_manager = nullptr;
    std::shared_ptr<lake::LocationProvider> _lake_location_provider;
    lake::UpdateManager* _lake_update_manager = nullptr;
    lake::ReplicationTxnManager* _lake_replication_txn_manager = nullptr;
    std::unique_ptr<lake::LakePersistentIndexParallelCompactMgr> _parallel_compact_mgr;

    AgentServer* _agent_server = nullptr;
    query_cache::CacheManagerRawPtr _cache_mgr = nullptr;
    std::shared_ptr<spill::DirManager> _spill_dir_mgr;
    std::shared_ptr<spill::GlobalSpillManager> _global_spill_manager;
    DiagnoseDaemon* _diagnose_daemon = nullptr;
    LookUpDispatcherMgr* _lookup_dispatcher_mgr = nullptr;
    ExecutionEnv _execution_services;
    RpcServices _rpc_services;
    LakeServices _lake_services;
    RuntimeServices _runtime_services;
    AgentServices _agent_services;
    QueryExecutionServices _query_execution_services;
    AdminServices _admin_services;
};

} // namespace starrocks
