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

#include <cstdint>

#include "common/status.h"
#include "common/thread/threadpool.h"
#include "compute_env/query_cache/cache_manager_fwd.h"
#include "compute_env/workgroup/work_group_fwd.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "runtime/env/global_env.h"
#include "runtime/mem_tracker_fwd.h"
#include "runtime/service_contexts.h"
// NOTE: Be careful about adding includes here. This file is included by many files.
// Unnecessary includes will cause compilation very slow.
// So please consider use forward declaration as much as possible.

namespace starrocks {
class AgentServer;
class ComputeEnv;
class DataStreamMgr;
class EvHttpServer;
class BaseLoadPathMgr;
class LoadPathMgr;
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
class WebPageHandler;
class StreamLoadExecutor;
class RuntimeFilterCache;
class ProfileReportWorker;

namespace pipeline {
class DriverExecutor;
class QueryContextManager;
class DriverLimiter;
} // namespace pipeline

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
    Status init(ProcessMetricsRegistry* process_metrics_registry, GlobalEnv* global_env);
    void stop();
    void clear_query_contexts();
    void destroy();
    /// Returns the first created exec env instance. In a normal starrocks, this is
    /// the only instance. In test setups with multiple ExecEnv's per process,
    /// we return the most recently created instance.
    static ExecEnv* GetInstance();

    // only used for test
    ExecEnv();

    // Empty destructor because the compiler-generated one requires full
    // declarations for classes in scoped_ptrs.
    ~ExecEnv();

    ProcessMetricsRegistry* process_metrics_registry() const { return _process_metrics_registry; }
    TableMetricsManager* table_metrics_mgr() const { return _table_metrics_mgr; }
    DataStreamMgr* stream_mgr();
    LookUpDispatcherMgr* lookup_dispatcher_mgr() { return _lookup_dispatcher_mgr; }
    ResultBufferMgr* result_mgr();
    ResultQueueMgr* result_queue_mgr();

    pipeline::DriverExecutor* wg_driver_executor();
    workgroup::ScanExecutor* scan_executor();
    workgroup::ScanExecutor* connector_scan_executor();
    workgroup::WorkGroupManager* workgroup_manager();

    BaseLoadPathMgr* load_path_mgr();
    LoadStreamMgr* load_stream_mgr();
    StreamContextMgr* stream_context_mgr();
    TransactionMgr* transaction_mgr() { return _transaction_mgr; }
    BatchWriteMgr* batch_write_mgr() { return _batch_write_mgr; }

    StreamLoadExecutor* stream_load_executor() { return _stream_load_executor; }
    const ExecutionEnv& execution_services() const { return _execution_services; }
    const PlatformServices& platform_services() const { return _platform_services; }
    const RpcServices& rpc_services() const { return _rpc_services; }
    const LakeServices& lake_services() const { return _lake_services; }
    const RuntimeServices& runtime_services() const { return _runtime_services; }
    const AgentServices& agent_services() const { return _agent_services; }
    const QueryExecutionServices& query_execution_services() const { return _query_execution_services; }
    const AdminServices& admin_services() const { return _admin_services; }

    connector::ConnectorSinkSpillExecutor* connector_sink_spill_executor() { return _connector_sink_spill_executor; }

    void set_runtime_filter_services(RuntimeFilterSender* sender, RuntimeFilterQueryLifecycle* query_lifecycle);
    MemTracker* query_pool_mem_tracker() { return _global_env->query_pool_mem_tracker(); }

    RuntimeFilterCache* runtime_filter_cache() { return _runtime_filter_cache; }

    ProfileReportWorker* profile_report_worker();

    pipeline::QueryContextManager* query_context_mgr() { return _query_context_mgr; }

    ComputeEnv* compute_env() const { return _compute_env; }

    int64_t max_executor_threads() const { return _global_env->max_executor_threads(); }

    uint32_t calc_pipeline_dop(int32_t pipeline_dop) const;

    uint32_t calc_pipeline_sink_dop(int32_t pipeline_sink_dop) const;

    AgentServer* agent_server() const { return _agent_server; }
    void set_agent_server(AgentServer* agent_server);
    void set_compute_env(ComputeEnv* compute_env);

    query_cache::CacheManagerRawPtr cache_mgr() const;

private:
    void _refresh_service_contexts();

    GlobalEnv* _global_env = nullptr;
    ProcessMetricsRegistry* _process_metrics_registry = nullptr;
    TableMetricsManager* _table_metrics_mgr = nullptr;
    pipeline::QueryContextManager* _query_context_mgr = nullptr;
    ComputeEnv* _compute_env = nullptr;

    TransactionMgr* _transaction_mgr = nullptr;
    BatchWriteMgr* _batch_write_mgr = nullptr;

    [[maybe_unused]] StorageEngine* _storage_engine = nullptr;

    StreamLoadExecutor* _stream_load_executor = nullptr;

    connector::ConnectorSinkSpillExecutor* _connector_sink_spill_executor = nullptr;

    RuntimeFilterSender* _runtime_filter_sender = nullptr;
    RuntimeFilterQueryLifecycle* _runtime_filter_query_lifecycle = nullptr;
    RuntimeFilterCache* _runtime_filter_cache = nullptr;

    AgentServer* _agent_server = nullptr;
    LookUpDispatcherMgr* _lookup_dispatcher_mgr = nullptr;
    ExecutionEnv _execution_services;
    PlatformServices _platform_services;
    RpcServices _rpc_services;
    LakeServices _lake_services;
    RuntimeServices _runtime_services;
    AgentServices _agent_services;
    QueryExecutionServices _query_execution_services;
    AdminServices _admin_services;
};

} // namespace starrocks
