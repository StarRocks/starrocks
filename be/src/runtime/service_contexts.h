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

#pragma once

#include <cstdint>

namespace starrocks {

class AgentServer;
class BackendServiceClient;
class BatchWriteMgr;
class BrokerMgr;
class BrpcStubCache;
class DataStreamMgr;
class DiagnoseDaemon;
class ExternalScanContextMgr;
class FragmentMgr;
class BaseLoadPathMgr;
class HeartbeatFlags;
class LoadChannelMgr;
class LoadStreamMgr;
class LookUpDispatcherMgr;
class PriorityThreadPool;
class ProfileReportWorker;
class ResultBufferMgr;
class ResultQueueMgr;
class RoutineLoadTaskExecutor;
class RuntimeFilterCache;
class RuntimeFilterWorker;
class SmallFileMgr;
class StreamContextMgr;
class StreamLoadExecutor;
class TFileBrokerServiceClient;
class ThreadPool;
class TransactionMgr;
class FrontendServiceClient;
template <class T>
class ClientCache;

namespace connector {
class ConnectorSinkSpillExecutor;
}

namespace lake {
class LakePersistentIndexParallelCompactMgr;
class ReplicationTxnManager;
class TabletManager;
class UpdateManager;
} // namespace lake

namespace pipeline {
class DriverLimiter;
class PipelineTimer;
class QueryContextManager;
} // namespace pipeline

namespace query_cache {
class CacheManager;
} // namespace query_cache

namespace spill {
class DirManager;
class GlobalSpillManager;
} // namespace spill

namespace workgroup {
class WorkGroupManager;
} // namespace workgroup

struct ExecutionEnv {
    PriorityThreadPool* thread_pool = nullptr;
    ThreadPool* streaming_load_thread_pool = nullptr;
    ThreadPool* load_rowset_thread_pool = nullptr;
    ThreadPool* load_segment_thread_pool = nullptr;
    ThreadPool* put_combined_txn_log_thread_pool = nullptr;
    PriorityThreadPool* udf_call_pool = nullptr;
    PriorityThreadPool* pipeline_prepare_pool = nullptr;
    PriorityThreadPool* pipeline_sink_io_pool = nullptr;
    PriorityThreadPool* query_rpc_pool = nullptr;
    PriorityThreadPool* datacache_rpc_pool = nullptr;
    ThreadPool* load_rpc_pool = nullptr;
    ThreadPool* dictionary_cache_pool = nullptr;
    ThreadPool* automatic_partition_pool = nullptr;
    workgroup::WorkGroupManager* workgroup_manager = nullptr;
    pipeline::DriverLimiter* driver_limiter = nullptr;
    pipeline::PipelineTimer* pipeline_timer = nullptr;
    int64_t max_executor_threads = 0;
};

struct RpcServices {
    ClientCache<BackendServiceClient>* backend_client_cache = nullptr;
    ClientCache<FrontendServiceClient>* frontend_client_cache = nullptr;
    ClientCache<TFileBrokerServiceClient>* broker_client_cache = nullptr;
    BrokerMgr* broker_mgr = nullptr;
    BrpcStubCache* brpc_stub_cache = nullptr;
};

struct LakeServices {
    lake::TabletManager* lake_tablet_manager = nullptr;
    lake::UpdateManager* lake_update_manager = nullptr;
    lake::ReplicationTxnManager* lake_replication_txn_manager = nullptr;
    ThreadPool* put_aggregate_metadata_thread_pool = nullptr;
    ThreadPool* lake_metadata_fetch_thread_pool = nullptr;
    lake::LakePersistentIndexParallelCompactMgr* parallel_compact_mgr = nullptr;
    ThreadPool* pk_index_execution_thread_pool = nullptr;
    ThreadPool* pk_index_memtable_flush_thread_pool = nullptr;
    ThreadPool* lake_partial_update_thread_pool = nullptr;
};

struct RuntimeServices {
    ExternalScanContextMgr* external_scan_context_mgr = nullptr;
    DataStreamMgr* stream_mgr = nullptr;
    LookUpDispatcherMgr* lookup_dispatcher_mgr = nullptr;
    ResultBufferMgr* result_mgr = nullptr;
    ResultQueueMgr* result_queue_mgr = nullptr;
    FragmentMgr* fragment_mgr = nullptr;
    BaseLoadPathMgr* load_path_mgr = nullptr;
    LoadChannelMgr* load_channel_mgr = nullptr;
    LoadStreamMgr* load_stream_mgr = nullptr;
    StreamContextMgr* stream_context_mgr = nullptr;
    TransactionMgr* transaction_mgr = nullptr;
    BatchWriteMgr* batch_write_mgr = nullptr;
    StreamLoadExecutor* stream_load_executor = nullptr;
    RoutineLoadTaskExecutor* routine_load_task_executor = nullptr;
    SmallFileMgr* small_file_mgr = nullptr;
    RuntimeFilterWorker* runtime_filter_worker = nullptr;
    RuntimeFilterCache* runtime_filter_cache = nullptr;
    ProfileReportWorker* profile_report_worker = nullptr;
    pipeline::QueryContextManager* query_context_mgr = nullptr;
    query_cache::CacheManager* cache_mgr = nullptr;
    spill::DirManager* spill_dir_mgr = nullptr;
    spill::GlobalSpillManager* global_spill_manager = nullptr;
    connector::ConnectorSinkSpillExecutor* connector_sink_spill_executor = nullptr;
    DiagnoseDaemon* diagnose_daemon = nullptr;
};

struct AgentServices {
    AgentServer* agent_server = nullptr;
    HeartbeatFlags* heartbeat_flags = nullptr;
};

struct QueryExecutionServices {
    const ExecutionEnv* execution = nullptr;
    const RpcServices* rpc = nullptr;
    const LakeServices* lake = nullptr;
    const RuntimeServices* runtime = nullptr;
};

struct AdminServices {
    const ExecutionEnv* execution = nullptr;
    const RpcServices* rpc = nullptr;
    const LakeServices* lake = nullptr;
    const RuntimeServices* runtime = nullptr;
    const AgentServices* agent = nullptr;
};

} // namespace starrocks
