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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/exec_env.cpp

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

#include "exec/exec_env.h"

#include <algorithm>
#include <memory>

#include "base/time/time.h"
#include "common/config_exec_env_fwd.h"
#include "common/logging.h"
#include "common/metrics/process_metrics_registry.h"
#include "common/process_exit.h"
#include "common/thread/priority_thread_pool.hpp"
#include "common/thread/threadpool.h"
#include "compute_env/compute_env.h"
#include "compute_env/load/stream_context_mgr.h"
#include "compute_env/pipeline/driver_limiter.h"
#include "compute_env/workgroup/scan_executor.h"
#include "compute_env/workgroup/work_group_manager.h"
#include "connector/builtin_connector_registry.h"
#include "connector/connector_registry.h"
#include "connector/connector_sink_executor.h"
#include "exec/batch_write/batch_write_mgr.h"
#include "exec/lookup_stream_mgr.h"
#include "exec/pipeline/primitives/driver_executor.h"
#include "exec/pipeline/query_context.h"
#include "exec/runtime/query_context_manager.h"
#include "exec/stream_load/stream_load_executor.h"
#include "exec/stream_load/transaction_mgr.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "platform/platform_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_filter_cache.h"
#include "udf/python/env.h"

#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/jit_engine.h"
#endif

namespace starrocks {
namespace {

template <typename T>
void delete_and_null(T*& ptr) {
    delete ptr;
    ptr = nullptr;
}

} // namespace

ExecEnv* ExecEnv::GetInstance() {
    static ExecEnv s_exec_env;
    return &s_exec_env;
}

ExecEnv::ExecEnv() : _global_env(GlobalEnv::GetInstance()) {
    _refresh_service_contexts();
}
ExecEnv::~ExecEnv() = default;

void ExecEnv::_refresh_service_contexts() {
    auto* global_env = _global_env;
    DCHECK(global_env != nullptr);
    _execution_services.thread_pool = global_env->thread_pool();
    _execution_services.streaming_load_thread_pool = global_env->streaming_load_thread_pool();
    _execution_services.load_rowset_thread_pool = global_env->load_rowset_thread_pool();
    _execution_services.load_segment_thread_pool = global_env->load_segment_thread_pool();
    _execution_services.put_combined_txn_log_thread_pool = global_env->put_combined_txn_log_thread_pool();
    _execution_services.udf_call_pool = global_env->udf_call_pool();
    _execution_services.pipeline_prepare_pool = global_env->pipeline_prepare_pool();
    _execution_services.pipeline_sink_io_pool = global_env->pipeline_sink_io_pool();
    _execution_services.query_rpc_pool = global_env->query_rpc_pool();
    _execution_services.datacache_rpc_pool = global_env->datacache_rpc_pool();
    _execution_services.load_rpc_pool = global_env->load_rpc_pool();
    _execution_services.dictionary_cache_pool = global_env->dictionary_cache_pool();
    _execution_services.automatic_partition_pool = global_env->automatic_partition_pool();
    _execution_services.workgroup_manager = workgroup_manager();
    _execution_services.driver_limiter = _compute_env == nullptr ? nullptr : _compute_env->driver_limiter();
    _execution_services.pipeline_timer = _compute_env == nullptr ? nullptr : _compute_env->pipeline_timer();
    _execution_services.max_executor_threads = global_env->max_executor_threads();

    auto* platform_env = PlatformEnv::GetInstance();
    _platform_services.store_path_registry = platform_env->store_path_registry();

    _rpc_services.backend_client_cache = platform_env->backend_client_cache();
    _rpc_services.frontend_client_cache = platform_env->frontend_client_cache();
    _rpc_services.broker_client_cache = platform_env->broker_client_cache();
    _rpc_services.broker_mgr = platform_env->broker_mgr();
    _rpc_services.brpc_stub_cache = platform_env->brpc_stub_cache();

    _lake_services.put_aggregate_metadata_thread_pool = global_env->put_aggregate_metadata_thread_pool();
    _lake_services.lake_metadata_fetch_thread_pool = global_env->lake_metadata_fetch_thread_pool();
    _lake_services.lake_vector_index_build_thread_pool = global_env->lake_vector_index_build_thread_pool();
    _lake_services.pk_index_execution_thread_pool = global_env->pk_index_execution_thread_pool();
    _lake_services.pk_index_memtable_flush_thread_pool = global_env->pk_index_memtable_flush_thread_pool();
    _lake_services.lake_partial_update_thread_pool = global_env->lake_partial_update_thread_pool();

    _runtime_services.stream_mgr = stream_mgr();
    _runtime_services.lookup_dispatcher_mgr = _lookup_dispatcher_mgr;
    _runtime_services.result_mgr = result_mgr();
    _runtime_services.result_queue_mgr = result_queue_mgr();
    _runtime_services.load_path_mgr = load_path_mgr();
    _runtime_services.load_stream_mgr = load_stream_mgr();
    _runtime_services.stream_context_mgr = stream_context_mgr();
    _runtime_services.transaction_mgr = _transaction_mgr;
    _runtime_services.batch_write_mgr = _batch_write_mgr;
    _runtime_services.stream_load_executor = _stream_load_executor;
    _runtime_services.runtime_filter_sender = _runtime_filter_sender;
    _runtime_services.runtime_filter_query_lifecycle = _runtime_filter_query_lifecycle;
    _runtime_services.runtime_filter_cache = _runtime_filter_cache;
    _runtime_services.profile_report_worker = profile_report_worker();
    _runtime_services.query_context_mgr = _query_context_mgr;
    _runtime_services.cache_mgr = cache_mgr();
    _runtime_services.spill_dir_mgr = _compute_env == nullptr ? nullptr : _compute_env->spill_dir_mgr();
    _runtime_services.global_spill_manager = _compute_env == nullptr ? nullptr : _compute_env->global_spill_manager();
    _runtime_services.connector_sink_spill_executor = _connector_sink_spill_executor;
    _runtime_services.diagnose_daemon = global_env->diagnose_daemon();

    _agent_services.agent_server = _agent_server;

    _query_execution_services.execution = &_execution_services;
    _query_execution_services.rpc = &_rpc_services;
    _query_execution_services.runtime = &_runtime_services;
    _query_execution_services.process_metrics =
            _process_metrics_registry == nullptr ? nullptr : _process_metrics_registry->root_registry();

    _admin_services.execution = &_execution_services;
    _admin_services.rpc = &_rpc_services;
    _admin_services.runtime = &_runtime_services;
    _admin_services.agent = &_agent_services;
}

void ExecEnv::set_agent_server(AgentServer* agent_server) {
    _agent_server = agent_server;
    _refresh_service_contexts();
}

void ExecEnv::set_compute_env(ComputeEnv* compute_env) {
    _compute_env = compute_env;
    _refresh_service_contexts();
}

void ExecEnv::set_runtime_filter_services(RuntimeFilterSender* sender, RuntimeFilterQueryLifecycle* query_lifecycle) {
    _runtime_filter_sender = sender;
    _runtime_filter_query_lifecycle = query_lifecycle;
    _refresh_service_contexts();
}

Status ExecEnv::init(ProcessMetricsRegistry* process_metrics_registry, GlobalEnv* global_env) {
    DCHECK(process_metrics_registry != nullptr);
    DCHECK(global_env != nullptr);
    _global_env = global_env;
    auto* platform_env = PlatformEnv::GetInstance();
    if (platform_env->backend_client_cache() == nullptr || platform_env->frontend_client_cache() == nullptr ||
        platform_env->broker_client_cache() == nullptr || platform_env->broker_mgr() == nullptr ||
        platform_env->brpc_stub_cache() == nullptr || platform_env->small_file_mgr() == nullptr) {
        return Status::InternalError("PlatformEnv is not initialized");
    }
    if (_compute_env == nullptr) {
        return Status::InternalError("ComputeEnv is not attached");
    }
    RETURN_IF_ERROR(connector::install_builtin_connectors(connector::ConnectorRegistry::default_instance()));
    _process_metrics_registry = process_metrics_registry;
    auto* process_metrics = process_metrics_registry->root_registry();
    _table_metrics_mgr = process_metrics_registry->table_metrics_mgr();
    _lookup_dispatcher_mgr = new LookUpDispatcherMgr();
    // query_context_mgr keeps slotted map with 64 slot to reduce contention
    _query_context_mgr = new pipeline::QueryContextManager(6);
    RETURN_IF_ERROR(_query_context_mgr->init(process_metrics));

    _stream_load_executor = new StreamLoadExecutor(this);
    _transaction_mgr = new TransactionMgr(this);

    std::unique_ptr<ThreadPool> batch_write_thread_pool;
    RETURN_IF_ERROR(ThreadPoolBuilder("batch_write")
                            .set_min_threads(config::merge_commit_thread_pool_num_min)
                            .set_max_threads(config::merge_commit_thread_pool_num_max)
                            .set_max_queue_size(config::merge_commit_thread_pool_queue_size)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(10000))
                            .build(&batch_write_thread_pool));
    auto batch_write_executor =
            std::make_unique<bthreads::ThreadPoolExecutor>(batch_write_thread_pool.release(), kTakesOwnership);
    _batch_write_mgr = new BatchWriteMgr(std::move(batch_write_executor));
    RETURN_IF_ERROR(_batch_write_mgr->init(process_metrics));

    _connector_sink_spill_executor = new connector::ConnectorSinkSpillExecutor();
    RETURN_IF_ERROR(_connector_sink_spill_executor->init());

    _runtime_filter_cache = new RuntimeFilterCache(8);
    RETURN_IF_ERROR(_runtime_filter_cache->init());

#ifdef STARROCKS_JIT_ENABLE
    auto jit_engine = JITEngine::get_instance();
    Status status = jit_engine->init();
    if (!status.ok()) {
        LOG(WARNING) << "Failed to init JIT engine: " << status.message();
    }
#endif

    PythonEnvManager::getInstance().start_background_cleanup_thread();

    _refresh_service_contexts();

    return Status::OK();
}

DataStreamMgr* ExecEnv::stream_mgr() {
    return _compute_env == nullptr ? nullptr : _compute_env->stream_mgr();
}

ResultBufferMgr* ExecEnv::result_mgr() {
    return _compute_env == nullptr ? nullptr : _compute_env->result_mgr();
}

ResultQueueMgr* ExecEnv::result_queue_mgr() {
    return _compute_env == nullptr ? nullptr : _compute_env->result_queue_mgr();
}

query_cache::CacheManagerRawPtr ExecEnv::cache_mgr() const {
    return _compute_env == nullptr ? nullptr : _compute_env->cache_mgr();
}

BaseLoadPathMgr* ExecEnv::load_path_mgr() {
    return _compute_env == nullptr ? nullptr : _compute_env->load_path_mgr();
}

LoadStreamMgr* ExecEnv::load_stream_mgr() {
    return _compute_env == nullptr ? nullptr : _compute_env->load_stream_mgr();
}

StreamContextMgr* ExecEnv::stream_context_mgr() {
    return _compute_env == nullptr ? nullptr : _compute_env->stream_context_mgr();
}

ProfileReportWorker* ExecEnv::profile_report_worker() {
    return _compute_env == nullptr ? nullptr : _compute_env->profile_report_worker();
}

void ExecEnv::stop() {
    int64_t total_start = MonotonicMillis();
    int64_t start;
    std::vector<std::pair<std::string, int64_t>> component_times;

    if (_lookup_dispatcher_mgr != nullptr) {
        _lookup_dispatcher_mgr->close();
    }

    if (_batch_write_mgr) {
        start = MonotonicMillis();
        _batch_write_mgr->stop();
        component_times.emplace_back("batch_write_mgr", MonotonicMillis() - start);
    }

    start = MonotonicMillis();
    PythonEnvManager::getInstance().close();
    component_times.emplace_back("PythonEnvManager", MonotonicMillis() - start);

    int64_t total_time = MonotonicMillis() - total_start;
    std::string summary = strings::Substitute("[ExecEnv::stop] Total: $0 ms", total_time);
    if (!component_times.empty()) {
        summary += " (";
        std::vector<std::string> parts;
        for (const auto& [name, time] : component_times) {
            if (time > 0) {
                parts.push_back(strings::Substitute("$0:$1ms", name, time));
            }
        }
        summary += JoinStrings(parts, ", ");
        summary += ")";
    }
    LOG(INFO) << summary;
}

void ExecEnv::clear_query_contexts() {
    if (_query_context_mgr == nullptr) {
        return;
    }
    int64_t start = MonotonicMillis();
    _query_context_mgr->clear();
    LOG(INFO) << strings::Substitute("[ExecEnv::clear_query_contexts] Total: $0 ms", MonotonicMillis() - start);
}

void ExecEnv::destroy() {
    delete_and_null(_transaction_mgr);
    delete_and_null(_stream_load_executor);
    delete_and_null(_connector_sink_spill_executor);
    delete_and_null(_query_context_mgr);

    delete_and_null(_runtime_filter_cache);
    delete_and_null(_lookup_dispatcher_mgr);
    delete_and_null(_batch_write_mgr);
    _table_metrics_mgr = nullptr;
    _process_metrics_registry = nullptr;
    _compute_env = nullptr;
    _refresh_service_contexts();
}

uint32_t ExecEnv::calc_pipeline_dop(int32_t pipeline_dop) const {
    if (pipeline_dop > 0) {
        return pipeline_dop;
    }

    // Default dop is a half of the number of hardware threads.
    return std::max<uint32_t>(1, max_executor_threads() / 2);
}

uint32_t ExecEnv::calc_pipeline_sink_dop(int32_t pipeline_sink_dop) const {
    if (pipeline_sink_dop > 0) {
        return pipeline_sink_dop;
    }

    // Default sink dop is the number of hardware threads with a cap of 64.
    auto dop = std::max<uint32_t>(1, max_executor_threads());
    return std::min<uint32_t>(dop, 64);
}

workgroup::WorkGroupManager* ExecEnv::workgroup_manager() {
    return _compute_env == nullptr ? nullptr : _compute_env->workgroup_manager();
}

pipeline::DriverExecutor* ExecEnv::wg_driver_executor() {
    return workgroup_manager()->shared_executors()->driver_executor();
}
workgroup::ScanExecutor* ExecEnv::scan_executor() {
    return workgroup_manager()->shared_executors()->scan_executor();
}
workgroup::ScanExecutor* ExecEnv::connector_scan_executor() {
    return workgroup_manager()->shared_executors()->connector_scan_executor();
}

} // namespace starrocks
