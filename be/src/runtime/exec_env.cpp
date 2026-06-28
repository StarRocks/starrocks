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

#include "runtime/exec_env.h"

#include <memory>
#include <thread>

#include "agent/agent_server.h"
#include "base/time/time.h"
#include "common/config_exec_env_fwd.h"
#include "common/config_lake_fwd.h"
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
#include "exec/pipeline/driver_executor_factory.h"
#include "exec/pipeline/driver_queue_factory.h"
#include "exec/pipeline/primitives/driver_executor.h"
#include "exec/pipeline/primitives/pipeline_metrics.h"
#include "exec/pipeline/query_context.h"
#include "exec/runtime/query_context_manager.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "platform/platform_env.h"
#include "platform/store_path.h"
#include "runtime/batch_write/batch_write_mgr.h"
#include "runtime/lookup_stream_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/transaction_mgr.h"
#include "storage/storage_env.h"
#include "storage/tablet_schema_map.h"
#include "storage/update_manager.h"
#include "udf/python/env.h"

#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/jit_engine.h"
#endif

namespace starrocks {

ExecEnv* ExecEnv::GetInstance() {
    static ExecEnv s_exec_env;
    return &s_exec_env;
}

ExecEnv::ExecEnv() : _global_env(GlobalEnv::GetInstance()), _compute_env(std::make_unique<ComputeEnv>()) {
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

void ExecEnv::set_runtime_filter_services(RuntimeFilterSender* sender, RuntimeFilterQueryLifecycle* query_lifecycle) {
    _runtime_filter_sender = sender;
    _runtime_filter_query_lifecycle = query_lifecycle;
    _refresh_service_contexts();
}

Status ExecEnv::init(const std::vector<StorePath>& store_paths, ProcessMetricsRegistry* process_metrics_registry,
                     GlobalEnv* global_env, bool as_cn) {
    DCHECK(process_metrics_registry != nullptr);
    DCHECK(global_env != nullptr);
    _global_env = global_env;
    auto* platform_env = PlatformEnv::GetInstance();
    if (platform_env->backend_client_cache() == nullptr || platform_env->frontend_client_cache() == nullptr ||
        platform_env->broker_client_cache() == nullptr || platform_env->broker_mgr() == nullptr ||
        platform_env->brpc_stub_cache() == nullptr || platform_env->small_file_mgr() == nullptr) {
        return Status::InternalError("PlatformEnv is not initialized");
    }
    RETURN_IF_ERROR(connector::install_builtin_connectors(connector::ConnectorRegistry::default_instance()));
    _process_metrics_registry = process_metrics_registry;
    auto* process_metrics = process_metrics_registry->root_registry();
    _table_metrics_mgr = process_metrics_registry->table_metrics_mgr();
    _lookup_dispatcher_mgr = new LookUpDispatcherMgr();
    // query_context_mgr keeps slotted map with 64 slot to reduce contention
    _query_context_mgr = new pipeline::QueryContextManager(6);
    RETURN_IF_ERROR(_query_context_mgr->init(process_metrics));
    RETURN_IF_ERROR(global_env->init_execution_thread_pools(process_metrics));

    // register the metrics to monitor the task queue len
    pipeline::PipelineExecutorMetrics::instance()->register_pipe_prepare_pool_queue_len_hook([global_env] {
        auto pool = global_env->pipeline_prepare_pool();
        return (pool == nullptr) ? 0U : pool->get_queue_size();
    });

    std::vector<std::string> compute_store_paths;
    compute_store_paths.reserve(store_paths.size());
    for (const auto& store_path : store_paths) {
        compute_store_paths.emplace_back(store_path.path);
    }

    ComputeEnvOptions compute_env_options;
    compute_env_options.global_env = global_env;
    compute_env_options.metrics = process_metrics;
    compute_env_options.store_paths = std::move(compute_store_paths);
    compute_env_options.as_cn = as_cn;
    compute_env_options.query_cache_capacity = std::max<size_t>(config::query_cache_capacity, 4L * 1024 * 1024);
    compute_env_options.driver_queue_factory = pipeline::create_query_shared_driver_queue;
    compute_env_options.driver_executor_factory = pipeline::create_workgroup_driver_executor;
    RETURN_IF_ERROR(_compute_env->init(compute_env_options));

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

    StorageEnvOptions storage_env_options;
    storage_env_options.store_path_registry = platform_env->store_path_registry();
    storage_env_options.update_mem_tracker = global_env->update_mem_tracker();
    storage_env_options.process_mem_limit = global_env->process_mem_limit();
    storage_env_options.vector_index_mem_tracker = global_env->vector_index_mem_tracker();
    storage_env_options.lake_metadata_cache_limit = config::lake_metadata_cache_limit;
#if defined(USE_STAROS) && !defined(BE_TEST) && !defined(BUILD_FORMAT_LIB)
    storage_env_options.lake_location_provider_mode = LakeLocationProviderMode::kStarlet;
#elif defined(BE_TEST)
    storage_env_options.lake_location_provider_mode = LakeLocationProviderMode::kFixed;
#endif
    RETURN_IF_ERROR_WITH_WARN(StorageEnv::GetInstance()->init(storage_env_options), "init StorageEnv failed");

    RETURN_IF_ERROR(global_env->init_lake_thread_pools(process_metrics));
    StorageEnv::GetInstance()->set_spill_dir_mgr(_compute_env->spill_dir_mgr());

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
    auto* global_env = _global_env;
    DCHECK(global_env != nullptr);

    if (_compute_env != nullptr) {
        start = MonotonicMillis();
        _compute_env->stop();
        component_times.emplace_back("compute_env", MonotonicMillis() - start);
    }
    if (_lookup_dispatcher_mgr != nullptr) {
        _lookup_dispatcher_mgr->close();
    }

    if (global_env->pipeline_sink_io_pool()) {
        start = MonotonicMillis();
        global_env->pipeline_sink_io_pool()->shutdown();
        component_times.emplace_back("pipeline_sink_io_pool", MonotonicMillis() - start);
    }

    if (global_env->put_aggregate_metadata_thread_pool()) {
        start = MonotonicMillis();
        global_env->put_aggregate_metadata_thread_pool()->shutdown();
        component_times.emplace_back("put_aggregate_metadata_thread_pool", MonotonicMillis() - start);
    }

    if (global_env->lake_metadata_fetch_thread_pool()) {
        start = MonotonicMillis();
        global_env->lake_metadata_fetch_thread_pool()->shutdown();
        component_times.emplace_back("lake_metadata_fetch_thread_pool", MonotonicMillis() - start);
    }

    if (global_env->lake_vector_index_build_thread_pool()) {
        start = MonotonicMillis();
        global_env->lake_vector_index_build_thread_pool()->shutdown();
        component_times.emplace_back("lake_vector_index_build_thread_pool", MonotonicMillis() - start);
    }

    if (StorageEnv::GetInstance()->parallel_compact_mgr() != nullptr) {
        start = MonotonicMillis();
        StorageEnv::GetInstance()->stop();
        component_times.emplace_back("parallel_compact_mgr", MonotonicMillis() - start);
    }

    if (global_env->pk_index_execution_thread_pool()) {
        start = MonotonicMillis();
        global_env->pk_index_execution_thread_pool()->shutdown();
        component_times.emplace_back("pk_index_execution_thread_pool", MonotonicMillis() - start);
    }

    if (global_env->pk_index_memtable_flush_thread_pool()) {
        start = MonotonicMillis();
        global_env->pk_index_memtable_flush_thread_pool()->shutdown();
        component_times.emplace_back("pk_index_memtable_flush_thread_pool", MonotonicMillis() - start);
    }

    if (global_env->lake_partial_update_thread_pool()) {
        start = MonotonicMillis();
        global_env->lake_partial_update_thread_pool()->shutdown();
        component_times.emplace_back("lake_partial_update_thread_pool", MonotonicMillis() - start);
    }

    if (profile_report_worker()) {
        start = MonotonicMillis();
        _compute_env->stop_profile_report_worker();
        component_times.emplace_back("profile_report_worker", MonotonicMillis() - start);
    }

    if (global_env->automatic_partition_pool()) {
        start = MonotonicMillis();
        global_env->automatic_partition_pool()->shutdown();
        component_times.emplace_back("automatic_partition_pool", MonotonicMillis() - start);
    }

    if (global_env->query_rpc_pool()) {
        start = MonotonicMillis();
        global_env->query_rpc_pool()->shutdown();
        component_times.emplace_back("query_rpc_pool", MonotonicMillis() - start);
    }

    if (global_env->datacache_rpc_pool()) {
        start = MonotonicMillis();
        global_env->datacache_rpc_pool()->shutdown();
        component_times.emplace_back("datacache_rpc_pool", MonotonicMillis() - start);
    }

    if (global_env->load_rpc_pool()) {
        start = MonotonicMillis();
        global_env->load_rpc_pool()->shutdown();
        component_times.emplace_back("load_rpc_pool", MonotonicMillis() - start);
    }

    if (global_env->thread_pool()) {
        start = MonotonicMillis();
        global_env->thread_pool()->shutdown();
        component_times.emplace_back("thread_pool", MonotonicMillis() - start);
    }

    if (_query_context_mgr) {
        start = MonotonicMillis();
        _query_context_mgr->clear();
        component_times.emplace_back("query_context_mgr", MonotonicMillis() - start);
    }

    if (_batch_write_mgr) {
        start = MonotonicMillis();
        _batch_write_mgr->stop();
        component_times.emplace_back("batch_write_mgr", MonotonicMillis() - start);
    }

    if (global_env->dictionary_cache_pool()) {
        start = MonotonicMillis();
        global_env->dictionary_cache_pool()->shutdown();
        component_times.emplace_back("dictionary_cache_pool", MonotonicMillis() - start);
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

void ExecEnv::destroy() {
    if (_compute_env != nullptr) {
        _compute_env->destroy_profile_report_worker();
    }
    SAFE_DELETE(_transaction_mgr);
    SAFE_DELETE(_stream_load_executor);
    SAFE_DELETE(_connector_sink_spill_executor);
    SAFE_DELETE(_query_context_mgr);
    // Query/workgroup teardown can release FragmentContext state that still uses
    // ComputeEnv-owned timers, pass-through stream buffers, and workgroup resources.
    if (_compute_env) {
        StorageEnv::GetInstance()->set_spill_dir_mgr(nullptr);
        _compute_env->destroy();
    }

    // WorkGroupManager should release MemTracker of WorkGroups belongs to itself before deallocate
    // _query_pool_mem_tracker.
    SAFE_DELETE(_runtime_filter_cache);
    SAFE_DELETE(_lookup_dispatcher_mgr);
    SAFE_DELETE(_batch_write_mgr);
    StorageEnv::GetInstance()->destroy();
    DCHECK(_global_env != nullptr);
    _global_env->destroy_thread_pools();
    _query_execution_services.process_metrics = nullptr;
    _table_metrics_mgr = nullptr;
    _process_metrics_registry = nullptr;
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
