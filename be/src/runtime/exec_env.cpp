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

#include <cctype>
#include <memory>
#include <thread>

#include "agent/agent_server.h"
#include "base/time/time.h"
#include "common/config_exec_env_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/logging.h"
#include "common/metrics/process_metrics_registry.h"
#include "common/process_exit.h"
#include "common/system/cpu_info.h"
#include "common/system/master_info.h"
#include "common/thread/priority_thread_pool.hpp"
#include "common/thread/threadpool.h"
#include "connector/builtin_connector_registry.h"
#include "connector/connector_registry.h"
#include "connector/connector_sink_executor.h"
#include "exec/pipeline/driver_limiter.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/pipeline_metrics.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/schedule/pipeline_timer.h"
#include "exec/query_cache/cache_manager.h"
#include "exec/spill/dir_manager.h"
#include "exec/spill/global_spill_manager.h"
#include "exec/spill/spill_metrics.h"
#include "exec/workgroup/pipeline_executor_set.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group.h"
#include "fs/fs_s3.h"
#include "gutil/strings/join.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "platform/platform_env.h"
#include "runtime/base_load_path_mgr.h"
#include "runtime/batch_write/batch_write_mgr.h"
#include "runtime/broker_mgr.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/diagnose_daemon.h"
#include "runtime/dummy_load_path_mgr.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/lookup_stream_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/profile_report_worker.h"
#include "runtime/rejected_record_sync_daemon.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_filter_worker.h"
#include "runtime/runtime_metrics.h"
#include "runtime/small_file_mgr.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/transaction_mgr.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"
#include "storage/lake/replication_txn_manager.h"
#include "storage/lake/starlet_location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "storage/tablet_schema_map.h"
#include "storage/update_manager.h"
#include "udf/python/env.h"

#ifdef USE_STAROS
#include <fslib/configuration.h>
#endif

#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/jit_engine.h"
#endif

namespace starrocks {

bool parse_resource_str(const string& str, string* value) {
    if (!str.empty()) {
        std::string tmp_str = str;
        StripLeadingWhiteSpace(&tmp_str);
        StripTrailingWhitespace(&tmp_str);
        if (tmp_str.empty()) {
            return false;
        } else {
            *value = tmp_str;
            std::transform(value->begin(), value->end(), value->begin(), [](char c) { return std::tolower(c); });
            return true;
        }
    } else {
        return false;
    }
}

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
    _execution_services.workgroup_manager = _workgroup_manager.get();
    _execution_services.driver_limiter = _driver_limiter;
    _execution_services.pipeline_timer = _pipeline_timer;
    _execution_services.max_executor_threads = global_env->max_executor_threads();

    auto* platform_env = PlatformEnv::GetInstance();
    _rpc_services.backend_client_cache = platform_env->backend_client_cache();
    _rpc_services.frontend_client_cache = platform_env->frontend_client_cache();
    _rpc_services.broker_client_cache = platform_env->broker_client_cache();
    _rpc_services.broker_mgr = _broker_mgr;
    _rpc_services.brpc_stub_cache = platform_env->brpc_stub_cache();

    _lake_services.lake_tablet_manager = _lake_tablet_manager;
    _lake_services.lake_update_manager = _lake_update_manager;
    _lake_services.lake_replication_txn_manager = _lake_replication_txn_manager;
    _lake_services.put_aggregate_metadata_thread_pool = global_env->put_aggregate_metadata_thread_pool();
    _lake_services.lake_metadata_fetch_thread_pool = global_env->lake_metadata_fetch_thread_pool();
    _lake_services.parallel_compact_mgr = _parallel_compact_mgr.get();
    _lake_services.pk_index_execution_thread_pool = global_env->pk_index_execution_thread_pool();
    _lake_services.pk_index_memtable_flush_thread_pool = global_env->pk_index_memtable_flush_thread_pool();
    _lake_services.lake_partial_update_thread_pool = global_env->lake_partial_update_thread_pool();

    _runtime_services.external_scan_context_mgr = _external_scan_context_mgr;
    _runtime_services.stream_mgr = _stream_mgr;
    _runtime_services.lookup_dispatcher_mgr = _lookup_dispatcher_mgr;
    _runtime_services.result_mgr = _result_mgr;
    _runtime_services.result_queue_mgr = _result_queue_mgr;
    _runtime_services.fragment_mgr = _fragment_mgr;
    _runtime_services.load_path_mgr = _load_path_mgr;
    _runtime_services.load_channel_mgr = _load_channel_mgr;
    _runtime_services.load_stream_mgr = _load_stream_mgr;
    _runtime_services.stream_context_mgr = _stream_context_mgr;
    _runtime_services.transaction_mgr = _transaction_mgr;
    _runtime_services.batch_write_mgr = _batch_write_mgr;
    _runtime_services.stream_load_executor = _stream_load_executor;
    _runtime_services.routine_load_task_executor = _routine_load_task_executor;
    _runtime_services.small_file_mgr = _small_file_mgr;
    _runtime_services.runtime_filter_worker = _runtime_filter_worker;
    _runtime_services.runtime_filter_cache = _runtime_filter_cache;
    _runtime_services.profile_report_worker = _profile_report_worker;
    _runtime_services.query_context_mgr = _query_context_mgr;
    _runtime_services.cache_mgr = _cache_mgr;
    _runtime_services.spill_dir_mgr = _spill_dir_mgr.get();
    _runtime_services.global_spill_manager = _global_spill_manager.get();
    _runtime_services.connector_sink_spill_executor = _connector_sink_spill_executor;
    _runtime_services.diagnose_daemon = _diagnose_daemon;

    _agent_services.agent_server = _agent_server;
    _agent_services.heartbeat_flags = _heartbeat_flags;

    _query_execution_services.execution = &_execution_services;
    _query_execution_services.rpc = &_rpc_services;
    _query_execution_services.lake = &_lake_services;
    _query_execution_services.runtime = &_runtime_services;
    _query_execution_services.process_metrics =
            _process_metrics_registry == nullptr ? nullptr : _process_metrics_registry->root_registry();

    _admin_services.execution = &_execution_services;
    _admin_services.rpc = &_rpc_services;
    _admin_services.lake = &_lake_services;
    _admin_services.runtime = &_runtime_services;
    _admin_services.agent = &_agent_services;
}

Status ExecEnv::init(const std::vector<StorePath>& store_paths, ProcessMetricsRegistry* process_metrics_registry,
                     GlobalEnv* global_env, bool as_cn) {
    DCHECK(process_metrics_registry != nullptr);
    DCHECK(global_env != nullptr);
    _global_env = global_env;
    auto* platform_env = PlatformEnv::GetInstance();
    if (platform_env->backend_client_cache() == nullptr || platform_env->frontend_client_cache() == nullptr ||
        platform_env->broker_client_cache() == nullptr || platform_env->brpc_stub_cache() == nullptr) {
        return Status::InternalError("PlatformEnv is not initialized");
    }
    RETURN_IF_ERROR(connector::install_builtin_connectors(connector::ConnectorRegistry::default_instance()));
    _process_metrics_registry = process_metrics_registry;
    auto* process_metrics = process_metrics_registry->root_registry();
    _table_metrics_mgr = process_metrics_registry->table_metrics_mgr();
    _store_paths = store_paths;
    _external_scan_context_mgr = new ExternalScanContextMgr(this, process_metrics);
    _stream_mgr = new DataStreamMgr(process_metrics);
    _lookup_dispatcher_mgr = new LookUpDispatcherMgr();
    _result_mgr = new ResultBufferMgr(process_metrics);
    _result_queue_mgr = new ResultQueueMgr(process_metrics);
    // query_context_mgr keeps slotted map with 64 slot to reduce contention
    _query_context_mgr = new pipeline::QueryContextManager(6);
    RETURN_IF_ERROR(_query_context_mgr->init(process_metrics));
    RETURN_IF_ERROR(global_env->init_execution_thread_pools(process_metrics));
    _fragment_mgr = new FragmentMgr(this, process_metrics);

    // register the metrics to monitor the task queue len
    pipeline::PipelineExecutorMetrics::instance()->register_pipe_prepare_pool_queue_len_hook([] {
        auto pool = ExecEnv::GetInstance()->pipeline_prepare_pool();
        return (pool == nullptr) ? 0U : pool->get_queue_size();
    });

    const int64_t max_executor_threads = global_env->max_executor_threads();
    _driver_limiter =
            new pipeline::DriverLimiter(max_executor_threads * config::pipeline_max_num_drivers_per_exec_thread);
    pipeline::PipelineExecutorMetrics::instance()->register_pipe_drivers_hook([] {
        auto* driver_limiter = ExecEnv::GetInstance()->driver_limiter();
        return (driver_limiter == nullptr) ? 0 : driver_limiter->num_total_drivers();
    });

    _pipeline_timer = new pipeline::PipelineTimer();
    RETURN_IF_ERROR(_pipeline_timer->start());

    const int num_io_threads = config::pipeline_scan_thread_pool_thread_num <= 0
                                       ? CpuInfo::num_cores()
                                       : config::pipeline_scan_thread_pool_thread_num;
    int connector_num_io_threads = int(config::pipeline_connector_scan_thread_num_per_cpu * CpuInfo::num_cores());
#ifdef BE_TEST
    connector_num_io_threads = std::min(connector_num_io_threads, 2);
#endif
    CHECK_GT(connector_num_io_threads, 0) << "pipeline_connector_scan_thread_num_per_cpu should greater than 0";

    if (config::hdfs_client_enable_hedged_read) {
        // Set hdfs client hedged read pool size
        config::hdfs_client_hedged_read_threadpool_size =
                std::min(connector_num_io_threads * 2, config::hdfs_client_hedged_read_threadpool_size);
        CHECK_GT(config::hdfs_client_hedged_read_threadpool_size, 0)
                << "hdfs_client_hedged_read_threadpool_size should greater than 0";
    }

    // Disable bind cpus when cgroup has cpu quota but no cpuset.
    const bool enable_bind_cpus = config::enable_resource_group_bind_cpus &&
                                  (!CpuInfo::is_cgroup_with_cpu_quota() || CpuInfo::is_cgroup_with_cpuset());
    config::enable_resource_group_bind_cpus = enable_bind_cpus;
    workgroup::PipelineExecutorSetConfig executors_manager_opts(
            CpuInfo::num_cores(), max_executor_threads, num_io_threads, connector_num_io_threads,
            CpuInfo::get_core_ids(), enable_bind_cpus, config::enable_resource_group_cpu_borrowing,
            pipeline::PipelineExecutorMetrics::instance());
    _workgroup_manager =
            std::make_unique<workgroup::WorkGroupManager>(std::move(executors_manager_opts), process_metrics);
    RETURN_IF_ERROR(_workgroup_manager->start());
    workgroup::DefaultWorkGroupInitialization default_workgroup_init(_workgroup_manager.get(), max_executor_threads);

    if (store_paths.empty() && as_cn) {
        _load_path_mgr = new DummyLoadPathMgr();
    } else {
        _load_path_mgr = new LoadPathMgr(this);
    }

    _broker_mgr = new BrokerMgr(process_metrics);

    _load_stream_mgr = new LoadStreamMgr(process_metrics);
    _stream_load_executor = new StreamLoadExecutor(this);
    _stream_context_mgr = new StreamContextMgr();
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

#ifndef __APPLE__
    _routine_load_task_executor = new RoutineLoadTaskExecutor(this);
    RETURN_IF_ERROR(_routine_load_task_executor->init(process_metrics));
#endif

    _connector_sink_spill_executor = new connector::ConnectorSinkSpillExecutor();
    RETURN_IF_ERROR(_connector_sink_spill_executor->init());

    _small_file_mgr = new SmallFileMgr(config::small_file_dir, process_metrics);
    _runtime_filter_worker = new RuntimeFilterWorker(&_runtime_services, &_rpc_services);
    _runtime_filter_cache = new RuntimeFilterCache(8);
    RETURN_IF_ERROR(_runtime_filter_cache->init());
    _profile_report_worker = new ProfileReportWorker(_fragment_mgr, _query_context_mgr);
    RuntimeMetrics::instance()->register_runtime_filter_event_queue_len_hook([] {
        auto pool = ExecEnv::GetInstance()->runtime_filter_worker();
        return (pool == nullptr) ? 0U : pool->queue_size();
    });

    RETURN_IF_ERROR(_result_mgr->init());

    // it means acting as compute node while store_path is empty. some threads are not needed for that case.
    Status status = _load_path_mgr->init();
    if (!status.ok()) {
        LOG(ERROR) << "load path mgr init failed." << status.message();
        exit(-1);
    }

    // Phase 3 of the rejected records feature. The daemon thread is
    // started unconditionally; its tick_loop() re-reads
    // `config::enable_rejected_record_sync` every interval and treats a
    // false value as a no-op. Starting it only when the flag is true at
    // BE-boot time would break the mutable-config contract: operators
    // (and tests) expect `update_config?enable_rejected_record_sync=true`
    // to activate shipping without a BE restart. Leaving the thread
    // parked costs one condvar wait per interval and no I/O when the
    // flag is off.
    _rejected_record_sync_daemon = new RejectedRecordSyncDaemon(this);
    Status rr_status = _rejected_record_sync_daemon->init();
    if (!rr_status.ok()) {
        LOG(ERROR) << "RejectedRecordSyncDaemon init failed: " << rr_status.message();
        // Non-fatal: the load path still works, we just don't ship
        // rejected records to the system table this run.
    }

#if defined(USE_STAROS) && !defined(BE_TEST) && !defined(BUILD_FORMAT_LIB)
    _lake_location_provider = std::make_shared<lake::StarletLocationProvider>();
    _lake_update_manager = new lake::UpdateManager(_lake_location_provider, global_env->update_mem_tracker());
    _lake_tablet_manager =
            new lake::TabletManager(_lake_location_provider, _lake_update_manager, config::lake_metadata_cache_limit);
    _lake_replication_txn_manager = new lake::ReplicationTxnManager(_lake_tablet_manager);
    if (config::starlet_cache_dir.empty()) {
        std::vector<std::string> starlet_cache_paths;
        std::for_each(store_paths.begin(), store_paths.end(), [&](const StorePath& root_path) {
            std::string starlet_cache_path = root_path.path + "/starlet_cache";
            starlet_cache_paths.emplace_back(starlet_cache_path);
        });
        config::starlet_cache_dir = JoinStrings(starlet_cache_paths, ":");
    }
    setenv(staros::starlet::fslib::kFslibCacheDir.c_str(), config::starlet_cache_dir.c_str(), 1);

    _parallel_compact_mgr = std::make_unique<lake::LakePersistentIndexParallelCompactMgr>(_lake_tablet_manager);
    RETURN_IF_ERROR_WITH_WARN(_parallel_compact_mgr->init(), "init ParallelCompactMgr failed");

#elif defined(BE_TEST)
    _lake_location_provider = std::make_shared<lake::FixedLocationProvider>(_store_paths.front().path);
    _lake_update_manager = new lake::UpdateManager(_lake_location_provider, global_env->update_mem_tracker());
    _lake_tablet_manager =
            new lake::TabletManager(_lake_location_provider, _lake_update_manager, config::lake_metadata_cache_limit);
    _lake_replication_txn_manager = new lake::ReplicationTxnManager(_lake_tablet_manager);
    _parallel_compact_mgr = std::make_unique<lake::LakePersistentIndexParallelCompactMgr>(_lake_tablet_manager);
    RETURN_IF_ERROR_WITH_WARN(_parallel_compact_mgr->init(), "init ParallelCompactMgr failed");
#endif

    RETURN_IF_ERROR(global_env->init_lake_thread_pools(process_metrics));

    _load_channel_mgr = new LoadChannelMgr(_lake_tablet_manager, process_metrics, _table_metrics_mgr);

    _agent_server = new AgentServer(this, false);
    RETURN_IF_ERROR(_agent_server->init());

    _broker_mgr->init();
    RETURN_IF_ERROR(_small_file_mgr->init());

    RETURN_IF_ERROR(_load_channel_mgr->init(global_env->load_mem_tracker()));

    _heartbeat_flags = new HeartbeatFlags();
    auto capacity = std::max<size_t>(config::query_cache_capacity, 4L * 1024 * 1024);
    _cache_mgr = new query_cache::CacheManager(capacity);

    _spill_dir_mgr = std::make_shared<spill::DirManager>();
    RETURN_IF_ERROR(_spill_dir_mgr->init(config::spill_local_storage_dir));
    // Bridge the local spill DirManager into the spill_disk_bytes_used gauge
    // via a collect-time hook so the metrics registry stays decoupled from
    // spill internals. The callback captures a raw pointer because the
    // DirManager lives for the lifetime of ExecEnv.
    if (auto* spill_metrics = SpillMetrics::instance(); spill_metrics->local_disk_bytes_used() != nullptr) {
        process_metrics->register_hook("spill_disk_bytes_used", [dir_mgr = _spill_dir_mgr.get(), spill_metrics]() {
            int64_t local_bytes = 0;
            for (auto& dir : dir_mgr->dirs()) {
                local_bytes += dir->get_current_size();
            }
            spill_metrics->local_disk_bytes_used()->set_value(local_bytes);
        });
    }

    _global_spill_manager = std::make_shared<spill::GlobalSpillManager>();

    _diagnose_daemon = new DiagnoseDaemon();
    RETURN_IF_ERROR(_diagnose_daemon->init());
#ifdef STARROCKS_JIT_ENABLE
    auto jit_engine = JITEngine::get_instance();
    status = jit_engine->init();
    if (!status.ok()) {
        LOG(WARNING) << "Failed to init JIT engine: " << status.message();
    }
#endif

    RETURN_IF_ERROR(PythonEnvManager::getInstance().init(config::python_envs));
    PythonEnvManager::getInstance().start_background_cleanup_thread();

    _refresh_service_contexts();

    return Status::OK();
}

std::string ExecEnv::token() const {
    return get_master_token();
}

void ExecEnv::stop() {
    int64_t total_start = MonotonicMillis();
    int64_t start;
    std::vector<std::pair<std::string, int64_t>> component_times;
    auto* global_env = _global_env;
    DCHECK(global_env != nullptr);

    if (_load_channel_mgr) {
        start = MonotonicMillis();
        // Clear load channel should be executed before stopping the storage engine,
        // otherwise some writing tasks will still be in the MemTableFlushThreadPool of the storage engine,
        // so when the ThreadPool is destroyed, it will crash.
        _load_channel_mgr->close();
        component_times.emplace_back("load_channel_mgr", MonotonicMillis() - start);
    }

    if (_load_stream_mgr) {
        start = MonotonicMillis();
        _load_stream_mgr->close();
        component_times.emplace_back("load_stream_mgr", MonotonicMillis() - start);
    }

    if (_fragment_mgr) {
        start = MonotonicMillis();
        _fragment_mgr->close();
        component_times.emplace_back("fragment_mgr", MonotonicMillis() - start);
    }

    if (_stream_mgr != nullptr) {
        start = MonotonicMillis();
        _stream_mgr->close();
        component_times.emplace_back("stream_mgr", MonotonicMillis() - start);
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

    if (_parallel_compact_mgr) {
        start = MonotonicMillis();
        _parallel_compact_mgr->shutdown();
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

    if (_agent_server) {
        start = MonotonicMillis();
        _agent_server->stop();
        component_times.emplace_back("agent_server", MonotonicMillis() - start);
    }

    if (_runtime_filter_worker) {
        start = MonotonicMillis();
        _runtime_filter_worker->close();
        component_times.emplace_back("runtime_filter_worker", MonotonicMillis() - start);
    }

    if (_profile_report_worker) {
        start = MonotonicMillis();
        _profile_report_worker->close();
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

    if (_workgroup_manager) {
        start = MonotonicMillis();
        _workgroup_manager->close();
        component_times.emplace_back("workgroup_manager", MonotonicMillis() - start);
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

    if (_result_mgr) {
        start = MonotonicMillis();
        _result_mgr->stop();
        component_times.emplace_back("result_mgr", MonotonicMillis() - start);
    }

    if (_stream_mgr) {
        start = MonotonicMillis();
        _stream_mgr->close();
        component_times.emplace_back("stream_mgr", MonotonicMillis() - start);
    }

    if (_batch_write_mgr) {
        start = MonotonicMillis();
        _batch_write_mgr->stop();
        component_times.emplace_back("batch_write_mgr", MonotonicMillis() - start);
    }

#ifndef __APPLE__
    if (_routine_load_task_executor) {
        start = MonotonicMillis();
        _routine_load_task_executor->stop();
        component_times.emplace_back("routine_load_task_executor", MonotonicMillis() - start);
    }
#endif

    if (global_env->dictionary_cache_pool()) {
        start = MonotonicMillis();
        global_env->dictionary_cache_pool()->shutdown();
        component_times.emplace_back("dictionary_cache_pool", MonotonicMillis() - start);
    }

    if (_diagnose_daemon) {
        start = MonotonicMillis();
        _diagnose_daemon->stop();
        component_times.emplace_back("diagnose_daemon", MonotonicMillis() - start);
    }

#if !defined(__APPLE__) && !defined(BE_TEST)
    start = MonotonicMillis();
    close_s3_clients();
    component_times.emplace_back("close_s3_clients", MonotonicMillis() - start);
#endif

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
    SAFE_DELETE(_agent_server);
    SAFE_DELETE(_runtime_filter_worker);
    SAFE_DELETE(_profile_report_worker);
    SAFE_DELETE(_heartbeat_flags);
    SAFE_DELETE(_small_file_mgr);
    SAFE_DELETE(_transaction_mgr);
    SAFE_DELETE(_stream_context_mgr);
#ifndef __APPLE__
    SAFE_DELETE(_routine_load_task_executor);
#endif
    SAFE_DELETE(_stream_load_executor);
    SAFE_DELETE(_connector_sink_spill_executor);
    SAFE_DELETE(_fragment_mgr);
    SAFE_DELETE(_load_stream_mgr);
    SAFE_DELETE(_load_channel_mgr);
    SAFE_DELETE(_broker_mgr);
    if (_rejected_record_sync_daemon != nullptr) {
        _rejected_record_sync_daemon->stop();
    }
    SAFE_DELETE(_rejected_record_sync_daemon);
    SAFE_DELETE(_load_path_mgr);
    SAFE_DELETE(_stream_mgr);
    SAFE_DELETE(_query_context_mgr);
    _workgroup_manager->destroy();
    _workgroup_manager.reset();

    if (_lake_tablet_manager != nullptr) {
        _lake_tablet_manager->prune_metacache();
    }

    // WorkGroupManager should release MemTracker of WorkGroups belongs to itself before deallocate
    // _query_pool_mem_tracker.
    SAFE_DELETE(_runtime_filter_cache);
    SAFE_DELETE(_driver_limiter);
    SAFE_DELETE(_pipeline_timer);
    SAFE_DELETE(_result_queue_mgr);
    SAFE_DELETE(_result_mgr);
    SAFE_DELETE(_lookup_dispatcher_mgr);
    SAFE_DELETE(_batch_write_mgr);
    SAFE_DELETE(_external_scan_context_mgr);
    SAFE_DELETE(_lake_tablet_manager);
    SAFE_DELETE(_lake_update_manager);
    SAFE_DELETE(_lake_replication_txn_manager);
    SAFE_DELETE(_cache_mgr);
    SAFE_DELETE(_diagnose_daemon);
    _parallel_compact_mgr.reset();
    DCHECK(_global_env != nullptr);
    _global_env->destroy_thread_pools();
    _query_execution_services.process_metrics = nullptr;
    _table_metrics_mgr = nullptr;
    _process_metrics_registry = nullptr;
}

void ExecEnv::_wait_for_fragments_finish() {
    if (config::loop_count_wait_fragments_finish < 0) {
        LOG(WARNING) << "'config::loop_count_wait_fragments_finish' is set to a negative integer, ignore it.";
        return;
    }

    size_t max_loop_secs = config::loop_count_wait_fragments_finish * 10;
    if (max_loop_secs == 0) {
        return;
    }

    size_t running_fragments = _get_running_fragments_count();
    size_t loop_secs = 0;

    // TODO: decouple the heartbeat with the graceful exit
    // only wait for frontend's heartbeat when the node is ever received heartbeats from the frontend
    bool need_wait_frontend_hb = config::graceful_exit_wait_for_frontend_heartbeat && get_backend_id().has_value();

    while ((running_fragments > 0 || (need_wait_frontend_hb && !is_frontend_aware_of_exit())) &&
           loop_secs < max_loop_secs) {
        LOG(INFO) << "Frontend is aware of exit: " << is_frontend_aware_of_exit() << ", " << running_fragments
                  << " fragment(s) are still running...";
        sleep(1);
        running_fragments = _get_running_fragments_count();
        loop_secs++;
    }
}

size_t ExecEnv::_get_running_fragments_count() const {
    // fragment is registered in _fragment_mgr in non-pipeline env
    // while _query_context_mgr is used in pipeline engine.
    return _fragment_mgr->running_fragment_count() + _query_context_mgr->size();
}

void ExecEnv::wait_for_finish() {
    _wait_for_fragments_finish();
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

ThreadPool* ExecEnv::delete_file_thread_pool() {
    return _agent_server ? _agent_server->get_thread_pool(TTaskType::DROP) : nullptr;
}

void ExecEnv::try_release_resource_before_core_dump() {
    std::set<std::string> modules;
    bool release_all = false;
    auto* global_env = _global_env;
    DCHECK(global_env != nullptr);
    if (config::try_release_resource_before_core_dump.value() == "*") {
        release_all = true;
    } else {
        SplitStringAndParseToContainer(StringPiece(config::try_release_resource_before_core_dump), ",",
                                       &parse_resource_str, &modules);
    }

    auto need_release = [&release_all, &modules](const std::string& name) {
        return release_all || modules.contains(name);
    };

    if (_workgroup_manager != nullptr && need_release("connector_scan_executor")) {
        _workgroup_manager->for_each_executors([](auto& executors) { executors.connector_scan_executor()->close(); });
    }
    if (_workgroup_manager != nullptr && need_release("olap_scan_executor")) {
        _workgroup_manager->for_each_executors([](auto& executors) { executors.scan_executor()->close(); });
    }
    if (global_env->thread_pool() != nullptr && need_release("non_pipeline_scan_thread_pool")) {
        global_env->thread_pool()->shutdown();
    }
    if (global_env->pipeline_prepare_pool() != nullptr && need_release("pipeline_prepare_thread_pool")) {
        global_env->pipeline_prepare_pool()->shutdown();
    }
    if (global_env->pipeline_sink_io_pool() != nullptr && need_release("pipeline_sink_io_thread_pool")) {
        global_env->pipeline_sink_io_pool()->shutdown();
    }
    if (global_env->query_rpc_pool() != nullptr && need_release("query_rpc_thread_pool")) {
        global_env->query_rpc_pool()->shutdown();
    }
    if (global_env->datacache_rpc_pool() != nullptr && need_release("datacache_rpc_thread_pool")) {
        global_env->datacache_rpc_pool()->shutdown();
        LOG(INFO) << "shutdown datacache rpc thread pool";
    }
    if (_agent_server != nullptr && need_release("publish_version_worker_pool")) {
        _agent_server->stop_task_worker_pool(TaskWorkerType::PUBLISH_VERSION);
    }
    if (_workgroup_manager != nullptr && need_release("wg_driver_executor")) {
        _workgroup_manager->for_each_executors([](auto& executors) { executors.driver_executor()->close(); });
    }
}

pipeline::DriverExecutor* ExecEnv::wg_driver_executor() {
    return _workgroup_manager->shared_executors()->driver_executor();
}
workgroup::ScanExecutor* ExecEnv::scan_executor() {
    return _workgroup_manager->shared_executors()->scan_executor();
}
workgroup::ScanExecutor* ExecEnv::connector_scan_executor() {
    return _workgroup_manager->shared_executors()->connector_scan_executor();
}

} // namespace starrocks
