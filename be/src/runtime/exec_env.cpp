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
#include "agent/master_info.h"
#include "column/column_pool.h"
#include "common/config.h"
#include "common/configbase.h"
#include "common/logging.h"
#include "exec/pipeline/driver_limiter.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/query_context.h"
#include "exec/spill/dir_manager.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"
#include "exprs/jit/jit_engine.h"
#include "fs/fs_s3.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/TFileBrokerService.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/dummy_load_path_mgr.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/mem_chunk_allocator.h"
#include "runtime/profile_report_worker.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_filter_worker.h"
#include "runtime/small_file_mgr.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/transaction_mgr.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/replication_txn_manager.h"
#include "storage/lake/starlet_location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/page_cache.h"
#include "storage/storage_engine.h"
#include "storage/tablet_schema_map.h"
#include "storage/update_manager.h"
#include "util/bfd_parser.h"
#include "util/brpc_stub_cache.h"
#include "util/cpu_info.h"
#include "util/mem_info.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/priority_thread_pool.hpp"
#include "util/starrocks_metrics.h"

namespace starrocks {

// Calculate the total memory limit of all load tasks on this BE
static int64_t calc_max_load_memory(int64_t process_mem_limit) {
    if (process_mem_limit == -1) {
        // no limit
        return -1;
    }
    int32_t max_load_memory_percent = config::load_process_max_memory_limit_percent;
    int64_t max_load_memory_bytes = process_mem_limit * max_load_memory_percent / 100;
    return std::min<int64_t>(max_load_memory_bytes, config::load_process_max_memory_limit_bytes);
}

static int64_t calc_max_compaction_memory(int64_t process_mem_limit) {
    int64_t limit = config::compaction_max_memory_limit;
    int64_t percent = config::compaction_max_memory_limit_percent;

    if (config::compaction_memory_limit_per_worker < 0) {
        config::compaction_memory_limit_per_worker = 2147483648; // 2G
    }

    if (process_mem_limit < 0) {
        return -1;
    }
    if (limit < 0) {
        limit = process_mem_limit;
    }
    if (percent < 0 || percent > 100) {
        percent = 100;
    }
    return std::min<int64_t>(limit, process_mem_limit * percent / 100);
}

static int64_t calc_max_consistency_memory(int64_t process_mem_limit) {
    int64_t limit = ParseUtil::parse_mem_spec(config::consistency_max_memory_limit, process_mem_limit);
    int64_t percent = config::consistency_max_memory_limit_percent;

    if (process_mem_limit < 0) {
        return -1;
    }
    if (limit < 0) {
        limit = process_mem_limit;
    }
    if (percent < 0 || percent > 100) {
        percent = 100;
    }
    return std::min<int64_t>(limit, process_mem_limit * percent / 100);
}

class SetMemTrackerForColumnPool {
public:
    SetMemTrackerForColumnPool(std::shared_ptr<MemTracker> mem_tracker) : _mem_tracker(std::move(mem_tracker)) {}

    template <typename Pool>
    void operator()() {
        Pool::singleton()->set_mem_tracker(_mem_tracker);
    }

private:
    std::shared_ptr<MemTracker> _mem_tracker = nullptr;
};

bool GlobalEnv::_is_init = false;

bool GlobalEnv::is_init() {
    return _is_init;
}

Status GlobalEnv::init() {
    RETURN_IF_ERROR(_init_mem_tracker());
    _is_init = true;
    return Status::OK();
}

Status GlobalEnv::_init_mem_tracker() {
    int64_t bytes_limit = 0;
    std::stringstream ss;
    // --mem_limit="" means no memory limit
    bytes_limit = ParseUtil::parse_mem_spec(config::mem_limit, MemInfo::physical_mem());
    // use 90% of mem_limit as the soft mem limit of BE
    bytes_limit = bytes_limit * 0.9;
    if (bytes_limit <= 0) {
        ss << "Failed to parse mem limit from '" + config::mem_limit + "'.";
        return Status::InternalError(ss.str());
    }

    if (bytes_limit > MemInfo::physical_mem()) {
        LOG(WARNING) << "Memory limit " << PrettyPrinter::print(bytes_limit, TUnit::BYTES)
                     << " exceeds physical memory of " << PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES)
                     << ". Using physical memory instead";
        bytes_limit = MemInfo::physical_mem();
    }

    if (bytes_limit <= 0) {
        ss << "Invalid mem limit: " << bytes_limit;
        return Status::InternalError(ss.str());
    }

    _process_mem_tracker = regist_tracker(MemTracker::PROCESS, bytes_limit, "process");
    int64_t query_pool_mem_limit =
            calc_max_query_memory(_process_mem_tracker->limit(), config::query_max_memory_limit_percent);
    _query_pool_mem_tracker =
            regist_tracker(MemTracker::QUERY_POOL, query_pool_mem_limit, "query_pool", this->process_mem_tracker());
    _connector_scan_pool_mem_tracker =
            regist_tracker(MemTracker::QUERY_POOL, query_pool_mem_limit * config::connector_scan_use_query_mem_ratio,
                           "query_pool/connector_scan", nullptr);

    int64_t load_mem_limit = calc_max_load_memory(_process_mem_tracker->limit());
    _load_mem_tracker = regist_tracker(MemTracker::LOAD, load_mem_limit, "load", process_mem_tracker());

    // Metadata statistics memory statistics do not use new mem statistics framework with hook
    _metadata_mem_tracker = regist_tracker(-1, "metadata", nullptr);

    _tablet_metadata_mem_tracker = regist_tracker(-1, "tablet_metadata", _metadata_mem_tracker.get());
    _rowset_metadata_mem_tracker = regist_tracker(-1, "rowset_metadata", _metadata_mem_tracker.get());
    _segment_metadata_mem_tracker = regist_tracker(-1, "segment_metadata", _metadata_mem_tracker.get());
    _column_metadata_mem_tracker = regist_tracker(-1, "column_metadata", _metadata_mem_tracker.get());

    _tablet_schema_mem_tracker = regist_tracker(-1, "tablet_schema", _tablet_metadata_mem_tracker.get());
    _segment_zonemap_mem_tracker = regist_tracker(-1, "segment_zonemap", _segment_metadata_mem_tracker.get());
    _short_key_index_mem_tracker = regist_tracker(-1, "short_key_index", _segment_metadata_mem_tracker.get());
    _column_zonemap_index_mem_tracker = regist_tracker(-1, "column_zonemap_index", _column_metadata_mem_tracker.get());
    _ordinal_index_mem_tracker = regist_tracker(-1, "ordinal_index", _column_metadata_mem_tracker.get());
    _bitmap_index_mem_tracker = regist_tracker(-1, "bitmap_index", _column_metadata_mem_tracker.get());
    _bloom_filter_index_mem_tracker = regist_tracker(-1, "bloom_filter_index", _column_metadata_mem_tracker.get());

    int64_t compaction_mem_limit = calc_max_compaction_memory(_process_mem_tracker->limit());
    _compaction_mem_tracker = regist_tracker(compaction_mem_limit, "compaction", _process_mem_tracker.get());
    _schema_change_mem_tracker = regist_tracker(-1, "schema_change", _process_mem_tracker.get());
    _column_pool_mem_tracker = regist_tracker(-1, "column_pool", _process_mem_tracker.get());
    _page_cache_mem_tracker = regist_tracker(-1, "page_cache", _process_mem_tracker.get());
    int32_t update_mem_percent = std::max(std::min(100, config::update_memory_limit_percent), 0);
    _update_mem_tracker = regist_tracker(bytes_limit * update_mem_percent / 100, "update", nullptr);
    _chunk_allocator_mem_tracker = regist_tracker(-1, "chunk_allocator", _process_mem_tracker.get());
    _clone_mem_tracker = regist_tracker(-1, "clone", _process_mem_tracker.get());
    int64_t consistency_mem_limit = calc_max_consistency_memory(_process_mem_tracker->limit());
    _consistency_mem_tracker = regist_tracker(consistency_mem_limit, "consistency", _process_mem_tracker.get());
    _replication_mem_tracker = regist_tracker(-1, "replication", _process_mem_tracker.get());

    MemChunkAllocator::init_instance(_chunk_allocator_mem_tracker.get(), config::chunk_reserved_bytes_limit);

    SetMemTrackerForColumnPool op(_column_pool_mem_tracker);
    ForEach<ColumnPoolList>(op);
    _init_storage_page_cache(); // TODO: move to StorageEngine
    return Status::OK();
}

void GlobalEnv::_reset_tracker() {
    for (auto iter = _mem_trackers.rbegin(); iter != _mem_trackers.rend(); ++iter) {
        iter->reset();
    }
}

void GlobalEnv::_init_storage_page_cache() {
    int64_t storage_cache_limit = get_storage_page_cache_size();
    storage_cache_limit = check_storage_page_cache_size(storage_cache_limit);
    StoragePageCache::create_global_cache(page_cache_mem_tracker(), storage_cache_limit);
}

int64_t GlobalEnv::get_storage_page_cache_size() {
    std::lock_guard<std::mutex> l(*config::get_mstring_conf_lock());
    int64_t mem_limit = MemInfo::physical_mem();
    if (process_mem_tracker()->has_limit()) {
        mem_limit = process_mem_tracker()->limit();
    }
    return ParseUtil::parse_mem_spec(config::storage_page_cache_limit, mem_limit);
}

int64_t GlobalEnv::check_storage_page_cache_size(int64_t storage_cache_limit) {
    if (storage_cache_limit > MemInfo::physical_mem()) {
        LOG(WARNING) << "Config storage_page_cache_limit is greater than memory size, config="
                     << config::storage_page_cache_limit << ", memory=" << MemInfo::physical_mem();
    }
    if (!config::disable_storage_page_cache) {
        if (storage_cache_limit < kcacheMinSize) {
            LOG(WARNING) << "Storage cache limit is too small, use default size.";
            storage_cache_limit = kcacheMinSize;
        }
        LOG(INFO) << "Set storage page cache size " << storage_cache_limit;
    }
    return storage_cache_limit;
}

template <class... Args>
std::shared_ptr<MemTracker> GlobalEnv::regist_tracker(Args&&... args) {
    auto mem_tracker = std::make_shared<MemTracker>(std::forward<Args>(args)...);
    _mem_trackers.emplace_back(mem_tracker);
    return mem_tracker;
}

int64_t GlobalEnv::calc_max_query_memory(int64_t process_mem_limit, int64_t percent) {
    if (process_mem_limit <= 0) {
        // -1 means no limit
        return -1;
    }
    if (percent < 0 || percent > 100) {
        percent = 90;
    }
    return process_mem_limit * percent / 100;
}

Status ExecEnv::init(const std::vector<StorePath>& store_paths, bool as_cn) {
    _store_paths = store_paths;
    _external_scan_context_mgr = new ExternalScanContextMgr(this);
    _metrics = StarRocksMetrics::instance()->metrics();
    _stream_mgr = new DataStreamMgr();
    _result_mgr = new ResultBufferMgr();
    _result_queue_mgr = new ResultQueueMgr();
    _backend_client_cache = new BackendServiceClientCache(config::max_client_cache_size_per_host);
    _frontend_client_cache = new FrontendServiceClientCache(config::max_client_cache_size_per_host);
    _broker_client_cache = new BrokerServiceClientCache(config::max_client_cache_size_per_host);
    // query_context_mgr keeps slotted map with 64 slot to reduce contention
    _query_context_mgr = new pipeline::QueryContextManager(6);
    RETURN_IF_ERROR(_query_context_mgr->init());
    _thread_pool =
            new PriorityThreadPool("table_scan_io", // olap/external table scan thread pool
                                   config::scanner_thread_pool_thread_num, config::scanner_thread_pool_queue_size);

    // Thread pool used for streaming load to scan StreamLoadPipe. The maximum number of
    // threads and queue size are set INT32_MAX which indicate there is no limit for the
    // thread pool, and this can avoid deadlock for concurrent streaming loads. The thread
    // pool will not be full easily because fragment execution pool and http workers also
    // limit the streaming load concurrency which is controlled by fragment_pool_thread_num_max
    // and webserver_num_workers respectively. This pool will be used when
    // enable_streaming_load_thread_pool is true.
    std::unique_ptr<ThreadPool> streaming_load_pool;
    RETURN_IF_ERROR(
            ThreadPoolBuilder("stream_load_io")
                    .set_min_threads(config::streaming_load_thread_pool_num_min)
                    .set_max_threads(INT32_MAX)
                    .set_max_queue_size(INT32_MAX)
                    .set_idle_timeout(MonoDelta::FromMilliseconds(config::streaming_load_thread_pool_idle_time_ms))
                    .build(&streaming_load_pool));
    _streaming_load_thread_pool = streaming_load_pool.release();

    _udf_call_pool = new PriorityThreadPool("udf", config::udf_thread_pool_size, config::udf_thread_pool_size);
    _fragment_mgr = new FragmentMgr(this);

    RETURN_IF_ERROR(ThreadPoolBuilder("automatic_partition") // automatic partition pool
                            .set_min_threads(0)
                            .set_max_threads(8)
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&_automatic_partition_pool));

    int num_prepare_threads = config::pipeline_prepare_thread_pool_thread_num;
    if (num_prepare_threads == 0) {
        num_prepare_threads = CpuInfo::num_cores();
    } else if (num_prepare_threads < 0) {
        // -n: means n * num_cpu_cores
        num_prepare_threads = -num_prepare_threads * CpuInfo::num_cores();
    }
    _pipeline_prepare_pool =
            new PriorityThreadPool("pip_prepare", num_prepare_threads, config::pipeline_prepare_thread_pool_queue_size);
    // register the metrics to monitor the task queue len
    auto task_qlen_fun = [] {
        auto pool = ExecEnv::GetInstance()->pipeline_prepare_pool();
        return (pool == nullptr) ? 0U : pool->get_queue_size();
    };
    REGISTER_GAUGE_STARROCKS_METRIC(pipe_prepare_pool_queue_len, task_qlen_fun);

    int num_sink_io_threads = config::pipeline_sink_io_thread_pool_thread_num;
    if (num_sink_io_threads <= 0) {
        num_sink_io_threads = CpuInfo::num_cores();
    }
    if (config::pipeline_sink_io_thread_pool_queue_size <= 0) {
        return Status::InvalidArgument("pipeline_sink_io_thread_pool_queue_size shoule be greater than 0");
    }
    _pipeline_sink_io_pool =
            new PriorityThreadPool("pip_sink_io", num_sink_io_threads, config::pipeline_sink_io_thread_pool_queue_size);

    int query_rpc_threads = config::internal_service_query_rpc_thread_num;
    if (query_rpc_threads <= 0) {
        query_rpc_threads = CpuInfo::num_cores();
    }
    _query_rpc_pool = new PriorityThreadPool("query_rpc", query_rpc_threads, std::numeric_limits<uint32_t>::max());

    // The _load_rpc_pool now handles routine load RPC and table function RPC.
    RETURN_IF_ERROR(ThreadPoolBuilder("load_rpc") // thread pool for load rpc
                            .set_min_threads(10)
                            .set_max_threads(1000)
                            .set_max_queue_size(0)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&_load_rpc_pool));
    REGISTER_GAUGE_STARROCKS_METRIC(load_rpc_threadpool_size, _load_rpc_pool->num_threads)

    RETURN_IF_ERROR(ThreadPoolBuilder("dictionary_cache") // thread pool for dictionary cache Sink
                            .set_min_threads(1)
                            .set_max_threads(8)
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&_dictionary_cache_pool));

    std::unique_ptr<ThreadPool> driver_executor_thread_pool;
    _max_executor_threads = CpuInfo::num_cores();
    if (config::pipeline_exec_thread_pool_thread_num > 0) {
        _max_executor_threads = config::pipeline_exec_thread_pool_thread_num;
    }
    _max_executor_threads = std::max<int64_t>(1, _max_executor_threads);
    LOG(INFO) << strings::Substitute("[PIPELINE] Exec thread pool: thread_num=$0", _max_executor_threads);

    _driver_limiter =
            new pipeline::DriverLimiter(_max_executor_threads * config::pipeline_max_num_drivers_per_exec_thread);

    std::unique_ptr<ThreadPool> wg_driver_executor_thread_pool;
    RETURN_IF_ERROR(ThreadPoolBuilder("pip_wg_executor") // pipeline executor for workgroup
                            .set_min_threads(0)
                            .set_max_threads(_max_executor_threads)
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&wg_driver_executor_thread_pool));
    _wg_driver_executor =
            new pipeline::GlobalDriverExecutor("wg_pip_exe", std::move(wg_driver_executor_thread_pool), true);
    _wg_driver_executor->initialize(_max_executor_threads);

    int connector_num_io_threads = int(config::pipeline_connector_scan_thread_num_per_cpu * CpuInfo::num_cores());
    CHECK_GT(connector_num_io_threads, 0) << "pipeline_connector_scan_thread_num_per_cpu should greater than 0";

    if (config::hdfs_client_enable_hedged_read) {
        // Set hdfs client hedged read pool size
        config::hdfs_client_hedged_read_threadpool_size =
                std::min(connector_num_io_threads * 2, config::hdfs_client_hedged_read_threadpool_size);
        CHECK_GT(config::hdfs_client_hedged_read_threadpool_size, 0)
                << "hdfs_client_hedged_read_threadpool_size should greater than 0";
    }

    std::unique_ptr<ThreadPool> connector_scan_worker_thread_pool_with_workgroup;
    RETURN_IF_ERROR(ThreadPoolBuilder("con_wg_scan_io")
                            .set_min_threads(0)
                            .set_max_threads(connector_num_io_threads)
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&connector_scan_worker_thread_pool_with_workgroup));
    _connector_scan_executor =
            new workgroup::ScanExecutor(std::move(connector_scan_worker_thread_pool_with_workgroup),
                                        std::make_unique<workgroup::WorkGroupScanTaskQueue>(
                                                workgroup::WorkGroupScanTaskQueue::SchedEntityType::CONNECTOR));
    _connector_scan_executor->initialize(connector_num_io_threads);

    workgroup::DefaultWorkGroupInitialization default_workgroup_init;

    if (store_paths.empty() && as_cn) {
        _load_path_mgr = new DummyLoadPathMgr();
    } else {
        _load_path_mgr = new LoadPathMgr(this);
    }

    _broker_mgr = new BrokerMgr(this);
    _bfd_parser = BfdParser::create();
    _load_channel_mgr = new LoadChannelMgr();
    _load_stream_mgr = new LoadStreamMgr();
    _brpc_stub_cache = new BrpcStubCache();
    _stream_load_executor = new StreamLoadExecutor(this);
    _stream_context_mgr = new StreamContextMgr();
    _transaction_mgr = new TransactionMgr(this);

    _routine_load_task_executor = new RoutineLoadTaskExecutor(this);
    RETURN_IF_ERROR(_routine_load_task_executor->init());

    _small_file_mgr = new SmallFileMgr(this, config::small_file_dir);
    _runtime_filter_worker = new RuntimeFilterWorker(this);
    _runtime_filter_cache = new RuntimeFilterCache(8);
    RETURN_IF_ERROR(_runtime_filter_cache->init());
    _profile_report_worker = new ProfileReportWorker(this);

    _backend_client_cache->init_metrics(StarRocksMetrics::instance()->metrics(), "backend");
    _frontend_client_cache->init_metrics(StarRocksMetrics::instance()->metrics(), "frontend");
    _broker_client_cache->init_metrics(StarRocksMetrics::instance()->metrics(), "broker");
    RETURN_IF_ERROR(_result_mgr->init());

    int num_io_threads = config::pipeline_scan_thread_pool_thread_num <= 0
                                 ? CpuInfo::num_cores()
                                 : config::pipeline_scan_thread_pool_thread_num;

    std::unique_ptr<ThreadPool> scan_worker_thread_pool_with_workgroup;
    RETURN_IF_ERROR(ThreadPoolBuilder("pip_wg_scan_io")
                            .set_min_threads(0)
                            .set_max_threads(num_io_threads)
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&scan_worker_thread_pool_with_workgroup));
    _scan_executor = new workgroup::ScanExecutor(std::move(scan_worker_thread_pool_with_workgroup),
                                                 std::make_unique<workgroup::WorkGroupScanTaskQueue>(
                                                         workgroup::WorkGroupScanTaskQueue::SchedEntityType::OLAP));
    _scan_executor->initialize(num_io_threads);
    // it means acting as compute node while store_path is empty. some threads are not needed for that case.
    Status status = _load_path_mgr->init();
    if (!status.ok()) {
        LOG(ERROR) << "load path mgr init failed." << status.message();
        exit(-1);
    }

#if defined(USE_STAROS) && !defined(BE_TEST)
    _lake_location_provider = new lake::StarletLocationProvider();
    _lake_update_manager =
            new lake::UpdateManager(_lake_location_provider, GlobalEnv::GetInstance()->update_mem_tracker());
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

#elif defined(BE_TEST)
    _lake_location_provider = new lake::FixedLocationProvider(_store_paths.front().path);
    _lake_update_manager =
            new lake::UpdateManager(_lake_location_provider, GlobalEnv::GetInstance()->update_mem_tracker());
    _lake_tablet_manager =
            new lake::TabletManager(_lake_location_provider, _lake_update_manager, config::lake_metadata_cache_limit);
    _lake_replication_txn_manager = new lake::ReplicationTxnManager(_lake_tablet_manager);
#endif

    _agent_server = new AgentServer(this, false);
    _agent_server->init_or_die();

    _broker_mgr->init();
    RETURN_IF_ERROR(_small_file_mgr->init());

    RETURN_IF_ERROR(_load_channel_mgr->init(GlobalEnv::GetInstance()->load_mem_tracker()));

    _heartbeat_flags = new HeartbeatFlags();
    auto capacity = std::max<size_t>(config::query_cache_capacity, 4L * 1024 * 1024);
    _cache_mgr = new query_cache::CacheManager(capacity);

    _spill_dir_mgr = std::make_shared<spill::DirManager>();
    RETURN_IF_ERROR(_spill_dir_mgr->init(config::spill_local_storage_dir));

    auto jit_engine = JITEngine::get_instance();
    status = jit_engine->init();
    if (!status.ok()) {
        LOG(WARNING) << "Failed to init JIT engine: " << status.message();
    }

    return Status::OK();
}

std::string ExecEnv::token() const {
    return get_master_token();
}

void ExecEnv::add_rf_event(const RfTracePoint& pt) {
    std::string msg =
            strings::Substitute("$0($1)", pt.msg, pt.network.empty() ? BackendOptions::get_localhost() : pt.network);
    _runtime_filter_cache->add_rf_event(pt.query_id, pt.filter_id, std::move(msg));
}

void ExecEnv::stop() {
    if (_load_channel_mgr) {
        // Clear load channel should be executed before stopping the storage engine,
        // otherwise some writing tasks will still be in the MemTableFlushThreadPool of the storage engine,
        // so when the ThreadPool is destroyed, it will crash.
        _load_channel_mgr->close();
    }

    if (_load_stream_mgr) {
        _load_stream_mgr->close();
    }

    if (_fragment_mgr) {
        _fragment_mgr->close();
    }

    if (_stream_mgr != nullptr) {
        _stream_mgr->close();
    }

    if (_pipeline_sink_io_pool) {
        _pipeline_sink_io_pool->shutdown();
    }

    if (_wg_driver_executor) {
        _wg_driver_executor->close();
    }

    if (_agent_server) {
        _agent_server->stop();
    }

    if (_runtime_filter_worker) {
        _runtime_filter_worker->close();
    }

    if (_profile_report_worker) {
        _profile_report_worker->close();
    }

    if (_automatic_partition_pool) {
        _automatic_partition_pool->shutdown();
    }

    if (_query_rpc_pool) {
        _query_rpc_pool->shutdown();
    }

    if (_load_rpc_pool) {
        _load_rpc_pool->shutdown();
    }

    if (_scan_executor) {
        _scan_executor->close();
    }

    if (_connector_scan_executor) {
        _connector_scan_executor->close();
    }

    if (_thread_pool) {
        _thread_pool->shutdown();
    }

    if (_query_context_mgr) {
        _query_context_mgr->clear();
    }

    if (_result_mgr) {
        _result_mgr->stop();
    }

    if (_stream_mgr) {
        _stream_mgr->close();
    }

    if (_routine_load_task_executor) {
        _routine_load_task_executor->stop();
    }

    if (_dictionary_cache_pool) {
        _dictionary_cache_pool->shutdown();
    }

#ifndef BE_TEST
    close_s3_clients();
#endif
}

void ExecEnv::destroy() {
    SAFE_DELETE(_agent_server);
    SAFE_DELETE(_runtime_filter_worker);
    SAFE_DELETE(_profile_report_worker);
    SAFE_DELETE(_heartbeat_flags);
    SAFE_DELETE(_small_file_mgr);
    SAFE_DELETE(_transaction_mgr);
    SAFE_DELETE(_stream_context_mgr);
    SAFE_DELETE(_routine_load_task_executor);
    SAFE_DELETE(_stream_load_executor);
    SAFE_DELETE(_fragment_mgr);
    SAFE_DELETE(_load_stream_mgr);
    SAFE_DELETE(_load_channel_mgr);
    SAFE_DELETE(_broker_mgr);
    SAFE_DELETE(_bfd_parser);
    SAFE_DELETE(_load_path_mgr);
    SAFE_DELETE(_wg_driver_executor);
    SAFE_DELETE(_brpc_stub_cache);
    SAFE_DELETE(_udf_call_pool);
    SAFE_DELETE(_pipeline_prepare_pool);
    SAFE_DELETE(_pipeline_sink_io_pool);
    SAFE_DELETE(_query_rpc_pool);
    _load_rpc_pool.reset();
    SAFE_DELETE(_scan_executor);
    SAFE_DELETE(_connector_scan_executor);
    SAFE_DELETE(_thread_pool);
    SAFE_DELETE(_streaming_load_thread_pool);

    if (_lake_tablet_manager != nullptr) {
        _lake_tablet_manager->prune_metacache();
    }

    SAFE_DELETE(_query_context_mgr);
    // WorkGroupManager should release MemTracker of WorkGroups belongs to itself before deallocate
    // _query_pool_mem_tracker.
    workgroup::WorkGroupManager::instance()->destroy();
    SAFE_DELETE(_runtime_filter_cache);
    SAFE_DELETE(_driver_limiter);
    SAFE_DELETE(_broker_client_cache);
    SAFE_DELETE(_frontend_client_cache);
    SAFE_DELETE(_backend_client_cache);
    SAFE_DELETE(_result_queue_mgr);
    SAFE_DELETE(_result_mgr);
    SAFE_DELETE(_stream_mgr);
    SAFE_DELETE(_external_scan_context_mgr);
    SAFE_DELETE(_lake_tablet_manager);
    SAFE_DELETE(_lake_location_provider);
    SAFE_DELETE(_lake_update_manager);
    SAFE_DELETE(_lake_replication_txn_manager);
    SAFE_DELETE(_cache_mgr);
    _metrics = nullptr;
}

void ExecEnv::_wait_for_fragments_finish() {
    size_t max_loop_cnt_cfg = config::loop_count_wait_fragments_finish;
    if (max_loop_cnt_cfg == 0) {
        return;
    }

    size_t running_fragments = _fragment_mgr->running_fragment_count();
    size_t loop_cnt = 0;

    while (running_fragments && loop_cnt < max_loop_cnt_cfg) {
        DLOG(INFO) << running_fragments << " fragment(s) are still running...";
        sleep(10);
        running_fragments = _fragment_mgr->running_fragment_count();
        loop_cnt++;
    }
}

void ExecEnv::wait_for_finish() {
    _wait_for_fragments_finish();
}

uint32_t ExecEnv::calc_pipeline_dop(int32_t pipeline_dop) const {
    if (pipeline_dop > 0) {
        return pipeline_dop;
    }

    // Default dop is a half of the number of hardware threads.
    return std::max<uint32_t>(1, _max_executor_threads / 2);
}

uint32_t ExecEnv::calc_pipeline_sink_dop(int32_t pipeline_sink_dop) const {
    if (pipeline_sink_dop > 0) {
        return pipeline_sink_dop;
    }

    // Default sink dop is the number of hardware threads with a cap of 64.
    auto dop = std::max<uint32_t>(1, _max_executor_threads);
    return std::min<uint32_t>(dop, 64);
}

ThreadPool* ExecEnv::delete_file_thread_pool() {
    return _agent_server ? _agent_server->get_thread_pool(TTaskType::DROP) : nullptr;
}

} // namespace starrocks
