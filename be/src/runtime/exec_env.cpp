// This file is made available under Elastic License 2.0.
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

#include <thread>

#include "column/column_pool.h"
#include "common/config.h"
#include "common/logging.h"
#include "exec/pipeline/driver_limiter.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/query_context.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"
#include "exec/workgroup/work_group_fwd.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/TFileBrokerService.h"
#include "gutil/strings/substitute.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/runtime_filter_worker.h"
#include "runtime/small_file_mgr.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/transaction_mgr.h"
#include "runtime/thread_resource_mgr.h"
#include "storage/page_cache.h"
#include "storage/storage_engine.h"
#include "storage/tablet_schema_map.h"
#include "storage/update_manager.h"
#include "util/bfd_parser.h"
#include "util/brpc_stub_cache.h"
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
    int64_t limit = ParseUtil::parse_mem_spec(config::consistency_max_memory_limit);
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

Status ExecEnv::init(ExecEnv* env, const std::vector<StorePath>& store_paths) {
    return env->_init(store_paths);
}

Status ExecEnv::_init(const std::vector<StorePath>& store_paths) {
    _store_paths = store_paths;
    _external_scan_context_mgr = new ExternalScanContextMgr(this);
    _metrics = StarRocksMetrics::instance()->metrics();
    _stream_mgr = new DataStreamMgr();
    _result_mgr = new ResultBufferMgr();
    _result_queue_mgr = new ResultQueueMgr();
    _backend_client_cache = new BackendServiceClientCache(config::max_client_cache_size_per_host);
    _frontend_client_cache = new FrontendServiceClientCache(config::max_client_cache_size_per_host);
    _broker_client_cache = new BrokerServiceClientCache(config::max_client_cache_size_per_host);
    _thread_mgr = new ThreadResourceMgr();
    // query_context_mgr keeps slotted map with 64 slot to reduce contention
    _query_context_mgr = new pipeline::QueryContextManager(6);
    RETURN_IF_ERROR(_query_context_mgr->init());
    _thread_pool = new PriorityThreadPool("olap_scan_io", // olap scan io
                                          config::doris_scanner_thread_pool_thread_num,
                                          config::doris_scanner_thread_pool_queue_size);

    int hdfs_num_io_threads = config::pipeline_hdfs_scan_thread_pool_thread_num;
    CHECK_GT(hdfs_num_io_threads, 0) << "pipeline_hdfs_scan_thread_pool_thread_num should greater than 0";

    _pipeline_hdfs_scan_io_thread_pool =
            new PriorityThreadPool("pip_hdfs_scan_io", // pipeline hdfs scan io
                                   hdfs_num_io_threads, config::pipeline_scan_thread_pool_queue_size);

    std::unique_ptr<ThreadPool> hdfs_scan_worker_thread_pool;
    RETURN_IF_ERROR(ThreadPoolBuilder("hdfs_scan_executor") // hdfs_scan io task executor
                            .set_min_threads(0)
                            .set_max_threads(hdfs_num_io_threads)
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&hdfs_scan_worker_thread_pool));
    _hdfs_scan_executor =
            new workgroup::ScanExecutor(std::move(hdfs_scan_worker_thread_pool),
                                        std::make_unique<workgroup::WorkGroupScanTaskQueue>(
                                                workgroup::WorkGroupScanTaskQueue::SchedEntityType::CONNECTOR));
    _hdfs_scan_executor->initialize(hdfs_num_io_threads);

    _udf_call_pool = new PriorityThreadPool("udf", config::udf_thread_pool_size, config::udf_thread_pool_size);
    _fragment_mgr = new FragmentMgr(this);

    std::unique_ptr<ThreadPool> driver_executor_thread_pool;
    _max_executor_threads = std::thread::hardware_concurrency();
    if (config::pipeline_exec_thread_pool_thread_num > 0) {
        _max_executor_threads = config::pipeline_exec_thread_pool_thread_num;
    }
    _max_executor_threads = std::max<int64_t>(1, _max_executor_threads);
    LOG(INFO) << strings::Substitute("[PIPELINE] Exec thread pool: thread_num=$0", _max_executor_threads);
    RETURN_IF_ERROR(ThreadPoolBuilder("pip_executor") // pipeline executor
                            .set_min_threads(0)
                            .set_max_threads(_max_executor_threads)
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&driver_executor_thread_pool));
    _driver_executor = new pipeline::GlobalDriverExecutor("pip_exe", std::move(driver_executor_thread_pool), false);
    _driver_executor->initialize(_max_executor_threads);

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

    starrocks::workgroup::DefaultWorkGroupInitialization default_workgroup_init;

    _master_info = new TMasterInfo();
    _load_path_mgr = new LoadPathMgr(this);
    _broker_mgr = new BrokerMgr(this);
    _bfd_parser = BfdParser::create();
    _load_channel_mgr = new LoadChannelMgr();
    _load_stream_mgr = new LoadStreamMgr();
    _brpc_stub_cache = new BrpcStubCache();
    _stream_load_executor = new StreamLoadExecutor(this);
    _stream_context_mgr = new StreamContextMgr();
    _transaction_mgr = new TransactionMgr(this);
    _routine_load_task_executor = new RoutineLoadTaskExecutor(this);
    _small_file_mgr = new SmallFileMgr(this, config::small_file_dir);
    _runtime_filter_worker = new RuntimeFilterWorker(this);
    _runtime_filter_cache = new RuntimeFilterCache(8);
    RETURN_IF_ERROR(_runtime_filter_cache->init());

    _backend_client_cache->init_metrics(StarRocksMetrics::instance()->metrics(), "backend");
    _frontend_client_cache->init_metrics(StarRocksMetrics::instance()->metrics(), "frontend");
    _broker_client_cache->init_metrics(StarRocksMetrics::instance()->metrics(), "broker");
    _result_mgr->init();
    // it means acting as compute node while store_path is empty. some threads are not needed for that case.
    if (!store_paths.empty()) {
        int num_io_threads = config::pipeline_scan_thread_pool_thread_num <= 0
                                     ? std::thread::hardware_concurrency()
                                     : config::pipeline_scan_thread_pool_thread_num;

        _pipeline_scan_io_thread_pool =
                new PriorityThreadPool("pip_scan_io", // pipeline scan io
                                       num_io_threads, config::pipeline_scan_thread_pool_queue_size);
        std::unique_ptr<ThreadPool> scan_worker_thread_pool;
        RETURN_IF_ERROR(ThreadPoolBuilder("scan_executor") // scan io task executor
                                .set_min_threads(0)
                                .set_max_threads(num_io_threads)
                                .set_max_queue_size(1000)
                                .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                                .build(&scan_worker_thread_pool));
        _scan_executor = new workgroup::ScanExecutor(std::move(scan_worker_thread_pool),
                                                     std::make_unique<workgroup::WorkGroupScanTaskQueue>(
                                                             workgroup::WorkGroupScanTaskQueue::SchedEntityType::OLAP));
        _scan_executor->initialize(num_io_threads);

        Status status = _load_path_mgr->init();
        if (!status.ok()) {
            LOG(ERROR) << "load path mgr init failed." << status.get_error_msg();
            exit(-1);
        }
    }
    _broker_mgr->init();
    _small_file_mgr->init();

    RETURN_IF_ERROR(_load_channel_mgr->init(_load_mem_tracker));
    _heartbeat_flags = new HeartbeatFlags();
    return Status::OK();
}

const std::string& ExecEnv::token() const {
    return _master_info->token;
}

void ExecEnv::add_rf_event(const RfTracePoint& pt) {
    std::string msg = strings::Substitute("$0($1)", std::move(pt.msg),
                                          pt.network.empty() ? BackendOptions::get_localhost() : pt.network);
    _runtime_filter_cache->add_rf_event(pt.query_id, pt.filter_id, std::move(msg));
}

class SetMemTrackerForColumnPool {
public:
    SetMemTrackerForColumnPool(MemTracker* mem_tracker) : _mem_tracker(mem_tracker) {}

    template <typename Pool>
    void operator()() {
        Pool::singleton()->set_mem_tracker(_mem_tracker);
    }

private:
    MemTracker* _mem_tracker = nullptr;
};

Status ExecEnv::init_mem_tracker() {
    int64_t bytes_limit = 0;
    std::stringstream ss;
    // --mem_limit="" means no memory limit
    bytes_limit = ParseUtil::parse_mem_spec(config::mem_limit);
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

    _mem_tracker = new MemTracker(MemTracker::PROCESS, bytes_limit, "process");
    _query_pool_mem_tracker = new MemTracker(MemTracker::QUERY_POOL, bytes_limit * 0.9, "query_pool", _mem_tracker);

    int64_t load_mem_limit = calc_max_load_memory(_mem_tracker->limit());
    _load_mem_tracker = new MemTracker(MemTracker::LOAD, load_mem_limit, "load", _mem_tracker);
    // Metadata statistics memory statistics do not use new mem statistics framework with hook
    _metadata_mem_tracker = new MemTracker(-1, "metadata", nullptr);
    _tablet_schema_mem_tracker = new MemTracker(-1, "tablet_schema", _metadata_mem_tracker);

    int64_t compaction_mem_limit = calc_max_compaction_memory(_mem_tracker->limit());
    _compaction_mem_tracker = new MemTracker(compaction_mem_limit, "compaction", _mem_tracker);
    _schema_change_mem_tracker = new MemTracker(-1, "schema_change", _mem_tracker);
    _column_pool_mem_tracker = new MemTracker(-1, "column_pool", _mem_tracker);
    _page_cache_mem_tracker = new MemTracker(-1, "page_cache", _mem_tracker);
    int32_t update_mem_percent = std::max(std::min(100, config::update_memory_limit_percent), 0);
    _update_mem_tracker = new MemTracker(bytes_limit * update_mem_percent / 100, "update", nullptr);
    _chunk_allocator_mem_tracker = new MemTracker(-1, "chunk_allocator", _mem_tracker);
    _clone_mem_tracker = new MemTracker(-1, "clone", _mem_tracker);
    int64_t consistency_mem_limit = calc_max_consistency_memory(_mem_tracker->limit());
    _consistency_mem_tracker = new MemTracker(consistency_mem_limit, "consistency", _mem_tracker);

    ChunkAllocator::init_instance(_chunk_allocator_mem_tracker, config::chunk_reserved_bytes_limit);

    SetMemTrackerForColumnPool op(_column_pool_mem_tracker);
    vectorized::ForEach<vectorized::ColumnPoolList>(op);
    _init_storage_page_cache();
    return Status::OK();
}

Status ExecEnv::_init_storage_page_cache() {
    int64_t storage_cache_limit = ParseUtil::parse_mem_spec(config::storage_page_cache_limit);
    if (storage_cache_limit > MemInfo::physical_mem()) {
        LOG(WARNING) << "Config storage_page_cache_limit is greater than memory size, config="
                     << config::storage_page_cache_limit << ", memory=" << MemInfo::physical_mem();
    }
    StoragePageCache::create_global_cache(_page_cache_mem_tracker, storage_cache_limit);

    // TODO(zc): The current memory usage configuration is a bit confusing,
    // we need to sort out the use of memory
    return Status::OK();
}

void ExecEnv::_destroy() {
    if (_runtime_filter_worker) {
        delete _runtime_filter_worker;
        _runtime_filter_worker = nullptr;
    }
    if (_heartbeat_flags) {
        delete _heartbeat_flags;
        _heartbeat_flags = nullptr;
    }
    if (_small_file_mgr) {
        delete _small_file_mgr;
        _small_file_mgr = nullptr;
    }
    if (_transaction_mgr) {
        delete _transaction_mgr;
        _transaction_mgr = nullptr;
    }
    if (_stream_context_mgr) {
        delete _stream_context_mgr;
        _stream_context_mgr = nullptr;
    }
    if (_routine_load_task_executor) {
        delete _routine_load_task_executor;
        _routine_load_task_executor = nullptr;
    }
    if (_stream_load_executor) {
        delete _stream_load_executor;
        _stream_load_executor = nullptr;
    }
    if (_brpc_stub_cache) {
        delete _brpc_stub_cache;
        _brpc_stub_cache = nullptr;
    }
    if (_load_stream_mgr) {
        delete _load_stream_mgr;
        _load_stream_mgr = nullptr;
    }
    if (_load_channel_mgr) {
        delete _load_channel_mgr;
        _load_channel_mgr = nullptr;
    }
    if (_broker_mgr) {
        delete _broker_mgr;
        _broker_mgr = nullptr;
    }
    if (_bfd_parser) {
        delete _bfd_parser;
        _bfd_parser = nullptr;
    }
    if (_load_path_mgr) {
        delete _load_path_mgr;
        _load_path_mgr = nullptr;
    }
    if (_master_info) {
        delete _master_info;
        _master_info = nullptr;
    }
    if (_driver_executor) {
        delete _driver_executor;
        _driver_executor = nullptr;
    }
    if (_wg_driver_executor) {
        delete _wg_driver_executor;
        _wg_driver_executor = nullptr;
    }
    if (_driver_limiter) {
        delete _driver_limiter;
        _driver_limiter = nullptr;
    }
    if (_fragment_mgr) {
        delete _fragment_mgr;
        _fragment_mgr = nullptr;
    }
    if (_udf_call_pool) {
        delete _udf_call_pool;
        _udf_call_pool = nullptr;
    }
    if (_pipeline_scan_io_thread_pool) {
        delete _pipeline_scan_io_thread_pool;
        _pipeline_scan_io_thread_pool = nullptr;
    }
    if (_pipeline_hdfs_scan_io_thread_pool) {
        delete _pipeline_hdfs_scan_io_thread_pool;
        _pipeline_hdfs_scan_io_thread_pool = nullptr;
    }
    if (_scan_executor) {
        delete _scan_executor;
        _scan_executor = nullptr;
    }
    if (_hdfs_scan_executor) {
        delete _hdfs_scan_executor;
        _hdfs_scan_executor = nullptr;
    }
    if (_runtime_filter_cache) {
        delete _runtime_filter_cache;
        _runtime_filter_cache = nullptr;
    }
    if (_thread_pool) {
        delete _thread_pool;
        _thread_pool = nullptr;
    }
    if (_thread_mgr) {
        delete _thread_mgr;
        _thread_mgr = nullptr;
    }
    if (_consistency_mem_tracker) {
        delete _consistency_mem_tracker;
        _consistency_mem_tracker = nullptr;
    }
    if (_clone_mem_tracker) {
        delete _clone_mem_tracker;
        _clone_mem_tracker = nullptr;
    }
    if (_chunk_allocator_mem_tracker) {
        delete _chunk_allocator_mem_tracker;
        _chunk_allocator_mem_tracker = nullptr;
    }
    if (_update_mem_tracker) {
        delete _update_mem_tracker;
        _update_mem_tracker = nullptr;
    }
    if (_page_cache_mem_tracker) {
        delete _page_cache_mem_tracker;
        _page_cache_mem_tracker = nullptr;
    }
    if (_column_pool_mem_tracker) {
        delete _column_pool_mem_tracker;
        _column_pool_mem_tracker = nullptr;
    }
    if (_schema_change_mem_tracker) {
        delete _schema_change_mem_tracker;
        _schema_change_mem_tracker = nullptr;
    }
    if (_compaction_mem_tracker) {
        delete _compaction_mem_tracker;
        _compaction_mem_tracker = nullptr;
    }

    if (_tablet_schema_mem_tracker) {
        delete _tablet_schema_mem_tracker;
        _tablet_schema_mem_tracker = nullptr;
    }
    if (_metadata_mem_tracker) {
        delete _metadata_mem_tracker;
        _metadata_mem_tracker = nullptr;
    }
    if (_load_mem_tracker) {
        delete _load_mem_tracker;
        _load_mem_tracker = nullptr;
    }
    // WorkGroupManager should release MemTracker of WorkGroups belongs to itself before deallocate _query_pool_mem_tracker.
    workgroup::WorkGroupManager::instance()->destroy();
    if (_query_pool_mem_tracker) {
        delete _query_pool_mem_tracker;
        _query_pool_mem_tracker = nullptr;
    }
    if (_query_context_mgr) {
        delete _query_context_mgr;
        _query_context_mgr = nullptr;
    }
    if (_mem_tracker) {
        delete _mem_tracker;
        _mem_tracker = nullptr;
    }
    if (_broker_client_cache) {
        delete _broker_client_cache;
        _broker_client_cache = nullptr;
    }
    if (_frontend_client_cache) {
        delete _frontend_client_cache;
        _frontend_client_cache = nullptr;
    }
    if (_backend_client_cache) {
        delete _backend_client_cache;
        _backend_client_cache = nullptr;
    }
    if (_result_queue_mgr) {
        delete _result_queue_mgr;
        _result_queue_mgr = nullptr;
    }
    if (_result_mgr) {
        delete _result_mgr;
        _result_mgr = nullptr;
    }
    if (_stream_mgr) {
        delete _stream_mgr;
        _stream_mgr = nullptr;
    }
    if (_external_scan_context_mgr) {
        delete _external_scan_context_mgr;
        _external_scan_context_mgr = nullptr;
    }
    _metrics = nullptr;
}

void ExecEnv::destroy(ExecEnv* env) {
    env->_destroy();
}

void ExecEnv::set_storage_engine(StorageEngine* storage_engine) {
    _storage_engine = storage_engine;
}

int32_t ExecEnv::calc_pipeline_dop(int32_t pipeline_dop) const {
    if (pipeline_dop > 0) {
        return pipeline_dop;
    }

    // Default dop is a half of the number of hardware threads.
    return std::max<int32_t>(1, _max_executor_threads / 2);
}

} // namespace starrocks
